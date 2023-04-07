// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.connector.iceberg.cost;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.Traverser;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperation;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.collect.Streams.stream;
import static com.starrocks.connector.iceberg.cost.IcebergTableStatisticsReader.getStatisticsFiles;
import static com.starrocks.connector.iceberg.cost.IcebergTableStatisticsWriter.StatsUpdateMode.INCREMENTAL_UPDATE;
import static com.starrocks.connector.iceberg.cost.IcebergTableStatisticsWriter.StatsUpdateMode.REPLACE;
import static java.lang.String.format;
import static java.util.Map.Entry.comparingByKey;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.SnapshotSummary.TOTAL_RECORDS_PROP;
import static org.apache.iceberg.puffin.PuffinCompressionCodec.ZSTD;
import static org.apache.iceberg.puffin.StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1;

public class IcebergTableStatisticsWriter {

    public enum StatsUpdateMode {
        // E.g. in ANALYZE case
        REPLACE,

        // E.g. in INSERT case
        INCREMENTAL_UPDATE,
    }

    public static final String APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY = "ndv";

    private IcebergTableStatisticsWriter() {}

    public StatisticsFile writeStatisticsFile(
            Table table,
            long snapshotId,
            StatsUpdateMode updateMode,
            CollectedStatistics collectedStatistics) {
        Snapshot snapshot = table.snapshot(snapshotId);
        TableOperations operations = ((HasTableOperations) table).operations();
        FileIO fileIO = operations.io();
        long snapshotSequenceNumber = snapshot.sequenceNumber();
        Schema schema = table.schemas().get(snapshot.schemaId());

        collectedStatistics = mergeStatisticsIfNecessary(
                table,
                snapshotId,
                fileIO,
                updateMode,
                collectedStatistics);
        Map<Integer, CompactSketch> ndvSketches = collectedStatistics.getNdvSketches();

        Set<Integer> validFieldIds = stream(
                Traverser.forTree((Types.NestedField nestedField) -> {
                    Type type = nestedField.type();
                    if (type instanceof Type.NestedType) {
                        Type.NestedType nestedType = (Type.NestedType) type;
                        return nestedType.fields();
                    }
                    if (type instanceof Type.PrimitiveType) {
                        return ImmutableList.of();
                    }
                    throw new IllegalArgumentException(String.format(
                            "Unrecognized type for field %s: %s", nestedField, type));
                }).depthFirstPreOrder(schema.columns()))
                .map(Types.NestedField::fieldId)
                .collect(toImmutableSet());

        String path = operations.metadataFileLocation(format("%s-%s.stats", "StarRocks", randomUUID()));
        OutputFile outputFile = fileIO.newOutputFile(path);
        try {
            try (PuffinWriter writer = Puffin.write(outputFile)
                    .createdBy("StarRocks")
                    .build()) {
                table.statisticsFiles().stream()
                        .filter(statisticsFile -> statisticsFile.snapshotId() == snapshotId)
                        .collect(toOptional())
                        .ifPresent(previousStatisticsFile ->
                                copyRetainedStatistics(
                                        fileIO, previousStatisticsFile, validFieldIds, ndvSketches.keySet(), writer));

                ndvSketches.entrySet().stream()
                        .sorted(comparingByKey())
                        .forEachOrdered(entry -> {
                            Integer fieldId = entry.getKey();
                            CompactSketch sketch = entry.getValue();
                            @SuppressWarnings("NumericCastThatLosesPrecision")
                            long ndvEstimate = (long) sketch.getEstimate();
                            writer.add(new Blob(
                                    APACHE_DATASKETCHES_THETA_V1,
                                    ImmutableList.of(fieldId),
                                    snapshotId,
                                    snapshotSequenceNumber,
                                    ByteBuffer.wrap(sketch.toByteArray()),
                                    ZSTD,
                                    ImmutableMap.of(APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY, Long.toString(ndvEstimate))));
                        });

                writer.finish();
                return new GenericStatisticsFile(
                        snapshotId,
                        path,
                        writer.fileSize(),
                        writer.footerSize(),
                        writer.writtenBlobsMetadata().stream()
                                .map(GenericBlobMetadata::from)
                                .collect(toImmutableList()));
            } catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
        } catch (Throwable throwable) {
            fileIO.deleteFile(path);
            throw throwable;
        }
    }

    private CollectedStatistics mergeStatisticsIfNecessary(
            Table table,
            long snapshotId,
            FileIO fileIO,
            StatsUpdateMode updateMode,
            CollectedStatistics collectedStatistics) {
        if (updateMode == INCREMENTAL_UPDATE) {
            Snapshot snapshot = table.snapshot(snapshotId);
            Preconditions.checkNotNull(snapshot,
                    "No snapshot information for snapshotId %s in table %s", snapshotId, table);
            if (snapshot.parentId() == null || !maySnapshotHaveData(table, snapshot.parentId(), fileIO)) {
                // No previous snapshot, or previous snapshot empty
                updateMode = REPLACE;
            }
        }

        switch (updateMode) {
            case REPLACE:
                return collectedStatistics;
            case INCREMENTAL_UPDATE:
                Map<Integer, CompactSketch> collectedNdvSketches = collectedStatistics.getNdvSketches();
                ImmutableMap.Builder<Integer, CompactSketch> ndvSketches = ImmutableMap.builder();

                Set<Integer> pendingPreviousNdvSketches = new HashSet<>(collectedNdvSketches.keySet());
                Iterator<StatisticsFile> statisticsFiles = getStatisticsFiles(table, snapshotId);
                while (!pendingPreviousNdvSketches.isEmpty() && statisticsFiles.hasNext()) {
                    StatisticsFile statisticsFile = statisticsFiles.next();

                    boolean hasUsefulData = statisticsFile.blobMetadata().stream()
                            .filter(blobMetadata -> blobMetadata.type().equals(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1))
                            .filter(blobMetadata -> blobMetadata.fields().size() == 1)
                            .anyMatch(blobMetadata ->
                                    pendingPreviousNdvSketches.contains(getOnlyElement(blobMetadata.fields())));

                    if (hasUsefulData) {
                        try (PuffinReader reader = Puffin.read(fileIO.newInputFile(statisticsFile.path()))
                                .withFileSize(statisticsFile.fileSizeInBytes())
                                .withFooterSize(statisticsFile.fileFooterSizeInBytes())
                                .build()) {
                            List<BlobMetadata> toRead = reader.fileMetadata().blobs().stream()
                                    .filter(blobMetadata -> blobMetadata.type().equals(APACHE_DATASKETCHES_THETA_V1))
                                    .filter(blobMetadata -> blobMetadata.inputFields().size() == 1)
                                    .filter(blobMetadata ->
                                            pendingPreviousNdvSketches.contains(getOnlyElement(blobMetadata.inputFields())))
                                    .collect(toImmutableList());
                            for (Pair<BlobMetadata, ByteBuffer> read : reader.readAll(toRead)) {
                                Integer fieldId = getOnlyElement(read.first().inputFields());
                                Preconditions.checkState(pendingPreviousNdvSketches.remove(fieldId),
                                        "Unwanted read of stats for field %s", fieldId);
                                // Memory.wrap(ByteBuffer) results in a different deserialized state
                                Memory memory = Memory.wrap(ByteBuffers.toByteArray(read.second()));
                                CompactSketch previousSketch = CompactSketch.wrap(memory);
                                CompactSketch newSketch = collectedNdvSketches.get(fieldId);
                                Preconditions.checkNotNull(newSketch, "ndvSketches.get(fieldId) is null");
                                ndvSketches.put(fieldId, SetOperation.builder().buildUnion().union(previousSketch, newSketch));
                            }
                        } catch (IOException exception) {
                            throw new UncheckedIOException(exception);
                        }
                    }
                }

                return new CollectedStatistics(ndvSketches.build());
            default:
                return collectedStatistics;
        }
    }

    private void copyRetainedStatistics(
            FileIO fileIO,
            StatisticsFile previousStatisticsFile,
            Set<Integer> validFieldIds,
            Set<Integer> columnsWithNewNdvSketches,
            PuffinWriter writer) {
        boolean anythingRetained = previousStatisticsFile.blobMetadata().stream()
                .anyMatch(blobMetadata ->
                        isBlobRetained(blobMetadata.type(), blobMetadata.fields(), validFieldIds, columnsWithNewNdvSketches));

        if (anythingRetained) {
            try (PuffinReader reader = Puffin.read(fileIO.newInputFile(previousStatisticsFile.path()))
                    .withFileSize(previousStatisticsFile.fileSizeInBytes())
                    .withFooterSize(previousStatisticsFile.fileFooterSizeInBytes())
                    .build()) {
                List<BlobMetadata> retained = reader.fileMetadata().blobs().stream()
                        .filter(blobMetadata -> isBlobRetained(
                                blobMetadata.type(), blobMetadata.inputFields(), validFieldIds, columnsWithNewNdvSketches))
                        .collect(toImmutableList());
                for (Pair<BlobMetadata, ByteBuffer> read : reader.readAll(retained)) {
                    String compressionCodec = read.first().compressionCodec();
                    writer.add(new Blob(
                            read.first().type(),
                            read.first().inputFields(),
                            read.first().snapshotId(),
                            read.first().sequenceNumber(),
                            read.second(),
                            compressionCodec == null
                                    ? null
                                    : tryGetCompressionCodec(compressionCodec).orElse(ZSTD),
                            read.first().properties()));
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private boolean isBlobRetained(
            String blobType,
            List<Integer> fields,
            Set<Integer> validFieldIds,
            Set<Integer> columnsWithNewNdvSketches) {
        if (!blobType.equals(APACHE_DATASKETCHES_THETA_V1)) {
            return true;
        }
        if (fields.size() != 1) {
            return true;
        }
        Integer fieldId = getOnlyElement(fields);
        return validFieldIds.contains(fieldId) &&
                !columnsWithNewNdvSketches.contains(fieldId);
    }

    private static Optional<PuffinCompressionCodec> tryGetCompressionCodec(String name) {
        Preconditions.checkNotNull(name, "name is null");
        try {
            return Optional.of(PuffinCompressionCodec.forName(name));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    static boolean maySnapshotHaveData(Table table, long snapshotId, FileIO fileIo) {
        Snapshot snapshot = table.snapshot(snapshotId);
        if (snapshot.summary().containsKey(TOTAL_RECORDS_PROP)) {
            return Long.parseLong(snapshot.summary().get(TOTAL_RECORDS_PROP)) != 0;
        }

        for (ManifestFile dataManifest : snapshot.dataManifests(fileIo)) {
            if (dataManifest.hasExistingFiles() || dataManifest.hasAddedFiles()) {
                return true;
            }
        }
        return false;
    }

}
