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


package com.starrocks.connector.iceberg;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.Util;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.thrift.TIcebergColumnStats;
import com.starrocks.thrift.TIcebergDataFile;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.starrocks.catalog.IcebergTable.ICEBERG_CATALOG_TYPE;
import static com.starrocks.catalog.IcebergTable.ICEBERG_IMPL;
import static com.starrocks.catalog.IcebergTable.ICEBERG_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergUtil.convertColumnType;
import static com.starrocks.connector.iceberg.IcebergUtil.fromSRType;
import static com.starrocks.connector.iceberg.IcebergUtil.getIcebergCustomCatalog;
import static com.starrocks.connector.iceberg.IcebergUtil.getIcebergGlueCatalog;
import static com.starrocks.connector.iceberg.IcebergUtil.getIcebergHiveCatalog;
import static com.starrocks.connector.iceberg.IcebergUtil.getIcebergRESTCatalog;

public class IcebergMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(IcebergMetadata.class);
    private String metastoreURI;
    private String catalogType;
    private String catalogImpl;
    private final String catalogName;
    private IcebergCatalog icebergCatalog;
    private Map<String, String> customProperties;
    private Transaction transaction;

    public IcebergMetadata(String catalogName, Map<String, String> properties) {
        this.catalogName = catalogName;
        if (IcebergCatalogType.HIVE_CATALOG == IcebergCatalogType.fromString(properties.get(ICEBERG_CATALOG_TYPE))) {
            catalogType = properties.get(ICEBERG_CATALOG_TYPE);
            metastoreURI = properties.get(ICEBERG_METASTORE_URIS);
            icebergCatalog = getIcebergHiveCatalog(metastoreURI, properties);
            Util.validateMetastoreUris(metastoreURI);
        } else if (IcebergCatalogType.CUSTOM_CATALOG ==
                IcebergCatalogType.fromString(properties.get(ICEBERG_CATALOG_TYPE))) {
            catalogType = properties.get(ICEBERG_CATALOG_TYPE);
            catalogImpl = properties.get(ICEBERG_IMPL);
            icebergCatalog = getIcebergCustomCatalog(catalogImpl, properties);
            properties.remove(ICEBERG_CATALOG_TYPE);
            properties.remove(ICEBERG_IMPL);
            customProperties = properties;
        } else if (IcebergCatalogType.GLUE_CATALOG == IcebergCatalogType.fromString(properties.get(ICEBERG_CATALOG_TYPE))) {
            catalogType = properties.get(ICEBERG_CATALOG_TYPE);
            icebergCatalog = getIcebergGlueCatalog(catalogName, properties);
        } else if (IcebergCatalogType.REST_CATALOG == IcebergCatalogType.fromString(properties.get(ICEBERG_CATALOG_TYPE))) {
            catalogType = properties.get(ICEBERG_CATALOG_TYPE);
            icebergCatalog = getIcebergRESTCatalog(properties);
        } else {
            throw new RuntimeException(String.format("Property %s is missing or not supported now.",
                    ICEBERG_CATALOG_TYPE));
        }
    }

    @Override
    public List<String> listDbNames() {
        return icebergCatalog.listAllDatabases();
    }

    @Override
    public Database getDb(String dbName) {
        try {
            return icebergCatalog.getDB(dbName);
        } catch (InterruptedException | TException e) {
            LOG.error("Failed to get iceberg database " + dbName, e);
            return null;
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        List<TableIdentifier> tableIdentifiers = icebergCatalog.listTables(Namespace.of(dbName));
        return tableIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        try {
            org.apache.iceberg.Table icebergTable
                    = icebergCatalog.loadTable(IcebergUtil.getIcebergTableIdentifier(dbName, tblName));
            // Submit a future task for refreshing
            GlobalStateMgr.getCurrentState().getIcebergRepository().refreshTable(icebergTable);
            if (IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.CUSTOM_CATALOG)) {
                return IcebergUtil.convertCustomCatalogToSRTable(icebergTable, catalogImpl, catalogName, dbName,
                        tblName, customProperties);
            } else if (IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.GLUE_CATALOG)) {
                return IcebergUtil.convertGlueCatalogToSRTable(icebergTable, catalogName, dbName, tblName);
            } else if (IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.REST_CATALOG)) {
                return IcebergUtil.convertRESTCatalogToSRTable(icebergTable, catalogName, dbName, tblName);
            } else {
                return IcebergUtil.convertHiveCatalogToSRTable(icebergTable, metastoreURI, catalogName, dbName, tblName);
            }
        } catch (DdlException e) {
            LOG.error("Failed to get iceberg table " + IcebergUtil.getIcebergTableIdentifier(dbName, tblName), e);
            return null;
        }
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName) {
        org.apache.iceberg.Table icebergTable
                = icebergCatalog.loadTable(IcebergUtil.getIcebergTableIdentifier(dbName, tblName));
        if (!IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.HIVE_CATALOG)
                && !IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.REST_CATALOG)) {
            throw new StarRocksIcebergException(
                    "Do not support get partitions from catalog type: " + catalogType);
        }
        if (icebergTable.spec().fields().stream()
                .anyMatch(partitionField -> !partitionField.transform().isIdentity())) {
            throw new StarRocksIcebergException(
                    "Do not support get partitions from No-Identity partition transform now");
        }

        return IcebergUtil.getIdentityPartitionNames(icebergTable);
    }

    @Override
    public void createTable(CreateTableStmt stmt) {
        transaction = newCreateTableTxn(stmt);
        transaction.newFastAppend().commit();
        transaction.commitTransaction();
    }

    public Transaction newCreateTableTxn(CreateTableStmt stmt) {
        Schema schema = toIcebergSchema(stmt.getColumns());
        String dbName = stmt.getDbName();
        String tblName = stmt.getTableName();
        PartitionSpec partitionSpec = parsePartitionFields(schema, ((RangePartitionDesc) stmt.getPartitionDesc()).getPartitionColNames());
        String targetPath = icebergCatalog.defaultTableLocation(dbName, tblName).toString();
        return icebergCatalog.newCreateTableTransaction(dbName, tblName, schema, partitionSpec, targetPath, stmt.getProperties());
    }

    public static PartitionSpec parsePartitionFields(Schema schema, List<String> fields) {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        for (String field : fields) {
            builder.identity(field);
        }
        return builder.build();
    }

    public static Schema toIcebergSchema(List<Column> columns) {
        List<Types.NestedField> icebergColumns = new ArrayList<>();
        for (Column column : columns) {
            int index = icebergColumns.size();
            org.apache.iceberg.types.Type type = fromSRType(column.getType());
            Types.NestedField field = Types.NestedField.of(index, false, column.getName(), type, column.getComment());
            icebergColumns.add(field);
        }
        org.apache.iceberg.types.Type icebergSchema = Types.StructType.of(icebergColumns);
        AtomicInteger nextFieldId = new AtomicInteger(1);
        icebergSchema = TypeUtil.assignFreshIds(icebergSchema, nextFieldId::getAndIncrement);
        return new Schema(icebergSchema.asStructType().fields());
    }

    public void finishInsert(String dbName, String tblName, List<TIcebergDataFile> dataFiles) {
        IcebergTable table = (IcebergTable) getTable(dbName, tblName);
        org.apache.iceberg.Table nativeTbl = table.getIcebergTable();
        AppendFiles appendFiles = nativeTbl.newAppend();
        for (TIcebergDataFile dataFile : dataFiles) {
            PartitionSpec partitionSpec = nativeTbl.spec();
            Metrics metrics = buildDataFileMetrics(dataFile);
            PartitionData partitionData = partitionDataFromPath(dataFile.partition_path, partitionSpec, nativeTbl.location() + "/data/");
            DataFiles.Builder builder =
                DataFiles.builder(partitionSpec)
                        .withMetrics(metrics)
                        .withPath(dataFile.path)
                        .withFormat(FileFormat.PARQUET)
                        .withRecordCount(dataFile.record_count)
                        .withFileSizeInBytes(dataFile.file_size_in_bytes)
                        .withPartition(partitionData)
                        .withSplitOffsets(dataFile.split_offsets);
            appendFiles.appendFile(builder.build());
        }
        appendFiles.commit();
    }

    private static Metrics buildDataFileMetrics(TIcebergDataFile dataFile) {
        TIcebergColumnStats stats = dataFile.column_stats;
        return new Metrics(dataFile.record_count, stats.columnSizes, stats.valueCounts,
                stats.nullValueCounts, null, stats.lowerBounds, stats.upperBounds);
    }

    public static class PartitionData implements StructLike {
        private final Object[] values;

        private PartitionData(int size) {
            this.values = new Object[size];
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T get(int pos, Class<T> javaClass) {
            return javaClass.cast(values[pos]);
        }

        @Override
        public <T> void set(int pos, T value) {
            if (value instanceof ByteBuffer) {
                // ByteBuffer is not Serializable
                ByteBuffer buffer = (ByteBuffer) value;
                byte[] bytes = new byte[buffer.remaining()];
                buffer.duplicate().get(bytes);
                values[pos] = bytes;
            } else {
                values[pos] = value;
            }
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            PartitionData that = (PartitionData) other;
            return Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }

    public static PartitionData partitionDataFromPath(String path, PartitionSpec spec, String tableLocation) {
        PartitionData data = new PartitionData(spec.fields().size());
        String relaPath = PartitionUtil.getSuffixName(tableLocation, path.substring(0, path.length() - 1));
        String[] partitions = relaPath.split("/", -1);
        List<PartitionField> partitionFields = spec.fields();

        for (int i = 0; i < partitions.length; i += 1) {
            PartitionField field = partitionFields.get(i);
            String[] parts = partitions[i].split("=", 2);
            if (parts.length != 2 || parts[0] == null || !field.name().equals(parts[0])) {
                continue;
            }
            Type sourceType = spec.schema().findType(field.sourceId());
            data.set(i, Conversions.fromPartitionString(sourceType, parts[1]));
        }
        return data;
    }
}
