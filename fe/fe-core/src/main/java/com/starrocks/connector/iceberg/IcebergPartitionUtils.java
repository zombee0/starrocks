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

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;

import java.util.List;

public class IcebergPartitionUtils {

    public static class IcebergPartition {
        private PartitionSpec spec;
        private StructLike data;

        IcebergPartition(PartitionSpec spec, StructLike data) {
            this.spec = spec;
            this.data = data;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof IcebergPartition)) {
                return false;
            }
            IcebergPartition partition = (IcebergPartition) o;
            if (!(this.spec.equals(partition.spec))) {
                return false;
            }
            if (this.data.size() != partition.data.size()) {
                return false;
            }
            for (int i = 0; i < this.data.size(); i++) {
                if (this.data.get(i, this.))
            }
        }
    }

    public static List<IcebergPartition> changedPartition(Table table, long from) {
        ImmutableList.Builder<IcebergPartition> builder = ImmutableList.builder();
        Snapshot snapShot = table.currentSnapshot();
        if (snapShot.timestampMillis() >= from) {
            while (snapShot.parentId() != null) {
                snapShot = table.snapshot(snapShot.parentId());
                if (snapShot.timestampMillis() < from) {
                    break;
                }
            }
            if (snapShot.timestampMillis() < from) {
                IncrementalChangelogScan incrementalChangelogScan = table.newIncrementalChangelogScan().
                        fromSnapshotExclusive(snapShot.snapshotId());
                try (CloseableIterable<ChangelogScanTask> tasks = incrementalChangelogScan.planFiles()) {
                    for (ChangelogScanTask task : tasks) {
                        PartitionSpec spec = task.asFileScanTask().spec();
                        StructLike data = task.asFileScanTask().partition();
                        builder.add(new IcebergPartition(spec, data));
                    }
                } catch (Exception ignore) {
                    // ignore
                }
            } else {
                try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                    for (FileScanTask task : tasks) {
                        PartitionSpec spec = task.spec();
                        StructLike data = task.partition();
                        builder.add(new IcebergPartition(spec, data));
                    }
                } catch (Exception ignore) {
                    // ignore
                }
            }
        }
        return builder.build();
    }
}
