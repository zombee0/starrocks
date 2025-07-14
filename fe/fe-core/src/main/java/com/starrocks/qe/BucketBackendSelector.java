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

package com.starrocks.qe;

import com.starrocks.common.StarRocksException;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class BucketBackendSelector implements BackendSelector {
    public static final Logger LOG = LogManager.getLogger(HDFSBackendSelector.class);

    private final ScanNode scanNode;
    private final List<TScanRangeLocations> locations;
    private final FragmentScanRangeAssignment assignment;
    private final WorkerProvider workerProvider;
    private final ConnectContext connectContext;
    private final boolean useIncrementalScanRanges;

    public BucketBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                               FragmentScanRangeAssignment assignment, WorkerProvider workerProvider,
                               boolean useIncrementalScanRanges,
                               ConnectContext connectContext) {
        this.scanNode = scanNode;
        this.locations = locations;
        this.assignment = assignment;
        this.workerProvider = workerProvider;
        this.connectContext = connectContext;
        this.useIncrementalScanRanges = useIncrementalScanRanges;
    }

    @Override
    public void computeScanRangeAssignment() throws StarRocksException {

        return;
    }

}
