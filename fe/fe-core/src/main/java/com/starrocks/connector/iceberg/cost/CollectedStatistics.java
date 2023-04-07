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

import com.google.common.collect.ImmutableMap;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.hadoop.util.Preconditions;

import java.util.Map;

public class CollectedStatistics {

    private Map<Integer, CompactSketch> ndvSketches;
    public CollectedStatistics(Map<Integer, CompactSketch> ndvSketches) {
        Preconditions.checkNotNull(ndvSketches);
        this.ndvSketches = ImmutableMap.copyOf(ndvSketches);
    }

    public Map<Integer, CompactSketch> getNdvSketches() {
        return ndvSketches;
    }
}
