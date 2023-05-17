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

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.common.Config;
import com.starrocks.connector.statistics.ConnectorColumnStatisticLoader;
import com.starrocks.connector.statistics.ExternalTableColumnKey;
import com.starrocks.connector.statistics.ExternalTableColumnStat;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public abstract class CachingBaseMetastoreCatalog extends BaseMetastoreCatalog {
    private static final Logger LOG = LogManager.getLogger(CachingBaseMetastoreCatalog.class);
    private final Cache<TableIdentifier, TableMetadata> cacheTableMetadatas;
    private final AsyncLoadingCache<ExternalTableColumnKey, Optional<ExternalTableColumnStat>> cacheTableStats;

    public CachingBaseMetastoreCatalog() {
        Caffeine<Object, Object> builder = Caffeine.newBuilder();
        cacheTableMetadatas = builder.maximumSize(Config.iceberg_meta_cache_size)
                .expireAfterAccess(Config.iceberg_meta_cache_ttl_s, TimeUnit.SECONDS)
                .build();
        
        cacheTableStats = Caffeine.newBuilder().maximumSize(Config.iceberg_meta_cache_size)
                .expireAfterAccess(Config.iceberg_meta_cache_ttl_s, TimeUnit.SECONDS)
                .buildAsync(new ConnectorColumnStatisticLoader());
    }

    @Override
    public TableOperations newTableOps(TableIdentifier tableIdentifier) {
        if (Config.enable_iceberg_meta_cache) {
            TableMetadata tableMetadata = cacheTableMetadatas.get(tableIdentifier, k -> doGetTableMetadata(tableIdentifier));
            return doNewCachingTableOps(tableIdentifier, tableMetadata);
        } else {
            return doNewTableOps(tableIdentifier);
        }
    }

    protected abstract TableOperations doNewTableOps(TableIdentifier tableIdentifier);

    protected abstract TableOperations doNewCachingTableOps(TableIdentifier tableIdentifier, TableMetadata tableMetadata);

    protected TableMetadata doGetTableMetadata(TableIdentifier tableIdentifier) {
        return doNewTableOps(tableIdentifier).current();
    }

    public void updateMetadata(TableIdentifier tableIdentifier, TableMetadata tableMetadata) {
        cacheTableMetadatas.put(tableIdentifier, tableMetadata);
    }

    public List<Optional<ExternalTableColumnStat>> getColumnStatistics(String tableUUID, List<String> columns) {
        List<ExternalTableColumnKey> keys = new ArrayList<>();
        for (String column : columns) {
            keys.add(new ExternalTableColumnKey(tableUUID, column));
        }
        try {
            CompletableFuture<Map<ExternalTableColumnKey, Optional<ExternalTableColumnStat>>> result =
                    cacheTableStats.getAll(keys);
            if (result.isDone()) {
                List<Optional<ExternalTableColumnStat>> columnStatistics = new ArrayList<>();
                Map<ExternalTableColumnKey, Optional<ExternalTableColumnStat>> realResult;
                realResult = result.get();
                for (String column : columns) {
                    Optional<ExternalTableColumnStat> columnStatistic =
                            realResult.getOrDefault(new ExternalTableColumnKey(tableUUID, column), Optional.empty());
                    columnStatistics.add(columnStatistic);
                }
                return columnStatistics;
            } else {
                return getDefaultColumnStatisticList(columns);
            }
        } catch (Exception e) {
            LOG.warn(e);
            return getDefaultColumnStatisticList(columns);
        }
    }

    private List<Optional<ExternalTableColumnStat>> getDefaultColumnStatisticList(List<String> columns) {
        List<Optional<ExternalTableColumnStat>> columnStatisticList = new ArrayList<>();
        for (int i = 0; i < columns.size(); ++i) {
            columnStatisticList.add(Optional.empty());
        }
        return columnStatisticList;
    }
}
