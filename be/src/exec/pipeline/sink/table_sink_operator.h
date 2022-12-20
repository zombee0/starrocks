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

#pragma once

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"

namespace starrocks {

namespace pipeline {

class TableSinkIOBuffer;
using PartitionId = int32_t;

class TableSinkOperator final : public Operator {
public:
    TableSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : Operator(factory, id, "table_sink", plan_node_id, driver_sequence) {}

    ~TableSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;
    bool pending_finish() const override;

    Status set_cancelled(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    std::map<PartitionId, std::shared_ptr<TableSinkIOBuffer>> partition_buffer_map;
};

class TableSinkOperatorFactory final : public OperatorFactory {
public:
    TableSinkOperatorFactory(int32_t id, FragmentContext* const fragment_ctx);

    ~TableSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TableSinkOperator>(this, _id, _plan_node_id, driver_sequence);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    FragmentContext* const _fragment_ctx;
};

} // namespace pipeline
} // namespace starrocks
