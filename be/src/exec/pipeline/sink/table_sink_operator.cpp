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

#include "table_sink_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/sink/sink_io_buffer.h"
#include "exec/vectorized/parquet_writer.h"

namespace starrocks::pipeline {

Status TableSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

void TableSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
}

bool TableSinkOperator::pending_finish() const {
    return false;
}

bool TableSinkOperator::is_finished() const {
    return true;
}

bool TableSinkOperator::need_input() const {
    return true;
}

Status TableSinkOperator::set_finishing(RuntimeState* state) {
    return Status::OK();
}

Status TableSinkOperator::set_cancelled(RuntimeState* state) {
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> TableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from table sink operator");
}

Status TableSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    // 1. partition
    // 2. check writer if exist
    // 3. select writer and append_chunk
    // sink_buffer->append_chunk(state, chunk);
    return Status::OK();
}

TableSinkOperatorFactory::TableSinkOperatorFactory(int32_t id, FragmentContext* const fragment_ctx)
            : OperatorFactory(id, "table_sink", Operator::s_pseudo_plan_node_id_for_result_sink),
              _fragment_ctx(fragment_ctx) {}

Status TableSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    return Status::OK();
}

void TableSinkOperatorFactory::close(RuntimeState* state) {
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline