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

class TableSinkIOBuffer final : public SinkIOBuffer {
public:
    TableSinkIOBuffer() : SinkIOBuffer(1) {}

    ~TableSinkIOBuffer() override = default;

    Status prepare(RuntimeState* state, RuntimeProfile* parent_profile) override;

    void close(RuntimeState* state) override;

private:
    void _process_chunk(bthread::TaskIterator<const vectorized::ChunkPtr>& iter) override;
    std::shared_ptr<vectorized::ParquetWriterWrap> _writer;
};

Status TableSinkIOBuffer::prepare(RuntimeState* state, RuntimeProfile* parent_profile) {
    bool expected = false;
    if (!_is_prepared.compare_exchange_strong(expected, true)) {
        return Status::OK();
    }

    _state = state;
    // _writer = std::make_shared<vectorized::ParquetWriterWrap>(tableInfo, partitionInfo);

    bthread::ExecutionQueueOptions options;
    options.executor = SinkIOExecutor::instance();
    _exec_queue_id = std::make_unique<bthread::ExecutionQueueId<const vectorized::ChunkPtr>>();
    int ret = bthread::execution_queue_start<const vectorized::ChunkPtr>(_exec_queue_id.get(), &options,
                                                                         &TableSinkIOBuffer::execute_io_task, this);
    if (ret != 0) {
        _exec_queue_id.reset();
        return Status::InternalError("start execution queue error");
    }
    return Status::OK();
}

void TableSinkIOBuffer::close(RuntimeState* state) {
    if (_writer != nullptr) {
        if (Status status = _writer->close(); !status.ok()) {
            set_io_status(status);
        }
        // get metadata
        _writer.reset();
    }

    SinkIOBuffer::close(state);
}

void TableSinkIOBuffer::_process_chunk(bthread::TaskIterator<const vectorized::ChunkPtr>& iter) {
    --_num_pending_chunks;
    // close is already done, just skip
    if (_is_finished) {
        return;
    }

    // cancelling has happened but close is not invoked
    if (_is_cancelled && !_is_finished) {
        close(_state);
        return;
    }

    const auto& chunk = *iter;
    if (chunk == nullptr) {
        // this is the last chunk
        close(_state);
        return;
    }
    if (Status status = _writer->append_chunk(chunk.get()); !status.ok()) {
        set_io_status(status);
        close(_state);
    }
}

Status TableSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

void TableSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
}

bool TableSinkOperator::pending_finish() const {
    for (const auto& p : partition_buffer_map) {
        if (!p.second->is_finished()) {
            return true;
        }
    }
    return false;
}

bool TableSinkOperator::is_finished() const {
    for (const auto& p : partition_buffer_map) {
        if (!p.second->is_finished()) {
            return false;
        }
    }
    return true;
}

bool TableSinkOperator::need_input() const {
    for (const auto& p : partition_buffer_map) {
        if (!p.second->need_input()) {
            return false;
        }
    }
    return true;
}

Status TableSinkOperator::set_finishing(RuntimeState* state) {
    for (auto& p : partition_buffer_map)
    {
        p.second->set_finishing();
    }
    return Status::OK();
}

Status TableSinkOperator::set_cancelled(RuntimeState* state) {
    for (auto& p : partition_buffer_map)
    {
        p.second->cancel_one_sinker();
    }
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> TableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from table sink operator");
}

Status TableSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    // 1. partition
    // 2. check buffer if exist
    // 3. select buffer and append_chunk
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