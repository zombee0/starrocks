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

#include "exec/pipeline/operator.h"
#include "exec/pipeline/fragment_context.h"
#include <parquet/arrow/writer.h>

#include <utility>
#include <gen_cpp/DataSinks_types.h>
#include "exec/vectorized/parquet_writer.h"
#include "common/logging.h"


namespace starrocks {
namespace pipeline {

class IcebergTableSinkOperator final : public Operator {

public:
    IcebergTableSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence, std::string location,
                             std::string file_format, std::string compression_codec, int32_t table_id, const std::shared_ptr<::parquet::schema::GroupNode>& schema,
                             FragmentContext* fragment_ctx, std::vector<ExprContext*>& output_expr_ctxs, const vector<ExprContext*>& partition_output_expr)
    : Operator(factory, id, "iceberg_table_sink", plan_node_id, driver_sequence),
    _location(std::move(location)),
    _file_format(std::move(file_format)),
    _compression_codec(std::move(compression_codec)),
    _target_table_id(table_id),
    _schema(std::move(schema)),
    _fragment_ctx(fragment_ctx),
    _output_expr_ctxs(output_expr_ctxs),
    _partition_expr_ctxs(partition_output_expr) {}

    ~IcebergTableSinkOperator() override = default;

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
    std::string _value_to_string(const ColumnPtr& column, size_t index);

    std::string _location;
    std::string _file_format;
    std::string _compression_codec;
    int64_t _target_table_id;
    std::shared_ptr<::parquet::schema::GroupNode> _schema;
    FragmentContext* _fragment_ctx = nullptr;
    std::vector<ExprContext*> _output_expr_ctxs;
    std::vector<ExprContext*> _partition_expr_ctxs;
    std::unordered_map<std::string, starrocks::vectorized::ParquetWriterWrap*> _writers;
    std::atomic<bool> _is_finished = false;
};


class IcebergTableSinkOperatorFactory final : public OperatorFactory {
public:
    IcebergTableSinkOperatorFactory(int32_t id, vector<TExpr> t_output_expr, FragmentContext* fragment_ctx,
                                    const TIcebergTableSink& t_iceberg_table_sink, vector<TExpr> partition_output_expr);

    ~IcebergTableSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<IcebergTableSinkOperator>(this, _id, _plan_node_id, driver_sequence, _location, _file_format,
                                                          _compression_codec, _target_table_id, _schema, _fragment_ctx, _output_expr_ctxs, _partition_expr_ctxs);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    void parse_schema(const std::vector<TParquetSchema>& p_decompose_data_sink_to_operatorarquet_schemas);

    static void build_schema_repetition_type(
            ::parquet::Repetition::type& parquet_repetition_type,
            const TParquetRepetitionType::type& column_repetition_type);

    static void build_schema_data_type(::parquet::Type::type& parquet_data_type,
                                       const TParquetDataType::type& column_data_type);

private:
    std::vector<TExpr> _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;
    TIcebergTableSink _t_iceberg_table_sink;
    FragmentContext* _fragment_ctx = nullptr;

    std::string _location;
    std::string _file_format;
    std::string _compression_codec;
    int64_t _target_table_id;
    std::shared_ptr<::parquet::schema::GroupNode> _schema;
    std::vector<TExpr> _partition_output_expr;
    std::vector<ExprContext*> _partition_expr_ctxs;
};

}
}