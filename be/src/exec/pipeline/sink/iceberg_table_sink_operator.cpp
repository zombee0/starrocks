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

#include "iceberg_table_sink_operator.h"

namespace starrocks::pipeline {

Status IcebergTableSinkOperator::prepare(RuntimeState* state) {
    LOG(WARNING) << "==========IcebergTableSinkOperator [prepare]==========";
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

void IcebergTableSinkOperator::close(RuntimeState* state) {
    LOG(WARNING) << "==========IcebergTableSinkOperator [close]=========";

    for (auto &i : _writers) {
        if (!i.second->closed()) {
            LOG(WARNING) << "============[error: close op -> close writer]==============";
            i.second->close(state);
        }
        //for (auto &j : i.second->_data_files) {
        //    state->add_iceberg_data_file(j);
        //}
    }
    Operator::close(state);
//    LOG(WARNING) << "===========[close finish]==============";
}

bool IcebergTableSinkOperator::need_input() const {
    LOG(WARNING) << "==========IcebergTableSinkOperator [need input]===============";
    for (auto &i : _writers) {
        if (!i.second->writable()) {
//            LOG(WARNING) << "==========[writable false]===============";
            return false;
        }
    }
//    LOG(WARNING) << "============[need input finish]=============";

    return true;
}

bool IcebergTableSinkOperator::is_finished() const {
    LOG(WARNING) << "===========IcebergTableSinkOperator [is_finished]==============";
    if (_writers.size() == 0) {
//        LOG(WARNING) << "===========[writer size is 0]==============";

        return _is_finished.load();
    }
    for (auto &i : _writers) {
        if (!i.second->closed()) {
//            LOG(WARNING) << "============[is_finished:closed is false]=============";
            return false;
        }
    }
//    LOG(WARNING) << "============[is_finished finish]=============";

    return true;
}

Status IcebergTableSinkOperator::set_finishing(RuntimeState* state) {
    LOG(WARNING) << "=============IcebergTableSinkOperator [set_finishing]============";

    for (auto &i : _writers) {
        if (!i.second->closed()) {
//            LOG(WARNING) << "============[set_finishing:closed is false]=============";
            i.second->close(state);
        }
    }

    if (_writers.size() == 0) {
        _is_finished = true;
    }
//    LOG(WARNING) << "===========[set_finishing finish]==============";

    return Status::OK();
}

bool IcebergTableSinkOperator::pending_finish() const {
    LOG(WARNING) << "=============IcebergTableSinkOperator [pending_finish]============";

    return !is_finished();
}

Status IcebergTableSinkOperator::set_cancelled(RuntimeState* state) {
    LOG(WARNING) << "============IcebergTableSinkOperator [set_cancelled]=============";

    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> IcebergTableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from mysql table sink operator");
}

Status IcebergTableSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    if (_partition_expr_ctxs.empty()) {
        if (_writers.find("") == _writers.end()) {
            starrocks::vectorized::TableInfo tableInfo;
            tableInfo._table_location = _location;
            tableInfo._file_format = _file_format;
            tableInfo._schema = _schema;
            starrocks::vectorized::PartitionInfo partitionInfo;
            auto writer = new vectorized::ParquetWriterWrap(tableInfo, partitionInfo, _output_expr_ctxs, _common_metrics.get());
            _writers.insert({"", writer});
            LOG(WARNING) << "==========[insert writer on [" << "" << "]===============";
        }
        _writers[""]->append_chunk(chunk.get(), state);
        return Status::OK();
    }

    //std::unordered_map<std::string, std::vector<uint32_t>> partition_row_map;
    //std::unordered_map<std::string, vectorized::PartitionInfo> partition_map;
    //
    //vectorized::Columns partitions_columns;
    //partitions_columns.resize(_partition_expr_ctxs.size());
    //for (size_t i = 0; i < partitions_columns.size(); ++i) {
    //    ASSIGN_OR_RETURN(partitions_columns[i], _partition_expr_ctxs[i]->evaluate(chunk.get()));
    //    DCHECK(partitions_columns[i] != nullptr);
    //}
    //
    //auto* iceberg_table_desc = dynamic_cast<const IcebergTableDescriptor*>(state->desc_tbl().get_table_descriptor(_target_table_id));
    //std::vector<std::string> partition_column_names = iceberg_table_desc->partition_columns_names();
    //
    //std::vector<std::string> partition_column_values;
    //for (size_t i = 0; i < chunk->num_rows(); ++i) {
    //    for (const vectorized::ColumnPtr& column : partitions_columns) {
    //        partition_column_values.emplace_back(_value_to_string(column, i));
    //    }
    //    vectorized::PartitionInfo partitionInfo = {partition_column_names, partition_column_values};
    //    std::string key = partitionInfo.partition_dir();
    //
    //    if (partition_row_map.find(key) == partition_row_map.end()) {
    //        partition_row_map.insert({key, std::vector<uint32_t>()});
    //        partition_map.insert({key, partitionInfo});
    //    }
    //    partition_row_map[key].emplace_back(i);
    //}

    //for (auto & i : partition_row_map) {
    //    int32_t size = i.second.size();
    //    std::string key = i.first;
    //    auto chunk_to_submit = chunk->clone_empty_with_slot(size);
    //    chunk_to_submit->append_selective(*chunk, i.second.data(), 0, size);
    //    if (_writers.find(key) == _writers.end()) {
    //        starrocks::vectorized::TableInfo tableInfo;
    //        tableInfo._table_location = _location;
    //        tableInfo._file_format = _file_format;
    //        tableInfo._schema = _schema;
    //        starrocks::vectorized::PartitionInfo partitionInfo = partition_map[key];
    //        auto writer = new vectorized::ParquetWriterWrap(tableInfo, partitionInfo, _output_expr_ctxs, _common_metrics.get());
    //        _writers.insert({key, writer});
    //        LOG(WARNING) << "==========[insert writer on [" << key << "]===============";
    //    }
    //
    //    _writers[key]->append_chunk(chunk_to_submit.get(), state);
    //}

    auto partition_column = _partition_expr_ctxs.front()->evaluate(chunk.get());
    std::unordered_map<std::string, std::vector<uint32_t>> partition_row_map;
    auto num_size = partition_column.value()->size();
    for (int i = 0; i < num_size; ++i) {
        std::string key = std::to_string(partition_column.value()->get(i).get_int32());
        if (partition_row_map.find(key) == partition_row_map.end()) {
            partition_row_map.insert({key, std::vector<uint32_t>()});
        }
        partition_row_map[key].emplace_back(i);
    }

    for (auto & i : partition_row_map) {
        int32_t size = i.second.size();
        std::string key = i.first;
        std::vector<std::string> keys = {key};
        auto chunk_to_submit = chunk->clone_empty_with_slot(size);
        chunk_to_submit->append_selective(*chunk, i.second.data(), 0, size);
        auto* iceberg_table_desc = dynamic_cast<const IcebergTableDescriptor*>(state->desc_tbl().get_table_descriptor(_target_table_id));
        std::vector<std::string> partition_column_names = iceberg_table_desc->partition_columns_names();
        if (_writers.find(key) == _writers.end()) {
            starrocks::vectorized::TableInfo tableInfo;
            tableInfo._table_location = _location;
            tableInfo._file_format = _file_format;
            tableInfo._schema = _schema;
            starrocks::vectorized::PartitionInfo partitionInfo = {partition_column_names, keys};
            auto writer = new vectorized::ParquetWriterWrap(tableInfo, partitionInfo, _output_expr_ctxs, _common_metrics.get());
            _writers.insert({key, writer});
            LOG(WARNING) << "==========[insert writer on [" << key << "]===============";

        }

        _writers[key]->append_chunk(chunk_to_submit.get(), state);
    }
//    LOG(WARNING) << "==========[push chunk finshed]===============";
    return Status::OK();
}

std::string IcebergTableSinkOperator::_value_to_string(const ColumnPtr& column, size_t index) {
    auto v = column->get(index);
    std::string res;
    v.visit([&](auto& variant) {
        std::visit(
            [&](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, Slice> ||
                        std::is_same_v<T, vectorized::TimestampValue> || std::is_same_v<T, vectorized::DateValue> ||
                        std::is_same_v<T, decimal12_t> || std::is_same_v<T, DecimalV2Value>) {
                    res = arg.to_string();
                } else if constexpr (std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> || std::is_same_v<T, float>
                        || std::is_same_v<T, double>) {
                    res = std::to_string(arg);
                }
                }, variant);
        });
    return res;
}

IcebergTableSinkOperatorFactory::IcebergTableSinkOperatorFactory(int32_t id, std::vector<TExpr> t_output_expr,
                                                             FragmentContext* fragment_ctx, const TIcebergTableSink& t_iceberg_table_sink,
                                                                 std::vector<TExpr> partition_output_expr)
    : OperatorFactory(id, "iceberg_table_sink", Operator::s_pseudo_plan_node_id_for_result_sink),
      _t_output_expr(std::move(t_output_expr)),
      _t_iceberg_table_sink(std::move(t_iceberg_table_sink)),
      _fragment_ctx(std::move(fragment_ctx)),
      _partition_output_expr(std::move(partition_output_expr)) {}

Status IcebergTableSinkOperatorFactory::prepare(RuntimeState* state) {
    LOG(WARNING) << "============IcebergTableSinkOperatorFactory [prepare]=============";
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _partition_output_expr, &_partition_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_partition_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_partition_expr_ctxs, state));

    _location = _t_iceberg_table_sink.location;
    _file_format = _t_iceberg_table_sink.file_format;
    _compression_codec = _t_iceberg_table_sink.compression_codec;
    _target_table_id = _t_iceberg_table_sink.target_table_id;
    parse_schema(_t_iceberg_table_sink.parquet_schemas);
//    LOG(WARNING) << "=========================";

    return Status::OK();
}

void IcebergTableSinkOperatorFactory::parse_schema(const std::vector<TParquetSchema> &parquet_schemas) {
    ::parquet::schema::NodeVector fields;
    ::parquet::Repetition::type parquet_repetition_type;
    ::parquet::Type::type parquet_data_type;
    for (const auto& parquet_schema : parquet_schemas) {
        build_schema_data_type(parquet_data_type, parquet_schema.schema_data_type);
        build_schema_repetition_type(parquet_repetition_type, parquet_schema.schema_repetition_type);
        ::parquet::schema::NodePtr nodePtr = ::parquet::schema::PrimitiveNode::Make(parquet_schema.schema_column_name,
                                                                                parquet_repetition_type,
                                                                                parquet_data_type,
                                                                                ::parquet::ConvertedType::NONE, -1, -1,
                                                                                -1, parquet_schema.field_id);
        fields.push_back(nodePtr);
    }
    _schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::REQUIRED, fields));
}

void IcebergTableSinkOperatorFactory::build_schema_data_type(::parquet::Type::type& parquet_data_type,
                                                const TParquetDataType::type& column_data_type) {
    switch (column_data_type) {
        case TParquetDataType::BOOLEAN: {
            parquet_data_type = ::parquet::Type::BOOLEAN;
            break;
        }
        case TParquetDataType::INT32: {
            parquet_data_type = ::parquet::Type::INT32;
            break;
        }
        case TParquetDataType::INT64: {
            parquet_data_type = ::parquet::Type::INT64;
            break;
        }
        case TParquetDataType::INT96: {
            parquet_data_type = ::parquet::Type::INT96;
            break;
        }
        case TParquetDataType::BYTE_ARRAY: {
            parquet_data_type = ::parquet::Type::BYTE_ARRAY;
            break;
        }
        case TParquetDataType::FLOAT: {
            parquet_data_type = ::parquet::Type::FLOAT;
            break;
        }
        case TParquetDataType::DOUBLE: {
            parquet_data_type = ::parquet::Type::DOUBLE;
            break;
        }
        case TParquetDataType::FIXED_LEN_BYTE_ARRAY: {
            parquet_data_type = ::parquet::Type::FIXED_LEN_BYTE_ARRAY;
            break;
        }
        default:
            parquet_data_type = ::parquet::Type::UNDEFINED;
    }
}

void IcebergTableSinkOperatorFactory::build_schema_repetition_type(
        ::parquet::Repetition::type& parquet_repetition_type,
        const TParquetRepetitionType::type& column_repetition_type) {
    switch (column_repetition_type) {
        case TParquetRepetitionType::REQUIRED: {
            parquet_repetition_type = ::parquet::Repetition::REQUIRED;
            LOG(WARNING) << "REQUIRED";
            break;
        }
        case TParquetRepetitionType::REPEATED: {
            parquet_repetition_type = ::parquet::Repetition::REPEATED;
            LOG(WARNING) << "REPEATED";
            break;
        }
        case TParquetRepetitionType::OPTIONAL: {
            parquet_repetition_type = ::parquet::Repetition::OPTIONAL;
            LOG(WARNING) << "OPTIONAL";
            break;
        }
        default:
            parquet_repetition_type = ::parquet::Repetition::UNDEFINED;
    }
}

void IcebergTableSinkOperatorFactory::close(RuntimeState* state) {
    LOG(WARNING) << "============IcebergTableSinkOperatorFactory [close]=============";
    Expr::close(_output_expr_ctxs, state);
    OperatorFactory::close(state);
    LOG(WARNING) << "===========[iceberg sink operator factory close]==============";

}

}

