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


#include "exec/vectorized/parquet_writer.h"

namespace starrocks::vectorized {

ParquetWriterWrap::ParquetWriterWrap(const TableInfo* tableInfo, const PartitionInfo* partitionInfo) {
    init_parquet_writer(tableInfo, partitionInfo);
}

Status ParquetWriterWrap::init_parquet_writer(const TableInfo* tableInfo, const PartitionInfo* partitionInfo) {
    // init fs property schema with tableInfo and partitionInfo
    // ::parquet::schema::NodeVector fields;
    // ::parquet::Repetition::type rep_type;
    // ::parquet::Type::type data_type;

    return Status::OK();
}

::parquet::Type::type convert_type(const LogicalType type) {
    switch (type) {
    case TYPE_BOOLEAN:
        return ::parquet::Type::BOOLEAN;
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
        return ::parquet::Type::INT32;
    case TYPE_BIGINT:
    case TYPE_DATE:
    case TYPE_DATETIME:
        return ::parquet::Type::INT64;
    case TYPE_FLOAT:
        return ::parquet::Type::FLOAT;
    case TYPE_DOUBLE:
        return ::parquet::Type::DOUBLE;
    default:
        throw std::runtime_error("Not Supported yet");
    }
}


std::string ParquetWriterWrap::get_new_file_name() {
    _current_file = "test";
    return _current_file;
}

Status ParquetWriterWrap::new_file_writer() {
    std::string file_name = get_new_file_name();
    ASSIGN_OR_RETURN(auto writable_file, _fs->new_writable_file(file_name));
    _writer = std::make_shared<starrocks::parquet::FileWriter>(std::move(writable_file), _properties, _schema);
    return Status::OK();
}

Status ParquetWriterWrap::append_chunk(vectorized::Chunk* chunk) {
    if (_writer == nullptr) {
        new_file_writer();
    }
    // exceed file size
    if (_writer->get_written_bytes() > _max_file_size) {
        close_current_writer();
        new_file_writer();
    }
    auto st = _writer->write(chunk);
    return st;
}

Status ParquetWriterWrap::close_current_writer() {
    auto st = _writer->close();
    if (!st.ok())
    {
        return st;
    }
    _metadatas.emplace_back(_writer->metadata());
    return Status::OK();
}

Status ParquetWriterWrap::close() {
    if (_writer != nullptr) {
        auto st = close_current_writer();
        if (!st.ok()) {
            return st;
        }
    }
    _writer.reset();
    return Status::OK();
}

} // starrocks::vectorized