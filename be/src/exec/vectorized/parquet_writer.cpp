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
#include "runtime/exec_env.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::vectorized {

ParquetWriterWrap::ParquetWriterWrap(const TableInfo* tableInfo, const PartitionInfo* partitionInfo) {
    init_parquet_writer(tableInfo, partitionInfo);
}

Status ParquetWriterWrap::init_parquet_writer(const TableInfo* tableInfo, const PartitionInfo* partitionInfo) {
    // init fs property schema with tableInfo and partitionInfo
    // ::parquet::schema::NodeVector fields;
    // ::parquet::Repetition::type rep_type;
    // ::parquet::Type::type data_type;

    //
    // location, schema, format, compress, dirctory default = enable,
    // partitionInfo:  column(FE) and chunk -> columnValue filepath
    // iceberg rolling file name
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
        auto st = close_current_writer();
        if (st.ok()) {
            new_file_writer();
        }
    }
    auto st = _writer->write(chunk);
    return st;
}

Status ParquetWriterWrap::close_current_writer() {
    bool ret = ExecEnv::GetInstance()->pipeline_sink_io_pool()->try_offer([&]() {
                _writer->close();
                });
    if (ret) {
        _pending_commits.emplace_back(_writer);
        return Status::OK();
    } else {
        return Status::IOError("submit close file error!");
    }

}

Status ParquetWriterWrap::close() {
    if (_writer != nullptr) {
        auto st = close_current_writer();
        if (!st.ok()) {
            return st;
        }
    }
    return Status::OK();
}

bool ParquetWriterWrap::closed() {
    for (auto& writer : _pending_commits) {
        if (writer != nullptr && writer->closed()) {
            _metadatas.emplace_back(writer->metadata());
            writer = nullptr;
        }
        if (writer != nullptr && (!writer->closed())) {
            return false;
        }
    }
    return true;
}

} // starrocks::vectorized