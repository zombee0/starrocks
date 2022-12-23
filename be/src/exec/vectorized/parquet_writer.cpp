
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

#include <fmt/format.h>

#include "exec/vectorized/parquet_writer.h"
#include "runtime/exec_env.h"
#include "util/priority_thread_pool.hpp"
#include "util/uid_util.h"

namespace starrocks::vectorized {

    ParquetWriterWrap::ParquetWriterWrap(const TableInfo& tableInfo, const PartitionInfo& partitionInfo,
                                         const std::vector<ExprContext*>& output_expr_ctxs, RuntimeProfile* parent_profile) :
    _output_expr_ctxs(output_expr_ctxs), _parent_profile(parent_profile) {
        std::cout << "construct" << std::endl;
        init_parquet_writer(tableInfo, partitionInfo);
    }

    Status ParquetWriterWrap::init_parquet_writer(const TableInfo& tableInfo, const PartitionInfo& partitionInfo) {
        std::cout << "init" << std::endl;
        ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(tableInfo._table_location));
        std::cout << "fs done" << std::endl;
        _schema = tableInfo._schema;
        ::parquet::WriterProperties::Builder builder;
        if (tableInfo._enable_dictionary) {
            builder.enable_dictionary();
        } else {
            builder.disable_dictionary();
        }
        builder.version(::parquet::ParquetVersion::PARQUET_2_0);
        switch (tableInfo._compress_type) {
            case tparquet::CompressionCodec::UNCOMPRESSED : {
                builder.compression(::parquet::Compression::UNCOMPRESSED);
                break;
            }
            case tparquet::CompressionCodec::SNAPPY : {
                builder.compression(::parquet::Compression::SNAPPY);
                break;
            }
            case tparquet::CompressionCodec::GZIP : {
                builder.compression(::parquet::Compression::GZIP);
                break;
            }
            case tparquet::CompressionCodec::LZO : {
                builder.compression(::parquet::Compression::LZO);
                break;
            }
            case tparquet::CompressionCodec::BROTLI : {
                builder.compression(::parquet::Compression::BROTLI);
                break;
            }
            case tparquet::CompressionCodec::LZ4 : {
                builder.compression(::parquet::Compression::LZ4);
                break;
            }
            case tparquet::CompressionCodec::ZSTD : {
                builder.compression(::parquet::Compression::ZSTD);
                break;
            }
            default: {
                return Status::InvalidArgument("Error compression type");
            }
        }
        _properties = builder.build();
        std::cout << "properties done" << std::endl;
        if (partitionInfo._column_names.size() != partitionInfo._column_values.size()) {
            return Status::InvalidArgument("columns and values are not matched in partitionInfo");
        }
        std::stringstream ss;
        ss << tableInfo._table_location;
        ss << "/data/";
        for (size_t i = 0; i < partitionInfo._column_names.size(); i++) {
            ss << partitionInfo._column_names[i];
            ss << "=";
            ss << partitionInfo._column_values[i];
            ss << "/";
        }
        _partition_dir = ss.str();
        std::cout << _partition_dir << std::endl;
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
        _cnt += 1;
        _location = _partition_dir + fmt::format("test_{}_{}.parquet", _cnt, generate_uuid_string());
        return _location;
    }

    Status ParquetWriterWrap::new_file_writer() {
        std::cout << "new file" << std::endl;
        std::string file_name = get_new_file_name();
        std::cout << file_name << std::endl;
        WritableFileOptions options{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto writable_file, _fs->new_writable_file(options, file_name));
        std::cout << "writable file done" << std::endl;
        _writer = std::make_shared<starrocks::parquet::FileWriter>(std::move(writable_file), _properties, _schema, _output_expr_ctxs, _parent_profile);
        std::cout << "file writer created  " << std::endl;
        auto st = _writer -> init();
        std::cout << "file writer inited  " << std::endl;
        return st;
    }

    Status ParquetWriterWrap::append_chunk(vectorized::Chunk* chunk, RuntimeState* state) {
        if (_writer == nullptr) {
            auto status = new_file_writer();
            if (!status.ok()) {
                std::cout << status.detailed_message() << std::endl;
            }
        }
        // exceed file size
        if (_writer->file_size() > _max_file_size) {
            auto st = close_current_writer(state);
            if (st.ok()) {
                new_file_writer();
            }
        }
        auto st = _writer->write(chunk);
        return st;
    }

    Status ParquetWriterWrap::close_current_writer(RuntimeState* state) {
        bool ret = ExecEnv::GetInstance()->pipeline_sink_io_pool()->try_offer([&]() {
//            LOG(WARNING) << "closing writer ====================";
            _writer->close();
//            LOG(WARNING) << "closing writer finished ====================";
        });
        if (ret) {
//            LOG(WARNING) << "put pending commit writer ====================";
            _pending_commits.emplace_back(_writer);
            return Status::OK();
        } else {
            return Status::IOError("submit close file error!");
        }

    }

    Status ParquetWriterWrap::close(RuntimeState* state) {
        if (_writer != nullptr) {
            LOG(WARNING) << "rg writer close ====================";
            auto st = close_current_writer(state);
            if (!st.ok()) {
                return st;
            }
        }
        return Status::OK();
    }

    bool ParquetWriterWrap::closed() {
        for (auto& writer : _pending_commits) {
            if (writer != nullptr && writer->closed()) {
                LOG(WARNING) << "======== parquet writer wrap close start ====================";
                TIcebergDataFile dataFile;
                _writer->buildIcebergDataFile(dataFile);
                dataFile.partition_path = _partition_dir;
                dataFile.path = _location;
                _data_files.emplace_back(dataFile);

                _metadatas.emplace_back(writer->metadata());
                writer = nullptr;
            }
            if (writer != nullptr && (!writer->closed())) {
                LOG(WARNING) << "==========file writer no close current====================";
                return false;
            }
        }

        if (_writer != nullptr) {
            return _writer->closed();
        }

        return true;
    }

} // starrocks::vectorized

    