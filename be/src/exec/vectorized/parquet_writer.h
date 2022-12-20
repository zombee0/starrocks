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

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include "formats/parquet/file_writer.h"
#include "fs/fs.h"

//SinkOperator
// member

// partition partition->chunk

// partition -> Sink_IO_buffer

// plan B:
// chunk -> chunk1 chunk2
// partition 1 -> sink_io_buffer   -> parition writer
// partition 2 -> sink_io_buffer


namespace starrocks::vectorized {

class TableInfo;
class PartitionInfo;

class ParquetWriterWrap {
public:
    ParquetWriterWrap(const TableInfo* tableInfo, const PartitionInfo* partitionInfo);
    ~ParquetWriterWrap() = default;

    // init filesystem, init writeproperties, schema
    Status init_parquet_writer(const TableInfo* tableInfo, const PartitionInfo* partitionInfo);
    Status append_chunk(vectorized::Chunk* chunk); //check if we need a new file, file_writer->write
    Status close();
    bool writable() { return _writer->writable(); }

private:
    std::string get_new_file_name();
    Status new_file_writer();
    Status close_current_writer();
    bool exceed_file_size();

    std::shared_ptr<FileSystem> _fs;
    std::shared_ptr<starrocks::parquet::FileWriter> _writer = nullptr;
    std::shared_ptr<::parquet::WriterProperties> _properties;
    std::shared_ptr<::parquet::schema::GroupNode> _schema;
    std::string _current_file = nullptr;
    std::vector<std::shared_ptr<::parquet::FileMetaData>> _metadatas;
    int64_t _max_file_size = 1024 * 1024 * 1024;
};

} // namespace starrocks::vectorized
