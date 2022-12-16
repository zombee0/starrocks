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

#include "formats/parquet/file_writer.h"
#include "fs/fs.h"

namespace starrocks::vectorized {

class TableInfo;
class PartitionInfo;

class ParquetWriterWrap {
public:
    ParquetWriterWrap(TableInfo tableInfo, PartitionInfo partitionInfo);
    ~ParquetWriterWrap();

    std::string get_new_file_name();

    Status init_parquet_writer(); // init filesystem, init writeproperties,
    Status append_chunk(vectorized::Chunk* chunk); //check if we need a new file, file_writer->write
    void close();

private:
    std::shared_ptr<FileSystem> _fs;
    std::unique_ptr<starrocks::parquet::FileWriter> _writer;

    std::string _current_file = nullptr;
    };

} // namespace starrocks::vectorized
