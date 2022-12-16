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
#include <arrow/io/api.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/writer.h>

#include <memory>

#include "fs/fs.h"

namespace starrocks::parquet {

class ParquetOutputStream : public arrow::io::OutputSream {
public:
    ParquetOutputStream(std::shared_ptr<starrocks::WritableFile> wfile);
    ~ParquetOutputStream() override;

    arrow::Status Write(const void *data, int64_t nbytes) override;
    arrow::Status Write(const std::shared_ptr<Buffer> &data) override;
    arrow::Status Close() override;
    arrow::Result<int64_t> Tell() const override;
    bool closed() const override { return _is_closed; };

private:
    std::shared_ptr<starrocks::WritableFile> _wfile;
    int64_t _cur_pos = 0;
    int64_t _written_len = 0;
    bool _is_closed = false;
};

class FileWriter {
public:
    FileWriter(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<parquet::WriterProperties> properties,
               std::shared_ptr<parquet::schema::GroupNode> schema);
    ~FileWriter() = default;

    Status init();
    Status write(vectorized::Chunk* chunk);
    void close();
    int64_t writen_len();
private:
    std::shared_ptr<ParquetOutputStream> _outstream;
    std::shared_ptr<parquet::WriterProperties> _properties;
    std::shared_ptr<parquet::schema::GroupNode> _schema;
    std::unique_ptr<parquet::ParquetFileWriter> _writer;
    parquet::RowGroupWriter* _rg_writer;
};

} // namespace starrocks::parquet