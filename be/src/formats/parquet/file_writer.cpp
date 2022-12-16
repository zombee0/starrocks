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

#include "formats/parquet/file_writer.h"

namespace starrocks::parquet {

ParquetOutputStream::ParquetOutputStream(std::shared_ptr<starrocks::WritableFile> wfile)
    : _wfile(wfile) {
}

ParquetOutputStream::~ParquetOutputStream() {}

arrow::Status ParquetOutputStream::Write(const std::shared_ptr<Buffer> &data) {

}

arrow::Status ParquetOutputStream::Write(const void* data, int64_t nbytes) {
    if (_is_closed) {
        return arrow::Status::OK();
    }
    Status st = _wfile->append();
    if (!st.ok()) {
        return arrow::Status::IOError(st.to_string());
    }
    _cur_pos += nbytes;
    _written_len += nbytes;
    return arrow::Status::OK();
}

arrow::Result<int64_t> ParquetOutputStream::Tell() const {
    return _cur_pos;
}

arrow::Status ParquetOutputStream::Close() {
    if (_is_closed) {
        return arrow::Status::OK();
    }
    Status st = _wfile->close();
    if (!st.ok()) {
        LOG(WARNING) << "close parquet output stream failed: " << st;
        return arrow::Status::IOError(st.to_string());
    }
    _is_closed = true;
    return arrow::Status::OK();
}

FileWriter::FileWriter(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<parquet::WriterProperties> properties,
    std::shared_ptr<parquet::schema::GroupNode> schema)
    : _properties(properties), _schema(schema) {
    _outstream = std::make_shared<ParquetOutputStream>(writable_file);
}

FileWriter::init() {
    _writer = parquet::ParquetFileWriter::Open(_outstream, _schema, _properties);
    if (_writer == nullptr) {
        return Status::InternalError("Failed to create file writer");
    }
    return Status::OK();
}

FileWriter::write()







} // namespace starrocks::parquet
