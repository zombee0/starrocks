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

#include "column/column_helper.h"
#include "formats/parquet/file_writer.h"
#include "util/slice.h"

namespace starrocks::parquet {

ParquetOutputStream::ParquetOutputStream(std::shared_ptr<starrocks::WritableFile> wfile)
    : _wfile(wfile) {
}

ParquetOutputStream::~ParquetOutputStream() {}

arrow::Status ParquetOutputStream::Write(const std::shared_ptr<arrow::Buffer> &data) {
    Write(data->data(), data->size());
    return arrow::Status::OK();
}

arrow::Status ParquetOutputStream::Write(const void* data, int64_t nbytes) {
    if (_is_closed) {
        return arrow::Status::OK();
    }
    // use _wfilt->append()
    Status st = _wfile->appendv(reinterpret_cast<const Slice*>(data), nbytes);
    if (!st.ok()) {
        return arrow::Status::IOError(st.to_string());
    }
    _cur_pos += nbytes;
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

int64_t ParquetOutputStream::get_written_len() const {
    return _cur_pos;
}

FileWriter::FileWriter(std::shared_ptr<WritableFile> writable_file, std::shared_ptr<::parquet::WriterProperties> properties,
    std::shared_ptr<::parquet::schema::GroupNode> schema)
    : _properties(properties), _schema(schema) {
    _outstream = std::make_shared<ParquetOutputStream>(writable_file);
}

::parquet::RowGroupWriter* FileWriter::get_rg_writer() {
    if (_rg_writer == nullptr) {
        _rg_writer = _writer->AppendBufferedRowGroup();
    }
    if (_rg_writer->total_bytes_written() > _max_row_group_size) {
        // TODO: we need async close _rg_writer
        _rg_writer->Close();
        // appendBufferedRowGroup will check and close current _rg_writer
        _rg_writer = _writer->AppendBufferedRowGroup();
        _cur_written_rows = 0;
    }
    return _rg_writer;
}

Status FileWriter::init() {
    _writer = ::parquet::ParquetFileWriter::Open(_outstream, _schema, _properties);
    if (_writer == nullptr) {
        return Status::InternalError("Failed to create file writer");
    }
    return Status::OK();
}

#define DISPATCH_PARQUET_NUMERIC_WRITER(WRITER, COLUMN_TYPE, NATIVE_TYPE)                            \
    ::parquet::RowGroupWriter* rg_writer = get_rg_writer();                                          \
    ::parquet::WRITER* col_writer = static_cast<::parquet::WRITER*>(rg_writer->column(i));           \
    col_writer->WriteBatch(num_rows, nullable ? def_level.data() : nullptr, nullptr,                 \
        reinterpret_cast<const NATIVE_TYPE*>(down_cast<const vectorized::COLUMN_TYPE*>(data_column)->get_data().data()));

Status FileWriter::write(vectorized::Chunk* chunk) {
    if (!chunk->has_rows()) {
        return Status::OK();
    }
    size_t num_rows = chunk->num_rows();
    for (size_t i = 0; i < chunk->num_columns(); i++) {
        auto& col = chunk->get_column_by_index(i);
        bool nullable = col->is_nullable();
        auto null_column = nullable && down_cast<vectorized::NullableColumn*>(col.get())->has_null()
                ? down_cast<vectorized::NullableColumn*>(col.get())->null_column()
                : nullptr;
        auto data_column = vectorized::ColumnHelper::get_data_column(col.get());
        auto type = chunk->schema()->field(i)->type()->type();

        std::vector<int16_t> def_level(num_rows);
        std::fill(def_level.begin(), def_level.end(), 1);
        if (null_column != nullptr) {
            auto nulls = null_column->get_data();
            for (size_t i = 0; i < num_rows; i++) {
                def_level[i] = nulls[i] == 0;
            }
        }
        switch (type) {
        case TYPE_BOOLEAN: {
            DISPATCH_PARQUET_NUMERIC_WRITER(BoolWriter, BooleanColumn, bool);
            break;
        }
        case TYPE_INT: {
            DISPATCH_PARQUET_NUMERIC_WRITER(Int32Writer, Int32Column, int32_t);
            break;
        }
        case TYPE_BIGINT: {
            DISPATCH_PARQUET_NUMERIC_WRITER(Int64Writer, Int64Column, int64_t);
            break;
        }
        case TYPE_FLOAT: {
            DISPATCH_PARQUET_NUMERIC_WRITER(FloatWriter, FloatColumn, float);
            break;
        }
        case TYPE_DOUBLE: {
            DISPATCH_PARQUET_NUMERIC_WRITER(DoubleWriter, DoubleColumn, double);
            break;
        }
        default: {
            return Status::InvalidArgument("Unsupported type");
        }
        }
    }
    _cur_written_rows += num_rows;
    return Status::OK();
}

int64_t FileWriter::get_written_bytes() const {
    int64_t written_bytes = _outstream->get_written_len();
    if (_rg_writer != nullptr) {
        written_bytes += _rg_writer->total_bytes_written();
    }
    return written_bytes;
}

Status FileWriter::close() {
    if (_rg_writer != nullptr) {
        _rg_writer->Close();
        _rg_writer = nullptr;
    }

    _writer->Close();
    _file_metadata = _writer->metadata();
    auto st = _outstream->Close();
    if (st != ::arrow::Status::OK()) {
        return Status::InternalError("Close file failed!");
    }
    return Status::OK();
}

} // namespace starrocks::parquet
