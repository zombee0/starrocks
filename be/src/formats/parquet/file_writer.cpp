
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
#include "util/priority_thread_pool.hpp"
#include "runtime/exec_env.h"
#include "common/logging.h"
#include "exprs/expr.h"
#include "util/runtime_profile.h"
#include "column/vectorized_fwd.h"


namespace starrocks::parquet {

ParquetOutputStream::ParquetOutputStream(std::unique_ptr<starrocks::WritableFile> wfile)
        : _wfile(std::move(wfile)) {
    set_mode(arrow::io::FileMode::WRITE);
    std::cout << "output stream create" << std::endl;
}

ParquetOutputStream::~ParquetOutputStream() {
    arrow::Status st = ParquetOutputStream::Close();
    if (!st.ok()) {
        LOG(WARNING) << "close parquet output stream failed: " << st;
    }
}

arrow::Status ParquetOutputStream::Write(const std::shared_ptr<arrow::Buffer> &data) {
    Write(data->data(), data->size());
    return arrow::Status::OK();
}

arrow::Status ParquetOutputStream::Write(const void* data, int64_t nbytes) {
    if (_is_closed) {
        return arrow::Status::OK();
    }
    LOG(WARNING) << "write data, length: " << nbytes;
    const char* ch = reinterpret_cast<const char*>(data);
    LOG(WARNING) << "write data, data: " << ch[0] << ch[1] << ch[2] << ch[3];

    Slice slice(ch, nbytes);
    Status st = _wfile->append(slice);
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

FileWriter::FileWriter(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<::parquet::WriterProperties> properties,
                       std::shared_ptr<::parquet::schema::GroupNode> schema, const std::vector<ExprContext*>& output_expr_ctxs,
                       RuntimeProfile* parent_profile)
        : _properties(std::move(properties)), _schema(std::move(schema)), _output_expr_ctxs(output_expr_ctxs), _parent_profile(parent_profile) {
    _outstream = std::make_shared<ParquetOutputStream>(std::move(writable_file));
    _buffered_values_estimate.reserve(_schema->field_count());
    _rg_close_counter = ADD_COUNTER(_parent_profile, "RowGroupWriterCloseCounter", TUnit::UNIT);
}

::parquet::RowGroupWriter* FileWriter::get_rg_writer() {
    if (rg_writer == nullptr) {
        rg_writer = _writer->AppendBufferedRowGroup();
        _cur_written_rows = 0;
    }
    return rg_writer;
}

Status FileWriter::init() {
    _writer = ::parquet::ParquetFileWriter::Open(_outstream, _schema, _properties);
    if (_writer == nullptr) {
        return Status::InternalError("Failed to create file writer");
    }
    return Status::OK();
}

#define DISPATCH_PARQUET_NUMERIC_WRITER(WRITER, COLUMN_TYPE, NATIVE_TYPE)                                     \
    FileWriter::get_rg_writer();                                                                    \
    ::parquet::WRITER* col_writer = static_cast<::parquet::WRITER*>(rg_writer->column(i));                       \
    col_writer->WriteBatch(num_rows, nullable ? def_level.data() : nullptr, nullptr,                          \
        reinterpret_cast<const NATIVE_TYPE*>(down_cast<const COLUMN_TYPE*>(data_column)->get_data().data())); \
    _buffered_values_estimate[i] = col_writer->EstimatedBufferedValueBytes();                                 \

Status FileWriter::write(vectorized::Chunk* chunk) {
    if (!chunk->has_rows()) {
        return Status::OK();
    }

    size_t num_rows = chunk->num_rows();
    for (size_t i = 0; i < chunk->num_columns(); i++) {
        auto& col = chunk->get_column_by_index(i);
        bool nullable = col->is_nullable();
        auto null_column = nullable && down_cast<starrocks::vectorized::NullableColumn*>(col.get())->has_null()
                           ? down_cast<starrocks::vectorized::NullableColumn*>(col.get())->null_column()
                           : nullptr;
        const auto data_column = vectorized::ColumnHelper::get_data_column(col.get());

        std::vector<int16_t> def_level(num_rows);
        std::fill(def_level.begin(), def_level.end(), 1);
        if (null_column != nullptr) {
            auto nulls = null_column->get_data();
            for (size_t j = 0; j < num_rows; j++) {
                def_level[j] = nulls[j] == 0;
            }
        }

        const auto type = _output_expr_ctxs[i]->root()->type().type;
        switch (type) {
            case TYPE_BOOLEAN: {
                DISPATCH_PARQUET_NUMERIC_WRITER(BoolWriter, starrocks::vectorized::BooleanColumn, bool)
                break;
            }
            case TYPE_INT: {
                DISPATCH_PARQUET_NUMERIC_WRITER(Int32Writer, starrocks::vectorized::Int32Column, int32_t)
                break;
            }
            case TYPE_BIGINT: {
                DISPATCH_PARQUET_NUMERIC_WRITER(Int64Writer, starrocks::vectorized::Int64Column, int64_t)
                break;
            }
            case TYPE_FLOAT: {
                DISPATCH_PARQUET_NUMERIC_WRITER(FloatWriter, starrocks::vectorized::FloatColumn, float)
                break;
            }
            case TYPE_DOUBLE: {
                DISPATCH_PARQUET_NUMERIC_WRITER(DoubleWriter, starrocks::vectorized::DoubleColumn, double)
                break;
            }
            default: {
                return Status::InvalidArgument("Unsupported type");
        }
        _cur_written_rows += num_rows;
//        LOG(WARNING) << "============= cur_written_rows: [" << _cur_written_rows << "]=======";
//        LOG(WARNING) << "============= rg writer written: [" << _rg_writer->total_bytes_written() << "]=======";
//        LOG(WARNING) << "============= rg writer compression written: [" << _rg_writer->total_compressed_bytes() << "]=======";
        //if (_rg_writer->total_bytes_written() > _max_row_group_size) {
        if (_rg_writer->total_compressed_bytes() + _rg_writer-> total_bytes_written() > 2000000) {
            _rg_writer_closing.store(true);
            bool ret = ExecEnv::GetInstance()->pipeline_sink_io_pool()->try_offer([&]() {
                _rg_writer->Close();
//                LOG(WARNING) << "============= after close rg writer written: [" << _rg_writer->total_bytes_written() << "]=======";
//                LOG(WARNING) << "============= rg writer compression written: [" << _rg_writer->total_compressed_bytes() << "]=======";
                _rg_writer = nullptr;
                _rg_writer_closing.store(false);
                COUNTER_UPDATE(_rg_close_counter, 1);
            });
            if (!ret) {
                _rg_writer_closing.store(false);
            }
        }
    }

    _cur_written_rows += num_rows;
    _check_size();
    return Status::OK();
}


void FileWriter::_check_size() {
    if (FileWriter::get_written_bytes() > _max_row_group_size) {
        bool ret = ExecEnv::GetInstance()->pipeline_sink_io_pool()->try_offer([&]() {
            _rg_writer_close();
        });
        if (!ret) {
            _rg_writer_closing.store(false);
        }
    }
}

void FileWriter::_rg_writer_close() {
    _rg_writer_closing.store(true);
    rg_writer->Close();
    _total_row_group_writen_bytes = _outstream->get_written_len();
    _total_rows += _cur_written_rows;
    rg_writer = nullptr;
    _rg_writer_closing.store(false);
    std::fill(_buffered_values_estimate.begin(), _buffered_values_estimate.end(), 0);
}

size_t FileWriter::get_written_bytes() {
    if (rg_writer == nullptr) {
        return 0;
    }
    int64_t estimated_bytes = 0;

    for (long i : _buffered_values_estimate) {
        estimated_bytes += i;
    }

    return rg_writer->total_bytes_written() + rg_writer->total_compressed_bytes() + estimated_bytes;
}

Status FileWriter::close() {
    if (rg_writer != nullptr) {
        _rg_writer_close();
    }
    _writer->Close();
    _file_metadata = _writer->metadata();
    auto st = _outstream->Close();
    if (st != ::arrow::Status::OK()) {
        return Status::InternalError("Close file failed!");
    }
    _closed.store(true);
    return Status::OK();
}

std::size_t FileWriter::file_size() {
    DCHECK(_outstream != nullptr);
    return _total_row_group_writen_bytes + get_written_bytes();
}

Status FileWriter::buildIcebergDataFile(TIcebergDataFile& dataFile) {
    dataFile.format = "parquet";
    dataFile.record_count = _cur_written_rows;
    dataFile.file_size_in_bytes = _outstream->get_written_len();
    std::vector<int64_t> split_offsets;
    splitOffsets(split_offsets);
    dataFile.split_offsets = split_offsets;

    std::unordered_map<int32_t, int64_t> column_sizes;
    std::unordered_map<int32_t, int64_t> value_counts;
    std::unordered_map<int32_t, int64_t> null_value_counts;
    std::unordered_map<int32_t, std::string> min_values;
    std::unordered_map<int32_t, std::string> max_values;

    for (int i = 0; i < _file_metadata->num_row_groups(); ++i) {
        auto block = _file_metadata->RowGroup(i);
        for (int j = 0; j < block->num_columns(); j++) {
            auto column_meta = block->ColumnChunk(j);
            int field_id = j + 1;
            if (null_value_counts.find(field_id) == null_value_counts.end()) {
//                    LOG(WARNING) << "================null_value_counts============";
                null_value_counts.insert({field_id, column_meta->statistics()->null_count()});
            } else {
                null_value_counts[field_id] += column_meta->statistics()->null_count();
            }

            if (column_sizes.find(field_id) == column_sizes.end()) {
                column_sizes.insert({field_id, column_meta->total_compressed_size()});
            } else {
                column_sizes[field_id] += column_meta->total_compressed_size();
            }

            if (value_counts.find(field_id) == value_counts.end()) {
                value_counts.insert({field_id, column_meta->num_values()});
            } else {
                value_counts[field_id] += column_meta->num_values();
            }

            min_values[field_id] = column_meta->statistics()->EncodeMin();
            max_values[field_id] = column_meta->statistics()->EncodeMax();
        }
    }

    TIcebergColumnStats stats;
    for(auto& i : column_sizes) {
        stats.columnSizes.insert({i.first, i.second});
    }
    for(auto& i : value_counts) {
        stats.valueCounts.insert({i.first, i.second});
    }
    for(auto& i : null_value_counts) {
        stats.nullValueCounts.insert({i.first, i.second});
    }
    for(auto& i : min_values) {
        stats.lowerBounds.insert({i.first, i.second});
    }
    for(auto& i : max_values) {
        stats.upperBounds.insert({i.first, i.second});
    }

    dataFile.column_stats = stats;
    return Status::OK();
}

Status FileWriter::splitOffsets(std::vector<int64_t>& splitOffsets) {
    for (int i = 0; i < _file_metadata->num_row_groups(); i++) {
        auto first_column_meta = _file_metadata->RowGroup(i)->ColumnChunk(0);
        int64_t dict_page_offset = first_column_meta->dictionary_page_offset();
        int64_t first_data_page_offset = first_column_meta->data_page_offset();
        int64_t split_offset = dict_page_offset > 0 && dict_page_offset < first_data_page_offset ? dict_page_offset
                                                                                                 : first_data_page_offset;
        splitOffsets.emplace_back(split_offset);
    }
    return Status::OK();
}

} // namespace starrocks::parquet

    