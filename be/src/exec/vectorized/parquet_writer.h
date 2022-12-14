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

namespace starrocks::vectorized {

    class ParquetOutputStream : public arrow::io::OutputSream {
    public:
        ParquetOutputStream(std::shared_ptr<starrocks::WritableFile> wfile);
        ~ParquetOutputStream() override;

        arrow::Status Write(const void *data, int64_t nbytes) override;
        arrow::Status Write(const std::shared_ptr<Buffer> &data) override;
        arrow::Status Flush() override;
        arrow::Status Close() override;
        arrow::Result<int64_t> Tell() const override;
        bool closed() const override;

    private:
        std::shared_ptr<starrocks::WritableFile> _wfile;
    };

    class ParquetWriterWrap {
    public:
        ParquetWriterWrap(std::shared_ptr<arrow::io::WritableFile>&& parquet_file);
        ~ParquetWriterWrap();

        void close();
        Status init_parquet_writer(const std::vector<SlotDescriptor*>& tuple_slot_descs, const std::string& timezone);
        Status write(const arrow::RecordBatch&);

    private:
        parquet::RowGroupWriter* get_row_group_writer();

        std::shared_ptr<ParquetOutputStream> _output_stream;
        std::shared_ptr<parquet::WriterProperties> _writer_properties;
        std::unique_ptr<parquet::arrow::FileWriter> _writer;

        // parquet file reader object
        std::shared_ptr<::arrow::RecordBatchReader> _rb_batch;
        std::shared_ptr<arrow::RecordBatch> _batch;
        std::shared_ptr<parquet::FileMetaData> _file_metadata;

        // For nested column type, it's consisting of multiple physical-columns
        std::map<std::string, std::vector<int>> _map_column_nested;
        std::vector<int> _parquet_column_ids;
        int _total_groups; // groups in a parquet file
        int _current_group;

        int _rows_of_group; // rows in a group.
        int _current_line_of_group;
        int _current_line_of_batch;

        int64_t _read_offset;
        int64_t _read_size;
    };

} // namespace starrocks::vectorized
