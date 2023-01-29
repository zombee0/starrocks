
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

#include "column/chunk.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"

#include <gen_cpp/DataSinks_types.h>

namespace starrocks::parquet {

    class ParquetOutputStream : public arrow::io::OutputStream {
    public:
        ParquetOutputStream(std::unique_ptr<starrocks::WritableFile> wfile);
        ~ParquetOutputStream() override;

        arrow::Status Write(const void *data, int64_t nbytes) override;
        arrow::Status Write(const std::shared_ptr<arrow::Buffer> &data) override;
        arrow::Status Close() override;
        arrow::Result<int64_t> Tell() const override;

        bool closed() const override { return _is_closed; };

        int64_t get_written_len() const;

        //const std::string& filename() { return _wfile->filename(); }

    private:
        std::unique_ptr<starrocks::WritableFile> _wfile;
        int64_t _cur_pos = 0;
        bool _is_closed = false;

        enum HEADER_STATE {
            INITED = 1,
            CACHED = 2,
            WRITEN = 3,
        };
        HEADER_STATE _header_state = INITED;
    };

    class FileWriter {
    public:
        FileWriter(std::unique_ptr<WritableFile> writable_file, std::string file_name, std::string& partition_dir,
                   std::shared_ptr<::parquet::WriterProperties> properties, std::shared_ptr<::parquet::schema::GroupNode> schema,
                   const std::vector<ExprContext*>& output_expr_ctxs, RuntimeProfile* parent_profile);

        ~FileWriter() = default;

        Status init();
        Status write(vectorized::Chunk* chunk);
        Status close(RuntimeState* state);
        std::shared_ptr<::parquet::FileMetaData> metadata() const { return _file_metadata; }
        bool writable() {
            auto lock = std::unique_lock(_m);
            return !_rg_writer_closing;
        }
        bool closed() {
            return _closed.load();
        }
        Status buildIcebergDataFile(TIcebergDataFile& dataFile);

        size_t get_written_bytes();
        Status splitOffsets(std::vector<int64_t> &splitOffsets);
        std::size_t file_size();
        //std::string filename() {
        //    std::string name;
        //    name.assign(_outstream->filename());
        //    return name;
        //}

    private:
        ::parquet::RowGroupWriter* get_rg_writer();
        void _check_size();
        void _rg_writer_close();

        std::shared_ptr<ParquetOutputStream> _outstream;
        std::string _file_name;
        std::string _partition_dir;
        std::shared_ptr<::parquet::WriterProperties> _properties;
        std::shared_ptr<::parquet::schema::GroupNode> _schema;
        std::unique_ptr<::parquet::ParquetFileWriter> _writer;
        ::parquet::RowGroupWriter* _rg_writer = nullptr;
        int64_t _cur_written_rows;
        int64_t _total_rows;
        int64_t _max_row_group_size = 128 * 1024 * 1024; // 128 * 1024 * 1024;
        std::shared_ptr<::parquet::FileMetaData> _file_metadata;
        std::atomic<bool> _closed = false;
        std::vector<ExprContext*> _output_expr_ctxs;
        RuntimeProfile* _parent_profile;
        RuntimeProfile::Counter* _rg_close_counter = nullptr;
        std::vector<int64_t> _buffered_values_estimate;
        int64_t _total_row_group_writen_bytes{0};
        std::condition_variable _cv;
        bool _rg_writer_closing = false;
        std::mutex _m;
    };

} // namespace starrocks::parquet

    