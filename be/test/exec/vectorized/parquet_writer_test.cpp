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

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/fixed_length_column.h"

namespace starrocks::vectorized {

class ParquetWriterTest : public ::testing::Test {
public:
    ParquetWriterTest() = default;
    ~ParquetWriterTest() override = default;

    void SetUp() override {}

    size_t cnt = 0;

    std::string make_string(size_t i) { return std::string("c").append(std::to_string(static_cast<int32_t>(i))); }

    VectorizedFieldPtr make_field(size_t i) {
        return std::make_shared<VectorizedField>(i, make_string(i), get_type_info(TYPE_INT), false);
    }

    VectorizedFields make_fields(size_t size) {
        VectorizedFields fields;
        for (size_t i = 0; i < size; i++) {
            fields.emplace_back(make_field(i));
        }
        return fields;
    }

    VectorizedSchemaPtr make_schema(size_t i) {
        VectorizedFields fields = make_fields(i);
        return std::make_shared<VectorizedSchema>(fields);
    }

    ColumnPtr make_column(size_t start, size_t size = 100) {
        auto column = FixedLengthColumn<int32_t>::create();
        for (int i = 0; i < size; i++) {
            column->append(start + cnt + i);
        }
        cnt += 100;
        return column;
    }

    Columns make_columns(size_t num_cols, size_t size = 100) {
        Columns columns;
        for (size_t i = 0; i < num_cols; i++) {
            columns.emplace_back(make_column(i, size));
        }
        return columns;
    }
};

TEST_F(ParquetWriterTest, test_parquet_write) {
    vectorized::TableInfo table;
    std::string starrocks_home = getenv("STARROCKS_HOME");
    table._table_location = starrocks_home + "/be/test/exec/parquet_writer_test/";

    ::parquet::schema::NodeVector fields;
    fields.push_back(::parquet::schema::PrimitiveNode::Make(
            "col1", ::parquet::Repetition::REQUIRED, ::parquet::LogicalType::None(), ::parquet::Type::INT32));
    table._schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::REQUIRED, fields));

    PartitionInfo partition;
    partition._column_names.emplace_back("col1");
    partition._column_values.emplace_back("1");

    std::cout << "ready to create writer" << std::endl;

    std::shared_ptr<ParquetWriterWrap> writer = std::make_shared<ParquetWriterWrap>(table, partition);
    std::cout << "writer created" << std::endl;
    std::cout << "writer inited" << std::endl;
    EXPECT_EQ(true, writer->writable());

    std::cout << "writer go to append" << std::endl;
    for (size_t i =0 ; i < 100; i++) {
        auto chunk = std::make_unique<Chunk>(make_columns(1), make_schema(1));
        while (!writer->writable()) {
            sleep(1);
        }
        writer->append_chunk(chunk.get());
    }
    std::cout << "writer go to close" << std::endl;
    auto st = writer->close();
    EXPECT_EQ(true, st.ok());

    sleep(5);
    EXPECT_EQ(true, writer->closed());
}

} // namespace starrocks::vectorized
