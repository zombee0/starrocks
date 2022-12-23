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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/mysql_table_sink.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "iceberg_table_sink.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

IcebergTableSink::IcebergTableSink(ObjectPool* pool, const RowDescriptor& row_desc, const std::vector<TExpr>& t_exprs)
    : _pool(pool), _t_output_expr(t_exprs) {}

IcebergTableSink::~IcebergTableSink() = default;

Status IcebergTableSink::init(const TDataSink& t_sink, RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::init(t_sink, state));
    LOG(WARNING) << "222222222";
    return Status::OK();
}

Status IcebergTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    std::stringstream title;
    title << "MysqlTableSink (frag_id=" << state->fragment_instance_id() << ")";
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));
    LOG(WARNING) << "3333333333";
    return Status::OK();
}

Status IcebergTableSink::open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    // create writer
    LOG(WARNING) << "44444444444";

    return Status::OK();
}

Status IcebergTableSink::send_chunk(RuntimeState* state, vectorized::Chunk* chunk) {
    LOG(WARNING) << "55555555555";
    return Status::OK();
}

Status IcebergTableSink::close(RuntimeState* state, Status exec_status) {
    LOG(WARNING) << "666666666";
    Expr::close(_output_expr_ctxs, state);
    return Status::OK();
}
}