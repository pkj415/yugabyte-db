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
//
// The following only applies to changes made to this file as part of YugaByte development.
//
// Portions Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#include "yb/tserver/pg_table_mutation_counter.h"
#include "yb/common/entity_ids.h"

namespace yb {
namespace tserver {
namespace tablemutationcounter {

  static std::mutex mutex_;

  // Table id is kept as string here as it is passed as bytes from PG to TServer. It is not
  // converted to oid to not convert it for each table and each transaction. It will be converted
  // to oid while triggering ANALYZE periodically.
  static std::unordered_map<std::string, int> table_mutation_counts_ GUARDED_BY(mutex_);

  void IncrementTableMutationCount(const std::string& table_id, int mutation_count) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (table_mutation_counts_.contains(table_id)) {
        table_mutation_counts_[table_id] += mutation_count;
    } else {
        table_mutation_counts_.insert({table_id, mutation_count});
    }
  }

  std::unordered_map<std::string, int> GetAllMutationCounts() {
    std::lock_guard<std::mutex> lock(mutex_);
    return table_mutation_counts_;
  }

  // TODO(velioglu): Remove it

  void PrintMutationCounts() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto i = table_mutation_counts_.begin(); i != table_mutation_counts_.end(); i++) {
      Result<uint32_t> table_oid = GetPgsqlTableOid(i->first);
      LOG(WARNING) << "Table with id " << table_oid << " has " << i->second << " mutation so far";
    }
  }

  void ClearTableMutationCount(std::string table_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    table_mutation_counts_.erase(table_id);
  }

  void ClearAllMutationCounts() {
    std::lock_guard<std::mutex> lock(mutex_);
    table_mutation_counts_.clear();
  }
}  // namespace tablemutationcounter
}  // namespace tserver
}  // namespace yb
