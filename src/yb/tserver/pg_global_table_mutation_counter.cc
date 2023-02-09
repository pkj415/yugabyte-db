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

#include "yb/util/shared_lock.h"

#include "yb/tserver/pg_global_table_mutation_counter.h"
#include "yb/common/entity_ids.h"

namespace yb {
namespace tserver {

  void GlobalTableMutationCounter::Increase(TableId table_id, uint64 mutation_count) {
    {
      SharedLock shared_lock(mutex_);
      if (table_mutation_counts_.contains(table_id)) {
        table_mutation_counts_[table_id] += mutation_count;
        return;
      }
    }

    std::lock_guard lock(mutex_);
    if (table_mutation_counts_.contains(table_id)) {
      table_mutation_counts_[table_id] += mutation_count;
    } else {
      table_mutation_counts_[table_id] = mutation_count;
    }
}

  std::unordered_map<TableId, std::atomic<uint64>> GlobalTableMutationCounter::GetAndClear() {
    std::lock_guard lock(mutex_);
    return std::move(table_mutation_counts_);
  }

}  // namespace tserver
}  // namespace yb
