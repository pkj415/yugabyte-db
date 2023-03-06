// Copyright (c) YugaByte, Inc.
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

#pragma once

#include <unordered_map>
#include "yb/client/client_fwd.h"
#include "yb/gutil/thread_annotations.h"
#include "yb/tserver/tserver_fwd.h"
#include "yb/util/locks.h"

namespace yb {
namespace tserver {

class GlobalTableMutationCounter {
 public:
    void Increase(TableId table_id, uint64 mutation_count) EXCLUDES(mutex_);
    std::unordered_map<TableId, std::atomic<uint64>> GetAndClear() EXCLUDES(mutex_);
 private:
    mutable rw_spinlock mutex_;
    // Table id is kept as string here as it is passed as bytes from PG to TServer. So, it
    // includes database id as a part of bytes. It is not converted to oid to not convert
    // it for each table and each transaction. It will be converted on the analyze
    // service side.
    std::unordered_map<TableId, std::atomic<uint64>> table_mutation_counts_ GUARDED_BY(mutex_);
};

}  // namespace tserver
}  // namespace yb
