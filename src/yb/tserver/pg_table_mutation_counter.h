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

namespace yb {
namespace tserver {
namespace tablemutationcounter {

void IncrementTableMutationCount(const std::string& table_id, int mutation_count);
void PrintMutationCounts();
std::unordered_map<std::string, int> GetAllMutationCounts();
void ClearTableMutationCount(std::string table_id);
void ClearAllMutationCounts();

}   // namespace tablemutationcounter
}  // namespace tserver
}  // namespace yb
