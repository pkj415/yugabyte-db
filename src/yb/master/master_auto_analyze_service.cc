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

#include "yb/common/common_flags.h"

#include "yb/master/catalog_manager.h"
#include "yb/master/master_auto_analyze.service.h"
#include "yb/master/master_service_base.h"
#include "yb/master/master_service_base-internal.h"


using namespace std::literals;

namespace yb {
namespace master {

namespace {

class MasterAutoAnalyzeServiceImpl : public MasterServiceBase, public MasterAutoAnalyzeIf {
 public:
  explicit MasterAutoAnalyzeServiceImpl(Master* master)
      : MasterServiceBase(master), MasterAutoAnalyzeIf(master->metric_entity()) {}

  void IncreaseMutationCounters(const IncreaseMutationCountersRequestPB* req,
                                 IncreaseMutationCountersResponsePB* resp,
                                 rpc::RpcContext rpc) override {

    // TODO: Complete with master implementation diff
    rpc.RespondSuccess();
  }
};

} // namespace

std::unique_ptr<rpc::ServiceIf> MakeMasterAutoAnalyzeService(Master* master) {
  return std::make_unique<MasterAutoAnalyzeServiceImpl>(master);
}

} // namespace master
} // namespace yb
