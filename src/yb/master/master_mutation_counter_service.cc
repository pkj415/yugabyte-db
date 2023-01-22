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

#include "yb/master/master_mutation_counter.service.h"
#include "yb/master/master_service_base.h"
#include "yb/master/master_service_base-internal.h"


using namespace std::literals;

namespace yb {
namespace master {

namespace {

class MasterMutationCounterServiceImpl : public MasterServiceBase, public MasterMutationCounterIf {
 public:
  explicit MasterMutationCounterServiceImpl(Master* master)
      : MasterServiceBase(master), MasterMutationCounterIf(master->metric_entity()) {}

  void IncremenentMutationCounter(const MutationCounterRequestPB* req,
                                  MutationCounterResponsePB* resp, rpc::RpcContext rpc) override {
    LOG(WARNING) << " *** ON THE MASTER CALL INCREMENTMUTATIONCOUNTER";
    for (int i = 0 ; i < req->table_id_size() ; i++) {
      LOG(WARNING) << "On the master table with the id " << req->table_id(i)
                   << " has the mutation count " << req->mutation_count(i);
    }

    rpc.RespondSuccess();
  }
};

} // namespace

std::unique_ptr<rpc::ServiceIf> MakeMasterMutationCounterService(Master* master) {
  return std::make_unique<MasterMutationCounterServiceImpl>(master);
}

} // namespace master
} // namespace yb
