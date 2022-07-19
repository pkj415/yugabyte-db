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

#ifndef YB_TSERVER_PG_RESPONSE_CACHE_H
#define YB_TSERVER_PG_RESPONSE_CACHE_H

#include <memory>
#include <variant>

#include "yb/client/client_fwd.h"

#include "yb/rpc/rpc_fwd.h"

#include "yb/tserver/pg_client.fwd.h"

#include "yb/util/monotime.h"

namespace yb {
namespace tserver {

using MyOps = std::vector<std::shared_ptr<client::YBPgsqlOp>>;

class PgResponseCache {
 public:
  PgResponseCache();
  ~PgResponseCache();

  class Entry;
  using EntryPtr = std::shared_ptr<Entry>;

  class Getter {
  public:
    explicit Getter(const EntryPtr& entry);
    ~Getter();
    void Get(PgPerformResponsePB* response, rpc::RpcContext* context);

  private:
    EntryPtr entry_;
  };

  class Setter {
  public:
    explicit Setter(const EntryPtr& entry);
    ~Setter();
    void SetData(const PgPerformResponsePB& resp, const MyOps& ops);
    void SetFailure(Status failure);

  private:
    EntryPtr entry_;
  };

  using EntryAccessor = std::variant<Getter, Setter>;

  EntryAccessor GetEntry(const PgPerformRequestPB& req, const CoarseTimePoint& deadline);

 private:
  class Impl;

  std::unique_ptr<Impl> impl_;
};

}  // namespace tserver
}  // namespace yb

#endif  // YB_TSERVER_PG_RESPONSE_CACHE_H
