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

#include "yb/tserver/pg_response_cache.h"

#include <atomic>
#include <mutex>
#include <future>

#include "yb/client/yb_op.h"

#include "yb/common/wire_protocol.h"

#include "yb/gutil/casts.h"

#include "yb/rpc/rpc_context.h"

#include "yb/tserver/pg_client.pb.h"

#include "yb/util/logging.h"

namespace yb {
namespace tserver {

namespace {

std::string BuildCacheKey(const PgPerformRequestPB& req) {
  auto clone_req(req);
  clone_req.clear_session_id();
  for (auto& op : *clone_req.mutable_ops()) {
    op.mutable_read()->clear_stmt_id();
  }
  return clone_req.ShortDebugString();
}

struct Value {
  Value(PgPerformResponsePB resp_, MyOps ops_)
      : resp(std::move(resp_)), ops(std::move(ops_)), status(Status::OK()) {
  }

  explicit Value(const Status& status_)
      : status(status_) {
  }

  PgPerformResponsePB resp;
  MyOps ops;
  Status status;
};

YB_DEFINE_ENUM(EntryState, (kPending)(kInitialized)(kFailed));

} // namespace

class PgResponseCache::Entry {
public:
  Entry(const CoarseTimePoint& deadline, size_t id)
      : // id_(id),
        state_(EntryState::kPending),
        deadline_(deadline),
        future_(promise_.get_future()) {
  }

  void Get(PgPerformResponsePB* response, rpc::RpcContext* context) {
//    LOG(INFO) << "--MARKER-- getting data of " << id_;
    const auto& value = future_.get();
    if (!value.status.ok()) {
      StatusToPB(value.status, response->mutable_status());
    } else {
      *response = value.resp;
      auto& responses = *response->mutable_responses();
      responses.Reserve(narrow_cast<int>(value.ops.size()));
      size_t side_cars = 0;
      for (const auto& op : value.ops) {
        auto& op_resp = *responses.Add();
        op_resp = op->response();
        if (op_resp.has_rows_data_sidecar()) {
//          LOG(INFO) << "--MARKER-- sidecars size " << op->rows_data().size() << " hash " << op->rows_data().hash() << " for " << side_cars;
          op_resp.set_rows_data_sidecar(narrow_cast<int>(context->AddRpcSidecar(op->rows_data())));
          ++side_cars;
        }
      }
//      LOG(INFO) << "--MARKER-- ops " << value.ops.size() << " processed side cars " << side_cars << " response " << response->DebugString();
    }
    context->RespondSuccess();
  }

  void SetData(const PgPerformResponsePB& resp, const MyOps& ops) {
//    LOG(INFO) << "--MARKER-- Setting data for " << id_;
    state_.store(EntryState::kInitialized, std::memory_order_release);
    promise_.set_value(Value(resp, ops));
  }

  void SetFailure(Status failure) {
//    LOG(INFO) << "--MARKER-- Setting failure";
    state_.store(EntryState::kFailed, std::memory_order_release);
    promise_.set_value(Value(failure));
  }

  bool IsValid() const {
    const auto state = state_.load(std::memory_order_acquire);
    switch (state)
    {
      case EntryState::kPending: return CoarseMonoClock::Now() < deadline_;
      case EntryState::kInitialized: return true;
      case EntryState::kFailed: return false;
    }
    FATAL_INVALID_ENUM_VALUE(EntryState, state);
  }

private:
//  const size_t id_;
  std::atomic<EntryState> state_;
  const CoarseTimePoint deadline_;
  std::promise<Value> promise_;
  std::shared_future<Value> future_;
};

PgResponseCache::Getter::Getter(const EntryPtr& entry)
    : entry_(entry) {
}

PgResponseCache::Getter::~Getter() = default;

void PgResponseCache::Getter::Get(PgPerformResponsePB* response, rpc::RpcContext* context) {
  entry_->Get(response, context);
}

PgResponseCache::Setter::Setter(const EntryPtr& entry)
    : entry_(entry) {
}

PgResponseCache::Setter::~Setter() = default;

void PgResponseCache::Setter::SetData(const PgPerformResponsePB& resp, const MyOps& ops) {
  entry_->SetData(resp, ops);
}

void PgResponseCache::Setter::SetFailure(Status failure) {
  entry_->SetFailure(failure);
}

class PgResponseCache::Impl {
public:
  Impl() {
  }

  PgResponseCache::EntryAccessor GetEntry(const PgPerformRequestPB& req, const CoarseTimePoint& deadline) {
    const auto key = BuildCacheKey(req);
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = entries_.find(key);
    bool found = (it != entries_.end());
    if (found && !it->second->IsValid()) {
      entries_.erase(it);
      found = false;
    }
    if (!found) {
      it = entries_.emplace(key, std::make_shared<Entry>(deadline, ++next_id_)).first;
    }
//    LOG(INFO) << "--MARKER-- " << (found ? "FOUND" : "NOT FOUND") << " for key " << key;
    return found ? PgResponseCache::EntryAccessor(Getter(it->second))
                 : PgResponseCache::EntryAccessor(Setter(it->second));
  }

  std::mutex mutex_;
  std::unordered_map<std::string, EntryPtr> entries_;
  size_t next_id_;
};

PgResponseCache::PgResponseCache()
    : impl_(new Impl()) {
}

PgResponseCache::~PgResponseCache() = default;

PgResponseCache::EntryAccessor PgResponseCache::GetEntry(
  const PgPerformRequestPB& req, const CoarseTimePoint& deadline) {
  return impl_->GetEntry(req, deadline);
}

} // namespace tserver
} // namespace yb
