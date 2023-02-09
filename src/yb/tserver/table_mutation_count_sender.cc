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

#include "yb/rpc/rpc_fwd.h"

#include "yb/server/server_base.pb.h"

#include "yb/tablet/tablet_peer.h"
#include "yb/tserver/pg_global_table_mutation_counter.h"
#include "yb/tserver/table_mutation_count_sender.h"
#include "yb/tserver/tablet_server.h"
#include "yb/tserver/tablet_server_options.h"
#include "yb/server/server_base.proxy.h"
#include "yb/master/master_auto_analyze.proxy.h"
#include "yb/master/master_rpc.h"

#include "yb/util/atomic.h"
#include "yb/util/enums.h"
#include "yb/util/logging.h"
#include "yb/util/net/net_util.h"
#include "yb/util/status.h"
#include "yb/util/thread.h"

using namespace std::literals;

DEFINE_RUNTIME_int32(table_mutation_count_sender_interval_ms, 5 * 1000,
  "Interval at which the table mutation counts are sent to analyze service.");

DECLARE_int32(yb_client_admin_operation_timeout_sec);

using std::shared_ptr;
using std::vector;
using std::set;
using strings::Substitute;

namespace yb {

using client::YBSession;
using client::YBTableName;
using client::YBqlOp;
using yb::master::GetLeaderMasterRpc;
using yb::rpc::RpcController;

namespace tserver {

// Most of the actual logic of the table mutation count sender is inside this inner class,
// to avoid having too many dependencies from the header itself.
//
// This is basically the "PIMPL" pattern.
class TableMutationCountSender::Thread {
 public:
  Thread(const TabletServerOptions& opts, TabletServer* server);

  Status Start();
  Status Stop();

  void set_master_addresses(server::MasterAddressesPtr master_addresses) {
    std::lock_guard<std::mutex> l(master_address_mtx_);

    std::vector<std::string> addresses;
    for (const auto& address : *master_addresses) {
      for (const auto& host_port : address) {
        addresses.push_back(host_port.ToString());
      }
    }

    master_addresses_string_ = JoinStrings(addresses, ",");

    if (client_ != NULL) {
      auto s = client_->SetMasterAddresses(master_addresses_string_);
      WARN_NOT_OK(
        s, "Problem setting master addresses for mutation counter sender thread: " + s.ToString());
    }

    VLOG_WITH_PREFIX(1) << "Setting master addresses to " << master_addresses_string_;
  }

 private:
  void RunThread();

  Status DoSendMutationCounts();

  bool IsCurrentThread() const;

  const std::string& LogPrefix() const {
    return log_prefix_;
  }

  // The server for which we are sending tables mutation counts.
  TabletServer* const server_;

  // The actual running thread (NULL before it is started)
  scoped_refptr<yb::Thread> thread_;

  // YBClient to the leader master.
  std::shared_ptr<client::YBClient> client_;

  // Mutex/condition pair to trigger the table mutation count sender thread
  std::mutex mutex_;
  std::condition_variable cond_;

  // Protecting master list and leader.
  std::mutex master_address_mtx_;

  std::string master_addresses_string_;

  // Protected by mutex_.
  bool should_run_;

  const std::string log_prefix_;

  TabletServerOptions opts_;

  DISALLOW_COPY_AND_ASSIGN(Thread);
};

////////////////////////////////////////////////////////////
// TableMutationCountSender
////////////////////////////////////////////////////////////

TableMutationCountSender::TableMutationCountSender(const TabletServerOptions& opts,
                                                   TabletServer* server)
  : thread_(new Thread(opts, server)) {
}
TableMutationCountSender::~TableMutationCountSender() {
  WARN_NOT_OK(Stop(), "Unable to stop table mutation count sender thread");
}

Status TableMutationCountSender::Start() {
  return thread_->Start();
}
Status TableMutationCountSender::Stop() {
  return thread_->Stop();
}

void TableMutationCountSender::set_master_addresses(server::MasterAddressesPtr master_addresses) {
  thread_->set_master_addresses(std::move(master_addresses));
}
////////////////////////////////////////////////////////////
// TableMutationCountSender::Thread
////////////////////////////////////////////////////////////

TableMutationCountSender::Thread::Thread(const TabletServerOptions& opts, TabletServer* server)
  : server_(server),
    log_prefix_(Format("P $0: ", server_->permanent_uuid())),
    opts_(opts) {
  set_master_addresses(opts_.GetMasterAddresses());
  VLOG_WITH_PREFIX(1) << "Initializing table mutation count sender thread";
}

Status TableMutationCountSender::Thread::DoSendMutationCounts() {
  CHECK(IsCurrentThread());

  if (client_ == NULL) {
    std::lock_guard<std::mutex> l(master_address_mtx_);

    client_ = VERIFY_RESULT(yb::client::YBClientBuilder()
                            .add_master_server_addr(master_addresses_string_)
                            .default_admin_operation_timeout(MonoDelta::FromSeconds(
                                FLAGS_yb_client_admin_operation_timeout_sec))
                            .Build(server_->messenger()));
  }

  std::unordered_map<TableId, std::atomic<uint64>> mutation_counts =
    server_->GetGlobalTableMutationCounter().GetAndClear();

  // Don't send RPC if there is no mutation for any table at all
  if (mutation_counts.size() == 0) {
    return Status::OK();
  }

  const Status s = client_->IncreaseMutationCounters(&mutation_counts);
  if (!s.ok()) {
    // If cluster-level aggregates are not updated, re-add tserver-level mutations back
    for (auto& table_id_count_pair : mutation_counts) {
      server_->GetGlobalTableMutationCounter().Increase(table_id_count_pair.first,
                                                        table_id_count_pair.second);
    }

    return s;
  }

  return Status::OK();
}

void TableMutationCountSender::Thread::RunThread() {
  CHECK(IsCurrentThread());
  VLOG_WITH_PREFIX(1) << "Table mutation count sender thread is starting";

  while (true) {
    auto deadline = CoarseMonoClock::now() + FLAGS_table_mutation_count_sender_interval_ms * 1ms;
    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait_until(lock, deadline);

    if (!should_run_) {
      VLOG_WITH_PREFIX(1) << "Table mutation count sender thread is finished";
      return;
    }

    Status s = DoSendMutationCounts();
    WARN_NOT_OK(s, "Failed to send table mutation counts, code = " + s.ToString());
  }
}

bool TableMutationCountSender::Thread::IsCurrentThread() const {
  return thread_.get() == yb::Thread::current_thread();
}

Status TableMutationCountSender::Thread::Start() {
  CHECK(thread_ == nullptr);

  std::lock_guard lock(mutex_);
  should_run_ = true;
  return yb::Thread::Create("table_mutation_count_sender", "table_mutation_count_send",
      &TableMutationCountSender::Thread::RunThread, this, &thread_);
}

Status TableMutationCountSender::Thread::Stop() {
  if (!thread_) {
    return Status::OK();
  }

  {
    cond_.notify_one();
    std::lock_guard lock(mutex_);
    should_run_ = false;
  }

  RETURN_NOT_OK(ThreadJoiner(thread_.get()).Join());
  thread_ = nullptr;
  return Status::OK();
}

} // namespace tserver
} // namespace yb
