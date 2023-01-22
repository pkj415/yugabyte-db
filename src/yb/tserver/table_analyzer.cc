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

#include "yb/tserver/table_analyzer.h"

#include <cstdint>
#include <iosfwd>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include <chrono>
#include <thread>
#include "yb/server/server_base.pb.h"

#ifdef __APPLE__
#include <mach/mach_init.h>
#include <mach/mach_error.h>
#include <mach/mach_host.h>
#include <mach/vm_map.h>
#else
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#endif

#include <boost/algorithm/string.hpp>

#include <rapidjson/document.h>

#include <glog/logging.h>

#include "yb/common/jsonb.h"
#include "yb/common/wire_protocol.h"
#include "yb/rpc/rpc_fwd.h"

#include "yb/gutil/bind.h"
#include "yb/client/client.h"
#include "yb/client/error.h"
#include "yb/client/schema.h"
#include "yb/client/session.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_op.h"
#include "yb/client/yb_table_name.h"

#include "yb/gutil/ref_counted.h"
#include "yb/gutil/stringprintf.h"
#include "yb/gutil/strings/escaping.h"
#include "yb/gutil/strings/substitute.h"

#include "yb/master/master_defaults.h"

#include "yb/tablet/tablet.h"
#include "yb/tablet/tablet_peer.h"
#include "yb/tserver/tablet_server.h"
#include "yb/tserver/tablet_server_options.h"
#include "yb/tserver/ts_tablet_manager.h"
#include "yb/server/server_base.proxy.h"
#include "yb/master/master_mutation_counter.proxy.h"
#include "yb/master/master_rpc.h"
#include "yb/client/client_fwd.h"
#include "yb/gutil/macros.h"

#include "yb/util/bytes_formatter.h"
#include "yb/util/capabilities.h"
#include "yb/util/date_time.h"
#include "yb/util/decimal.h"
#include "yb/util/enums.h"
#include "yb/util/flags.h"
#include "yb/util/logging.h"
#include "yb/util/mem_tracker.h"
#include "yb/util/metrics.h"
#include "yb/util/monotime.h"
#include "yb/util/net/net_util.h"
#include "yb/util/status.h"
#include "yb/util/status_log.h"
#include "yb/util/thread.h"
#include "yb/util/tsan_util.h"
#include "yb/util/varint.h"
#include "yb/tserver/pg_table_mutation_counter.h"

using namespace std::literals;

DEFINE_UNKNOWN_int32(table_analyzer_interval_ms, 3 * 1000,
             "Interval at which the tables are analyzed.");
TAG_FLAG(table_analyzer_interval_ms, advanced);

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

// Most of the actual logic of the table analyzer is inside this inner class,
// to avoid having too many dependencies from the header itself.
//
// This is basically the "PIMPL" pattern.
class TableAnalyzer::Thread {
 public:
  Thread(const TabletServerOptions& opts, TabletServer* server);

  Status Start();
  Status Stop();

  void set_master_addresses(server::MasterAddressesPtr master_addresses) {
    std::lock_guard<std::mutex> l(master_meta_mtx_);
    master_addresses_ = std::move(master_addresses);
    VLOG_WITH_PREFIX(1) << "Setting master addresses to " << yb::ToString(master_addresses_);
  }

 private:
  void RunThread();
  Status FindLeaderMaster(CoarseTimePoint deadline,
                          HostPort* leader_hostport) REQUIRES(master_meta_mtx_);;
  Status ConnectToMaster();
  int GetMillisUntilNextTableAnalyzer() const;

  Status DoTableAnalyze();

  bool IsCurrentThread() const;

  const std::string& LogPrefix() const {
    return log_prefix_;
  }

  // The server for which we are analyzing tables.
  TabletServer* const server_;

  // The actual running thread (NULL before it is started)
  scoped_refptr<yb::Thread> thread_;

  // Host and port of the most recent leader master.
  HostPort leader_master_hostport_;

  // Current RPC proxy to the leader master.
  std::unique_ptr<master::MasterMutationCounterProxy> proxy_;

  // Mutex/condition pair to trigger the table analyzer thread
  // to either snapshot early or exit.
  Mutex mutex_;
  ConditionVariable cond_;

  server::MasterAddressesPtr get_master_addresses_unlocked() {
    CHECK_NOTNULL(master_addresses_.get());
    return master_addresses_;
  }

  // Protecting master list and leader.
  std::mutex master_meta_mtx_;

  // The hosts/ports of masters that we may heartbeat to.
  //
  // We keep the HostPort around rather than a Sockaddr because the
  // masters may change IP addresses, and we'd like to re-resolve on
  // every new attempt at connecting.
  server::MasterAddressesPtr master_addresses_;

  // Protected by mutex_.
  bool should_run_ = false;

  const std::string log_prefix_;

  TabletServerOptions opts_;

  rpc::Rpcs rpcs_;

  DISALLOW_COPY_AND_ASSIGN(Thread);
};

////////////////////////////////////////////////////////////
// TableAnalyzer
////////////////////////////////////////////////////////////

TableAnalyzer::TableAnalyzer(const TabletServerOptions& opts, TabletServer* server)
  : thread_(new Thread(opts, server)) {
}
TableAnalyzer::~TableAnalyzer() {
  WARN_NOT_OK(Stop(), "Unable to stop table analyzer thread");
}

Status TableAnalyzer::Start() {
  return thread_->Start();
}
Status TableAnalyzer::Stop() {
  return thread_->Stop();
}

void TableAnalyzer::set_master_addresses(server::MasterAddressesPtr master_addresses) {
  LOG(WARNING) << "TABLE ANALYZER SET MASTER ADDRESS";
  thread_->set_master_addresses(std::move(master_addresses));
}
////////////////////////////////////////////////////////////
// TableAnalyzer::Thread
////////////////////////////////////////////////////////////

TableAnalyzer::Thread::Thread(const TabletServerOptions& opts, TabletServer* server)
  : server_(server),
    cond_(&mutex_),
    master_addresses_(opts.GetMasterAddresses()),
    log_prefix_(Format("P $0: ", server_->permanent_uuid())),
    opts_(opts) {
  VLOG_WITH_PREFIX(1) << "Initializing table analyzer thread";
}

namespace {

struct FindLeaderMasterData {
  HostPort result;
  Synchronizer sync;
  std::shared_ptr<GetLeaderMasterRpc> rpc;
};

void LeaderMasterCallback(const std::shared_ptr<FindLeaderMasterData>& data,
                          const Status& status,
                          const HostPort& result) {
  if (status.ok()) {
    data->result = result;
  }
  data->sync.StatusCB(status);
}

} // anonymous namespace

int TableAnalyzer::Thread::GetMillisUntilNextTableAnalyzer() const {
  return FLAGS_table_analyzer_interval_ms;
}

Status TableAnalyzer::Thread::DoTableAnalyze() {
  CHECK(IsCurrentThread());

  if (!proxy_) {
    VLOG_WITH_PREFIX(1) << "No valid master proxy. Connecting...";
    RETURN_NOT_OK(ConnectToMaster());
    DCHECK(proxy_);
  }

  master::MutationCounterRequestPB req;

  std::unordered_map<std::string, int> mutation_counts =
    tablemutationcounter::GetAllMutationCounts();

  for (auto& key_value_pair : mutation_counts) {
    req.add_table_id(key_value_pair.first);
    req.add_mutation_count(key_value_pair.second);
  }

  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_table_analyzer_interval_ms));

  master::MutationCounterResponsePB resp;

  LOG(WARNING) << " *** SENDING REQUEST TO MASTER";
  RETURN_NOT_OK_PREPEND(proxy_->IncremenentMutationCounter(req, &resp, &rpc),
                        "Failed to increment mutation counter");

  return Status::OK();
}

void TableAnalyzer::Thread::RunThread() {
  CHECK(IsCurrentThread());
  VLOG_WITH_PREFIX(1) << "Table analyzer thread starting";

  while (true) {
    MonoTime next_table_analyzer = MonoTime::Now();
    next_table_analyzer.AddDelta(
        MonoDelta::FromMilliseconds(GetMillisUntilNextTableAnalyzer()));

    // Wait for either the snapshot interval to elapse, or for the signal to shut down.
    {
      MutexLock l(mutex_);
      while (true) {
        MonoDelta remaining = next_table_analyzer.GetDeltaSince(MonoTime::Now());
        if (remaining.ToMilliseconds() <= 0 ||
            !should_run_) {
          break;
        }
        cond_.TimedWait(remaining);
      }

      if (!should_run_) {
        VLOG_WITH_PREFIX(1) << "Table analyzer thread finished";
        return;
      }
    }

    Status s = DoTableAnalyze();
    if (!s.ok()) {
      LOG_WITH_PREFIX(WARNING) << "Failed to analyze tables, code=" << s;
    }
  }
}

Status TableAnalyzer::Thread::FindLeaderMaster(CoarseTimePoint deadline,
                                             HostPort* leader_hostport) {
  Status s = Status::OK();
  const auto master_addresses = get_master_addresses_unlocked();
  if (master_addresses->size() == 1 && (*master_addresses)[0].size() == 1) {
    // "Shortcut" the process when a single master is specified.
    *leader_hostport = (*master_addresses)[0][0];
    return Status::OK();
  }
  auto master_sock_addrs = *master_addresses;
  if (master_sock_addrs.empty()) {
    return STATUS(NotFound, "Unable to resolve any of the master addresses!");
  }
  auto data = std::make_shared<FindLeaderMasterData>();
  data->rpc = std::make_shared<GetLeaderMasterRpc>(
      Bind(&LeaderMasterCallback, data),
      master_sock_addrs,
      deadline,
      server_->messenger(),
      &server_->proxy_cache(),
      &rpcs_,
      true /* should_timeout_to_follower_ */);
  data->rpc->SendRpc();
  auto status = data->sync.WaitFor(deadline - CoarseMonoClock::Now() + 1s);
  if (status.ok()) {
    *leader_hostport = data->result;
  }
  rpcs_.RequestAbortAll();
  return status;
}

Status TableAnalyzer::Thread::ConnectToMaster() {
  std::lock_guard<std::mutex> l(master_meta_mtx_);
  auto deadline = CoarseMonoClock::Now() + FLAGS_table_analyzer_interval_ms * 1ms;
  // TODO send heartbeats without tablet reports to non-leader masters.
  Status s = FindLeaderMaster(deadline, &leader_master_hostport_);
  if (!s.ok()) {
    LOG_WITH_PREFIX(INFO) << "Find leader master " <<  leader_master_hostport_.ToString()
                          << " hit error " << s;
    return s;
  }

  // Pings are common for both Master and Tserver.
  auto new_proxy = std::make_unique<server::GenericServiceProxy>(
      &server_->proxy_cache(), leader_master_hostport_);

  // Ping the master to verify that it's alive.
  server::PingRequestPB req;
  server::PingResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_table_analyzer_interval_ms));
  RETURN_NOT_OK_PREPEND(new_proxy->Ping(req, &resp, &rpc),
                        Format("Failed to ping master at $0", leader_master_hostport_));
  LOG_WITH_PREFIX(INFO) << "Connected to a leader master server at " << leader_master_hostport_;

  // Save state in the instance.
  proxy_ = std::make_unique<master::MasterMutationCounterProxy>(
      &server_->proxy_cache(), leader_master_hostport_);
  return Status::OK();
}

bool TableAnalyzer::Thread::IsCurrentThread() const {
  return thread_.get() == yb::Thread::current_thread();
}

Status TableAnalyzer::Thread::Start() {
  CHECK(thread_ == nullptr);

  should_run_ = true;
  return yb::Thread::Create("table_analyzer", "table_analyze",
      &TableAnalyzer::Thread::RunThread, this, &thread_);
}

Status TableAnalyzer::Thread::Stop() {
  if (!thread_) {
    return Status::OK();
  }

  {
    MutexLock l(mutex_);
    should_run_ = false;
    cond_.Signal();
  }
  RETURN_NOT_OK(ThreadJoiner(thread_.get()).Join());
  thread_ = nullptr;
  return Status::OK();
}

} // namespace tserver
} // namespace yb
