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

#include "yb/util/test_thread_holder.h"
#include "yb/yql/pgwrapper/pg_mini_test_base.h"

DECLARE_bool(enable_wait_queues);
DECLARE_bool(yb_enable_advisory_lock);
DECLARE_bool(ysql_yb_enable_advisory_lock);
DECLARE_bool(yb_enable_read_committed_isolation);
DECLARE_uint32(num_advisory_locks_tablets);

namespace yb::pgwrapper {

const std::string session_level_locks[] = {
  "select pg_advisory_lock(1)",
  "select pg_advisory_unlock(1)",
  "select pg_advisory_lock_shared(1)",
  "select pg_advisory_unlock_shared(1)",
  "select pg_advisory_lock(1, 1)",
  "select pg_advisory_unlock(1, 1)",
  "select pg_advisory_lock_shared(1, 1)",
  "select pg_advisory_unlock_shared(1, 1)",
  "select pg_try_advisory_lock(1)",
  "select pg_try_advisory_lock(1, 1)",
  "select pg_try_advisory_lock_shared(1)",
  "select pg_try_advisory_lock_shared(1, 1)",
};

const std::string xact_level_locks[] = {
  "select pg_advisory_xact_lock(1)",
  "select pg_advisory_xact_lock_shared(1)",
  "select pg_advisory_xact_lock(1, 1)",
  "select pg_advisory_xact_lock_shared(1, 1)",
  "select pg_try_advisory_xact_lock(1)",
  "select pg_try_advisory_xact_lock(1, 1)",
  "select pg_try_advisory_xact_lock_shared(1)",
  "select pg_try_advisory_xact_lock_shared(1, 1)",
};

class PgAdvisoryLockTestBase : public PgMiniTestBase {
 protected:
  void CheckStmtNotFullyImplemented(const std::string& stmt) {
    auto conn = ASSERT_RESULT(Connect());
    auto status = conn.Execute(stmt);
    ASSERT_NOK(status);
    ASSERT_STR_CONTAINS(status.message().ToBuffer(),
                        "session-level advisory locks are not yet implemented");
  }

  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_yb_enable_read_committed_isolation) = true;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_yb_enable_advisory_lock) = true;
    PgMiniTestBase::SetUp();
  }
};

class PgAdvisoryLockTest : public PgAdvisoryLockTestBase {
 protected:
  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_wait_queues) = true;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_ysql_yb_enable_advisory_lock) = true;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_num_advisory_locks_tablets) = 1;
    PgAdvisoryLockTestBase::SetUp();
  }
};

TEST_F(PgAdvisoryLockTest, XactAdvisoryLock) {
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
  for (const auto& xact_level_lock : xact_level_locks) {
    ASSERT_OK(conn.Fetch(xact_level_lock));
  }
  ASSERT_OK(conn.CommitTransaction());
}

TEST_F(PgAdvisoryLockTest, XactAdvisoryLockWithTwoIntKeys) {
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
  ASSERT_OK(conn.Fetch("select pg_advisory_xact_lock(1)"));

  auto conn2 = ASSERT_RESULT(Connect());
  ASSERT_OK(conn2.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
  ASSERT_OK(conn2.Fetch("select pg_advisory_xact_lock(0, 1)"));

  auto lock_result = ASSERT_RESULT(conn.FetchRow<bool>(
      "select pg_try_advisory_xact_lock_shared(0, 1)"));
  ASSERT_FALSE(lock_result);
  ASSERT_OK(conn.CommitTransaction());
}

TEST_F(PgAdvisoryLockTest, XactAdvisoryLockWaitOnConflict) {
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
  // Conn1: acquire exclusive lock on 1.
  ASSERT_OK(conn.Fetch("select pg_advisory_xact_lock(1)"));

  std::atomic<bool> another_conn_acquired{false};
  TestThreadHolder thread_holder;
  thread_holder.AddThreadFunctor([&] {
    auto another_conn = ASSERT_RESULT(Connect());
    ASSERT_OK(another_conn.Fetch("select pg_advisory_xact_lock(1)"));
    ASSERT_OK(another_conn.CommitTransaction());
    another_conn_acquired.store(true);
  });
  SleepFor(1s);
  ASSERT_FALSE(another_conn_acquired.load());
  ASSERT_OK(conn.CommitTransaction());
  thread_holder.JoinAll();
  ASSERT_TRUE(another_conn_acquired.load());
}

TEST_F(PgAdvisoryLockTest, XactAdvisoryLockSkipOnConflict) {
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));

  // Conn1: acquire exclusive lock on 1.
  ASSERT_OK(conn.Fetch("select pg_advisory_xact_lock(1)"));
  // Conn1: acquire the second exclusive lock on 1 with non-blocking method.
  auto lock_result = ASSERT_RESULT(conn.FetchRow<bool>("select pg_try_advisory_xact_lock(1)"));
  ASSERT_TRUE(lock_result);

  auto another_conn = ASSERT_RESULT(Connect());
  // Conn2: acquire exclusive lock on 1 with non-blocking method. Should fail immediately.
  lock_result = ASSERT_RESULT(another_conn.FetchRow<bool>("select pg_try_advisory_xact_lock(1)"));
  ASSERT_FALSE(lock_result);
  // Conn2: acquire shared lock on 1 with non-blocking method. Should fail immediately.
  lock_result = ASSERT_RESULT(another_conn.FetchRow<bool>(
      "select pg_try_advisory_xact_lock_shared(1)"));
  ASSERT_FALSE(lock_result);

  // Conn1: release all advisory locks acquired by conn1.
  ASSERT_OK(conn.CommitTransaction());

  ASSERT_OK(another_conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
  // Conn2: acquire shared lock on 1 with non-blocking method. Should succeed.
  lock_result = ASSERT_RESULT(another_conn.FetchRow<bool>(
      "select pg_try_advisory_xact_lock_shared(1)"));
  ASSERT_TRUE(lock_result);

  ASSERT_OK(conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
  // Conn1: acquire shared lock on 1 with non-blocking method. Should succeed.
  lock_result = ASSERT_RESULT(conn.FetchRow<bool>("select pg_try_advisory_xact_lock_shared(1)"));
  ASSERT_TRUE(lock_result);

  // Conn2: acquire shared lock on 1 with non-blocking method. Should fail immediately because
  //        it's conflicting with the shared lock acquired by conn1.
  lock_result = ASSERT_RESULT(another_conn.FetchRow<bool>("select pg_try_advisory_xact_lock(1)"));
  ASSERT_FALSE(lock_result);

  ASSERT_OK(conn.CommitTransaction());
  ASSERT_OK(another_conn.CommitTransaction());
}

TEST_F(PgAdvisoryLockTest, SessionLevelAdvisoryLockNotImplemented) {
  for (const auto& session_level_lock : session_level_locks) {
    CheckStmtNotFullyImplemented(session_level_lock);
  }
  CheckStmtNotFullyImplemented("select pg_advisory_unlock_all()");
}

TEST_F(PgAdvisoryLockTest, AcquireXactLocksInDifferentDBs) {
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("CREATE DATABASE db1"));
  ASSERT_OK(conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
  ASSERT_OK(conn.Fetch("select pg_advisory_xact_lock(1)"));
  auto conn2 = ASSERT_RESULT(ConnectToDB("db1"));
  ASSERT_OK(conn2.Fetch("select pg_advisory_xact_lock(1)"));
  ASSERT_OK(conn.CommitTransaction());
}

TEST_F(PgAdvisoryLockTest, RollbackXactAdvisoryLocks) {
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
  ASSERT_OK(conn.Fetch("select pg_advisory_xact_lock(1)"));

  auto conn2 = ASSERT_RESULT(Connect());
  auto lock_result = ASSERT_RESULT(conn2.FetchRow<bool>("select pg_try_advisory_xact_lock(1)"));
  ASSERT_FALSE(lock_result);

  ASSERT_OK(conn.RollbackTransaction());
  lock_result = ASSERT_RESULT(conn2.FetchRow<bool>("select pg_try_advisory_xact_lock(1)"));
  ASSERT_TRUE(lock_result);
}

TEST_F(PgAdvisoryLockTest, Savepoints) {
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
  ASSERT_OK(conn.Execute("savepoint s1"));
  ASSERT_OK(conn.Execute("savepoint s2"));
  ASSERT_OK(conn.Fetch("select pg_advisory_xact_lock(1)"));

  auto conn2 = ASSERT_RESULT(Connect());
  auto lock_result = ASSERT_RESULT(conn2.FetchRow<bool>("select pg_try_advisory_xact_lock(1)"));
  ASSERT_FALSE(lock_result);

  ASSERT_OK(conn.Execute("release savepoint s2"));
  lock_result = ASSERT_RESULT(conn2.FetchRow<bool>("select pg_try_advisory_xact_lock(1)"));
  ASSERT_FALSE(lock_result);

  ASSERT_OK(conn.Execute("rollback to savepoint s1"));
  lock_result = ASSERT_RESULT(conn2.FetchRow<bool>("select pg_try_advisory_xact_lock(1)"));
  ASSERT_TRUE(lock_result);
}

class PgAdvisoryLockWithWaitQueueDisabledTest : public PgAdvisoryLockTestBase {
 protected:
  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_wait_queues) = false;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_ysql_yb_enable_advisory_lock) = true;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_num_advisory_locks_tablets) = 1;
    PgAdvisoryLockTestBase::SetUp();
  }
};

TEST_F(PgAdvisoryLockWithWaitQueueDisabledTest, XactAdvisoryLockFailOnConflict) {
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("SET yb_transaction_priority_lower_bound = 0.5"));
  ASSERT_OK(conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
  // Conn1: acquire exclusive lock on 1.
  ASSERT_OK(conn.Fetch("select pg_advisory_xact_lock(1)"));

  auto another_conn = ASSERT_RESULT(Connect());
  ASSERT_OK(another_conn.Execute("SET yb_transaction_priority_upper_bound = 0.4"));
  ASSERT_OK(another_conn.Execute("SET yb_max_query_layer_retries=5"));
  auto res = another_conn.Fetch("select pg_advisory_xact_lock(1)");
  ASSERT_NOK(res);
  ASSERT_STR_CONTAINS(res.status().message().ToBuffer(),
                      "could not serialize access due to concurrent update");
}

class PgAdvisoryLockNotSupportedTest : public PgAdvisoryLockTestBase {
 protected:
  void CheckStmtNotSupported(const std::string& stmt) {
    auto conn = ASSERT_RESULT(Connect());
    auto status = conn.Execute(stmt);
    ASSERT_NOK(status);
    ASSERT_STR_CONTAINS(status.message().ToBuffer(), "advisory locks are not yet implemented");
  }

  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_ysql_yb_enable_advisory_lock) = false;
    PgAdvisoryLockTestBase::SetUp();
  }
};

TEST_F(PgAdvisoryLockNotSupportedTest, AdvisoryLockNotSupported) {
  for (const auto& session_level_lock : session_level_locks) {
    CheckStmtNotSupported(session_level_lock);
  }
  for (const auto& xact_level_lock : xact_level_locks) {
    CheckStmtNotSupported(xact_level_lock);
  }
}

} // namespace yb::pgwrapper
