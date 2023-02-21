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

#include <memory>
#include <string>

#include "yb/client/yb_table_name.h"
#include "yb/common/common.pb.h"
#include "yb/integration-tests/cql_test_util.h"
#include "yb/master/master_defaults.h"
#include "yb/server/skewed_clock.h"
#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/tablet_server.h"
#include "yb/util/debug-util.h"
#include "yb/util/lw_function.h"
#include "yb/util/metrics.h"
#include "yb/util/status.h"
#include "yb/util/test_macros.h"

#include "yb/yql/pgwrapper/libpq_utils.h"
#include "yb/yql/pgwrapper/pg_mini_test_base.h"

DECLARE_bool(TEST_allow_skewed_clock_in_ysql);

namespace yb {
namespace pgwrapper {
namespace {

class PgClockSkewTest : public PgMiniTestBase {
 protected:
  void SetUp() override {
    FLAGS_TEST_allow_skewed_clock_in_ysql = true;
    server::SkewedClock::Register();
    PgMiniTestBase::SetUp();
  }

  size_t NumTabletServers() override {
    return 3;
  }

  std::vector<uint16_t> TabletServerClockSkew() override {
    return std::vector<uint16_t>({100, 200, 300});
  }
};

} // namespace

TEST_F(PgClockSkewTest, InTxnLimitBug) {
  // This test case is inspired from #15933 which is in turn caused due to #16034.
  // Below is a simpler test case that also fails due to #15933.
  //
  // TODO: The bug gets masked due to a fix in commit 03055c43. So, if you want to see the
  // constraint violation on the index key, that commit needs to be reverted. Does this test
  // serve any purpose in that case?
  auto conn = ASSERT_RESULT(Connect());

  ASSERT_OK(conn.Execute(
      "CREATE TABLE test (id int NOT NULL, value int DEFAULT 0 NOT NULL,"
      " CONSTRAINT test_pkey PRIMARY KEY((id) HASH));"));

  ASSERT_OK(conn.Execute(
      "CREATE UNIQUE INDEX test_value_key1 ON test USING lsm (value HASH)"
      " WHERE (value IS NOT NULL);"));

  ASSERT_OK(conn.Execute("INSERT INTO test SELECT s, s from generate_series(1, 10) as s;"));
  for (int i=0; i < 10; i++) {
    ASSERT_OK(conn.ExecuteFormat("UPDATE test SET value=$0 WHERE id = $1", i+10, i));
  }
}

} // namespace pgwrapper
} // namespace yb
