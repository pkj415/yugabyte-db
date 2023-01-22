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
#include "yb/master/master_defaults.h"
#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/tablet_server.h"
#include "yb/util/metrics.h"
#include "yb/util/status.h"
#include "yb/util/test_macros.h"

#include "yb/yql/pgwrapper/libpq_utils.h"
#include "yb/yql/pgwrapper/pg_mini_test_base.h"

using namespace yb;

const client::YBTableName kAutoAnalyzeFullyQualifiedTableName(
    YQL_DATABASE_CQL, master::kSystemNamespaceName, master::kAutoAnalyzeTableName);

namespace yb {
namespace pgwrapper {
namespace {

class PgAutoAnalyzeTest : public PgMiniTestBase {
 protected:
  void SetUp() override {
    PgMiniTestBase::SetUp();
  }

  size_t NumTabletServers() override {
    return 1;
  }
};

} // namespace

TEST_F(PgAutoAnalyzeTest, YB_DISABLE_TEST_IN_TSAN(CheckTableMutationsCount)) {
  auto conn = ASSERT_RESULT(Connect());
  std::string table_name = "accounts";
  ASSERT_OK(conn.ExecuteFormat(
      "CREATE TABLE $0 (account_id INT PRIMARY KEY, balance INT)", table_name));
  ASSERT_OK(conn.ExecuteFormat("INSERT INTO $0 SELECT s, s FROM generate_series(1, 100) AS s"));
}

} // namespace pgwrapper
} // namespace yb
