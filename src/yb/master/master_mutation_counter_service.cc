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

#include <mutex>
#include "yb/bfql/bfql.h"
#include "yb/bfql/gen_opcodes.h"
#include "yb/client/async_initializer.h"
#include "yb/client/error.h"
#include "yb/client/schema.h"
#include "yb/client/session.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_op.h"
#include "yb/client/yb_table_name.h"
#include "yb/common/common_flags.h"
#include "yb/common/common.pb.h"
#include "yb/common/ql_protocol.pb.h"

#include "yb/master/catalog_manager.h"
#include "yb/master/master_ddl.pb.h"
#include "yb/master/master_mutation_counter.service.h"
#include "yb/master/master_service_base.h"
#include "yb/master/master_service_base-internal.h"
#include "yb/util/service_util.h"
#include "yb/util/status.h"
#include "yb/util/status_format.h"
#include "yb/util/unique_lock.h"

using namespace std::literals;

DEFINE_NON_RUNTIME_uint32(auto_analyze_table_num_tablets, 1,
    "Number of tablets to use when creating the auto analyze table. "
    "0 to use the same default num tablets as for regular tables.");
TAG_FLAG(auto_analyze_table_num_tablets, advanced);

DEFINE_RUNTIME_bool(yb_enable_auto_analyze, false,
    "Toggle the auto-analyze feature which automatically ANALYZEs tables when they change "
    "significantly (the threshold for this is dictated by master gflags "
    "yb_auto_analyze_tuples_threshold and yb_auto_analyze_scale_factor).");

DEFINE_RUNTIME_bool(yb_enable_auto_analyze_per_table_mutations_counting, false,
    "This flag allows tracking per table mutation counts even when the auto-analyze is disabled. "
    "Mutation counts tracking can't be turned off if auto-analyze is enabled.");

DEFINE_RUNTIME_uint32(yb_auto_analyze_tuples_threshold, 50,
    "Specifies the minimum number of mutated tuples needed to trigger an ANALYZE in any table.");

DEFINE_RUNTIME_double(yb_auto_analyze_scale_factor, 0.2,
    "Mutations in a table need to be higher than this fraction of the table size on top of "
    "yb_auto_analyze_tuples_threshold to trigger an auto-analyze on the table.");

namespace yb {
namespace master {

const client::YBTableName kAutoAnalyzeFullyQualifiedTableName(
    YQL_DATABASE_CQL, kSystemNamespaceName, kAutoAnalyzeTableName);

namespace {

using BfuncCompile = yb::bfql::BFCompileApi<DataType, DataType>;

class MasterMutationCounterServiceImpl : public MasterServiceBase, public MasterMutationCounterIf {
 public:
  explicit MasterMutationCounterServiceImpl(Master* master, CatalogManager* catalog_manager)
      : MasterServiceBase(master), MasterMutationCounterIf(master->metric_entity()),
        master_(master),
        catalog_manager_(catalog_manager) {}

  void IncremenentMutationCounter(const MutationCounterRequestPB* req,
                                  MutationCounterResponsePB* resp, rpc::RpcContext rpc) override {
    VLOG(5) << "IncremenentMutationCounter req=" << req->ShortDebugString();

    auto* ybclient = master_->cdc_state_client_initializer().client();
    if (!ybclient) {
      SetupErrorAndRespond(
          resp->mutable_error(), STATUS(IllegalState, "Client not initialized or shutting down"),
          MasterErrorPB_Code_UNKNOWN_ERROR, &rpc);
      return;
    }

    if (!auto_analyze_table_ready_) {
      // TODO(auto-analyze): change to a Pg sys catalog table for the following reasons -
      //  1. If present in sys catalog, we won't have to create the table here. Also, we won't have
      //     to check if the table exists or not.
      //  2. Users can query the table from YSQL itself to see information about the current and
      //     previous ANALYZE.
      //  3. We will be able to atomically reduce the mutations in the auto analyze table and set
      //     reltuples and other statistics using a single DDL transaction.
      auto status = CreateAutoAnalyzeTableIfNotFound();
      if (!status.ok()) {
        rpc.RespondFailure(status);
        return;
      }
      auto table = std::make_unique<client::TableHandle>();
      auto s = table->Open(kAutoAnalyzeFullyQualifiedTableName, ybclient);
      if (!s.ok()) {
        SetupErrorAndRespond(resp->mutable_error(), s, MasterErrorPB_Code_UNKNOWN_ERROR, &rpc);
      }
      auto_analyze_table_.swap(table);
      auto_analyze_table_ready_ = true;
    }

    // Return NOT_LEADER error if not leader

    // inc all tables
    const client::YBSchema& schema = auto_analyze_table_->schema();
    auto col_id = schema.ColumnId(schema.FindColumn(kAutoAnalyzeMutations));
    std::shared_ptr<client::YBSession> session = ybclient->NewSession();

    // INSERT entries for tables that don't exist in the auto analyze table
    for (int i = 0 ; i < req->table_id_size() ; i++) {
      const auto op = auto_analyze_table_->NewWriteOp(QLWriteRequestPB::QL_STMT_UPDATE);
      auto* const update_req = op->mutable_request();
      QLExpressionPB *key_column = update_req->add_hashed_column_values();
      key_column->mutable_value()->set_string_value(req->table_id(i));
      QLColumnValuePB *col_pb = update_req->add_column_values();
      col_pb->set_column_id(col_id);
      col_pb->mutable_expr()->mutable_value()->set_int64_value(0);
      update_req->mutable_if_expr()->mutable_condition()->set_op(::yb::QLOperator::QL_OP_NOT_EXISTS);
      LOG(INFO) << "Piyush insert if not exists - " << update_req->ShortDebugString();
      session->Apply(op);
    }

    auto future = session->FlushFuture();
    auto future_status = future.wait_for(ybclient->default_rpc_timeout().ToChronoMilliseconds());
    if (future_status != std::future_status::ready) {
      LOG(WARNING) << "Piyush here1";
      SetupErrorAndRespond(
          resp->mutable_error(),
          STATUS(IllegalState, "Timedout waiting for yb client flush"),
          MasterErrorPB_Code_UNKNOWN_ERROR, &rpc);
      return;
    }

    // TODO(Piyush): Handle error at the per-op level
    client::FlushStatus flush_status = future.get();
    if (!flush_status.status.ok()) {
      LOG(WARNING) << "Piyush here2 " << flush_status.status;
      SetupErrorAndRespond(resp->mutable_error(), flush_status.status, MasterErrorPB_Code_UNKNOWN_ERROR, &rpc);
      return;
    }

    for (int i = 0 ; i < req->table_id_size() ; i++) {
      const auto op = auto_analyze_table_->NewWriteOp(QLWriteRequestPB::QL_STMT_UPDATE);
      auto* const update_req = op->mutable_request();
      QLExpressionPB *key_column = update_req->add_hashed_column_values();
      key_column->mutable_value()->set_string_value(req->table_id(i));
      update_req->mutable_column_refs()->add_ids(col_id);
      QLColumnValuePB *col_pb = update_req->add_column_values();
      col_pb->set_column_id(col_id);
      QLBCallPB* bfcall_expr_pb = col_pb->mutable_expr()->mutable_bfcall();
      bfcall_expr_pb->set_opcode(to_underlying(bfql::BFOpcode::OPCODE_AddI64I64_80));
      QLExpressionPB* operand1 = bfcall_expr_pb->add_operands();
      QLExpressionPB* operand2 = bfcall_expr_pb->add_operands();
      operand1->set_column_id(col_id);
      operand2->mutable_value()->set_int64_value(req->mutation_count(i));      
      session->Apply(op);
    }

    future = session->FlushFuture();
    future_status = future.wait_for(ybclient->default_rpc_timeout().ToChronoMilliseconds());
    if (future_status != std::future_status::ready) {
      LOG(WARNING) << "Piyush here1";
      SetupErrorAndRespond(
          resp->mutable_error(),
          STATUS(IllegalState, "Timedout waiting for yb client flush"),
          MasterErrorPB_Code_UNKNOWN_ERROR, &rpc);
      return;
    }

    // TODO(Piyush): Handle error at the per-op level
    flush_status = future.get();
    if (!flush_status.status.ok()) {
      LOG(WARNING) << "Piyush here2 " << flush_status.status;
      SetupErrorAndRespond(resp->mutable_error(), flush_status.status, MasterErrorPB_Code_UNKNOWN_ERROR, &rpc);
      return;
    }

    // Get reltuples from pg_class if not existing here

    // Trigger ANALYZE if needed and fetch reltuples too

    // Throttle ANALYZE

    rpc.RespondSuccess();
  }

 private:
  Result<int64_t> GetLeaderTermFromCatalogManager() {
    SCOPED_LEADER_SHARED_LOCK(l, catalog_manager_);

    if (!l.IsInitializedAndIsLeader()) {
      return l.first_failed_status();
    }

    return l.GetLeaderReadyTerm();
  }

  Status CreateAutoAnalyzeTableIfNotFound() {
    if (PREDICT_TRUE(VERIFY_RESULT(
            catalog_manager_->TableExists(kSystemNamespaceName, kAutoAnalyzeTableName)))) {
      return Status::OK();
    }

    // Set up a CreateTable request internally.
    CreateTableRequestPB req;
    CreateTableResponsePB resp;
    req.set_name(kAutoAnalyzeTableName);
    req.mutable_namespace_()->set_name(kSystemNamespaceName);
    req.set_table_type(TableType::YQL_TABLE_TYPE);

    // Schema:
    // table_id string (HASH), mutations_since_last_analyze int64, last_analyze_duration_ms int64
    client::YBSchemaBuilder schema_builder;
    schema_builder.AddColumn(kAutoAnalyzeTableId)->HashPrimaryKey()->Type(DataType::STRING);
    schema_builder.AddColumn(kAutoAnalyzeMutations)->Type(DataType::INT64);
    schema_builder.AddColumn(kAutoAnalyzeLastAnalyzeInfo)->Type(DataType::JSONB);
    schema_builder.AddColumn(kAutoAnalyzeCurrentAnalyzeInfo)->Type(DataType::JSONB);

    client::YBSchema yb_schema;
    RETURN_NOT_OK(schema_builder.Build(&yb_schema));

    const auto& schema = yb::client::internal::GetSchema(yb_schema);
    SchemaToPB(schema, req.mutable_schema());

    // Explicitly set the number tablets if the corresponding flag is set, otherwise CreateTable
    // will use the same defaults as for regular tables.
    if (FLAGS_auto_analyze_table_num_tablets > 0) {
      req.mutable_schema()->mutable_table_properties()->set_num_tablets(
          FLAGS_auto_analyze_table_num_tablets);
    }

    Status status = catalog_manager_->CreateTable(&req, &resp, nullptr /*RpcContext*/);

    // We do not lock here so it is technically possible that the table was already created.
    // If so, there is nothing to do so we just ignore the "AlreadyPresent" error.
    if (!status.ok() && !status.IsAlreadyPresent()) {
      return status;
    }

    // RETURN_NOT_OK(catalog_manager_->WaitForCreateTableToFinish(kAutoAnalyzeFullyQualifiedTableName));
    auto_analyze_table_ready_ = true;

    return Status::OK();
  }

  Master* const master_;
  CatalogManager* const catalog_manager_;
  bool auto_analyze_table_ready_ = false;
  std::unique_ptr<client::TableHandle> auto_analyze_table_;
};

} // namespace

std::unique_ptr<rpc::ServiceIf> MakeMasterMutationCounterService(
    Master* master, CatalogManager* catalog_manager) {
  return std::make_unique<MasterMutationCounterServiceImpl>(master, catalog_manager);
}

} // namespace master
} // namespace yb

// bfql::BFOpcode opcode_for_addition;
// Status s = BfuncCompile::FindQLOpcode("+", std::vector<DataType>(UINT64, UINT64), &opcode_for_addition, nullptr, nullptr);
// if (!s.ok()) {
//   LOG(INFO) << "Piyush - faced error in FindQLOpcode: " << s;
//   SetupErrorAndRespond(resp->mutable_error(), s, MasterErrorPB_Code_UNKNOWN_ERROR, &rpc);
//   return;
// }
// LOG(INFO) << "Piyush - opcode_for_addition=" << (int32_t) opcode_for_addition;