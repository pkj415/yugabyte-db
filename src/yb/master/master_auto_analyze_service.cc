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
#include "yb/master/master_auto_analyze.service.h"
#include "yb/master/master_ddl.pb.h"
#include "yb/master/master_service_base.h"
#include "yb/master/master_service_base-internal.h"
#include "yb/util/service_util.h"
#include "yb/util/status.h"
#include "yb/util/status_format.h"
#include "yb/util/unique_lock.h"

using namespace std::literals;

namespace yb {
namespace master {

const client::YBTableName kAutoAnalyzeFullyQualifiedTableName(
    YQL_DATABASE_CQL, kSystemNamespaceName, kAutoAnalyzeTableName);

namespace {

using BfuncCompile = yb::bfql::BFCompileApi<DataType, DataType>;

// TODO(auto-analyze): Currently this service only aggregates mutation counts from all nodes.
// We will later add logic to periodically compare mutations with thresholds and trigger ANALYZE.


class MasterAutoAnalyzeServiceImpl : public MasterServiceBase, public MasterAutoAnalyzeIf {
 public:
  explicit MasterAutoAnalyzeServiceImpl(Master* master, CatalogManager* catalog_manager)
      : MasterServiceBase(master), MasterAutoAnalyzeIf(master->metric_entity()),
        master_(master),
        catalog_manager_(catalog_manager) {
    // TODO(auto-analyze): better to use a separate ybclient than to reuse the one used by CDC or
    // rename cdc_state_client_initializer to a more generic name. But we would anyway not need this
    // once this code moves to the stateful services framework, so okay to leave it as is for now.
    // Moreover, xcluster_safe_time_service re-uses it as well.
    auto* ybclient = master_->cdc_state_client_initializer().client();
    session_ = ybclient->NewSession();
  }

  void IncreaseMutationCounters(const IncreaseMutationCountersRequestPB* req,
                                IncreaseMutationCountersResponsePB* resp,
                                rpc::RpcContext rpc) override {
    VLOG(5) << "IncremenentMutationCounter req=" << req->ShortDebugString();

    auto* ybclient = master_->cdc_state_client_initializer().client();
    if (!PREDICT_TRUE(ybclient)) {
      LOG(ERROR) << "Client not initialized or shutting down";
      SetupErrorAndRespond(
          resp->mutable_error(), STATUS(IllegalState, "Client not initialized or shutting down"),
          MasterErrorPB_Code_UNKNOWN_ERROR, &rpc);
      return;
    }

    if (!PREDICT_TRUE(auto_analyze_table_ready_)) {
      auto status = CreateAutoAnalyzeTableIfNotFound(ybclient);
      if (!status.ok()) {
        LOG(ERROR) << "Failed to create " << kAutoAnalyzeFullyQualifiedTableName.ToString()
                   << " table. " << status;
        rpc.RespondFailure(status);
        return;
      }

      auto table = std::make_unique<client::TableHandle>();
      status = table->Open(kAutoAnalyzeFullyQualifiedTableName, ybclient);
      if (!status.ok()) {
        LOG(ERROR) << "Failed to open " << kAutoAnalyzeFullyQualifiedTableName.ToString()
                   << " table. " << status;
        SetupErrorAndRespond(resp->mutable_error(), status, MasterErrorPB_Code_UNKNOWN_ERROR, &rpc);
      }

      // TODO(auto-analyze): is swap safe when multiple threads try to swap auto_analyze_table_
      // concurrently? Again, this would won't be needed once we move the auto-analyze service to
      // the stateful services framework.
      auto_analyze_table_.swap(table);
      auto_analyze_table_ready_ = true;
    }

    // Increment mutation counters for tables
    const client::YBSchema& schema = auto_analyze_table_->schema();
    auto col_id = schema.ColumnId(schema.FindColumn(kAutoAnalyzeMutations));

    // INSERT entries for tables that don't exist in the auto analyze table
    for (int i = 0 ; i < req->table_mutation_counts_size() ; i++) {
      const auto op = auto_analyze_table_->NewWriteOp(QLWriteRequestPB::QL_STMT_UPDATE);
      auto* const upsert_req = op->mutable_request();
      QLExpressionPB *key_column = upsert_req->add_hashed_column_values();
      key_column->mutable_value()->set_string_value(req->table_mutation_counts(i).table_id());
      QLColumnValuePB *col_pb = upsert_req->add_column_values();
      col_pb->set_column_id(col_id);
      col_pb->mutable_expr()->mutable_value()->set_int64_value(0);
      upsert_req->mutable_if_expr()->mutable_condition()->set_op(::yb::QLOperator::QL_OP_NOT_EXISTS);
      VLOG(2) << "Insert table entry if does not exist - " << upsert_req->ShortDebugString();
      session_->Apply(op);
    }

    auto future = session_->FlushFuture();
    auto future_status = future.wait_for(ybclient->default_rpc_timeout().ToChronoMilliseconds());
    if (future_status != std::future_status::ready) {
      VLOG(1) << "Timed out waiting for write to the auto analyze table";
      SetupErrorAndRespond(
          resp->mutable_error(),
          STATUS(TimedOut, "Timed out waiting for write to the auto analyze table"),
          MasterErrorPB_Code_UNKNOWN_ERROR, &rpc);
      return;
    }

    // TODO(Piyush): Handle error at the per-op level
    client::FlushStatus flush_status = future.get();
    if (!flush_status.status.ok()) {
      VLOG(1) << "RPC to auto analyze table failed with status: " << flush_status.status;
      SetupErrorAndRespond(
          resp->mutable_error(), flush_status.status, MasterErrorPB_Code_UNKNOWN_ERROR, &rpc);
      return;
    }

    for (int i = 0 ; i < req->table_mutation_counts_size() ; i++) {
      const auto op = auto_analyze_table_->NewWriteOp(QLWriteRequestPB::QL_STMT_UPDATE);
      auto* const update_req = op->mutable_request();
      QLExpressionPB *key_column = update_req->add_hashed_column_values();
      key_column->mutable_value()->set_string_value(req->table_mutation_counts(i).table_id());
      update_req->mutable_column_refs()->add_ids(col_id);
      QLColumnValuePB *col_pb = update_req->add_column_values();
      col_pb->set_column_id(col_id);
      QLBCallPB* bfcall_expr_pb = col_pb->mutable_expr()->mutable_bfcall();
      bfcall_expr_pb->set_opcode(to_underlying(bfql::BFOpcode::OPCODE_AddI64I64_80));
      QLExpressionPB* operand1 = bfcall_expr_pb->add_operands();
      QLExpressionPB* operand2 = bfcall_expr_pb->add_operands();
      operand1->set_column_id(col_id);
      operand2->mutable_value()->set_int64_value(req->table_mutation_counts(i).mutation_count());
      VLOG(2) << "Increment table mutations - " << update_req->ShortDebugString();
      session_->Apply(op);
    }

    future = session_->FlushFuture();
    future_status = future.wait_for(ybclient->default_rpc_timeout().ToChronoMilliseconds());
    if (future_status != std::future_status::ready) {
      // TODO(auto-analyze): What if we keep timing out but the global mutation counter retries?
      // It might result in counting mutations multiple times.
      VLOG(1) << "Timed out waiting for write to the auto analyze table";
      SetupErrorAndRespond(
          resp->mutable_error(),
          STATUS(TimedOut, "Timed out waiting for write to the auto analyze table"),
          MasterErrorPB_Code_UNKNOWN_ERROR, &rpc);
      return;
    }

    // TODO(Piyush): Handle error at the per-op level
    flush_status = future.get();
    if (!flush_status.status.ok()) {
      VLOG(1) << "RPC to auto analyze table failed with status: " << flush_status.status;
      SetupErrorAndRespond(
          resp->mutable_error(), flush_status.status, MasterErrorPB_Code_UNKNOWN_ERROR, &rpc);
      return;
    }

    rpc.RespondSuccess();
  }

 private:
  Status CreateAutoAnalyzeTableIfNotFound(client::YBClient* ybclient) {
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

    Status status = catalog_manager_->CreateTable(&req, &resp, nullptr /*RpcContext*/);

    // We do not lock here so it is technically possible that the table was already created.
    // If so, there is nothing to do so we just ignore the "AlreadyPresent" error.
    if (!status.ok() && !status.IsAlreadyPresent()) {
      return status;
    }

    RETURN_NOT_OK(ybclient->WaitForCreateTableToFinish(kAutoAnalyzeFullyQualifiedTableName));
    auto_analyze_table_ready_ = true;

    return Status::OK();
  }

  Master* const master_;
  CatalogManager* const catalog_manager_;
  bool auto_analyze_table_ready_ = false;
  std::unique_ptr<client::TableHandle> auto_analyze_table_;
  std::shared_ptr<client::YBSession> session_;
};

} // namespace

std::unique_ptr<rpc::ServiceIf> MakeMasterAutoAnalyzeService(
    Master* master,  CatalogManager* catalog_manager) {
  return std::make_unique<MasterAutoAnalyzeServiceImpl>(master, catalog_manager);
}

} // namespace master
} // namespace yb
