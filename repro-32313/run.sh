#!/bin/bash
# Usage: ./run.sh [duration_secs] [host] [port]
#   ./run.sh 30                  -> 127.0.0.1:5433, 30s
#   ./run.sh 60 127.0.0.4        -> the other cluster, default port
#   ./run.sh 60 127.0.0.4 5433   -> explicit port
#   ./run.sh 30 6433             -> a bare number in arg 2 is taken as the port
# Env:
#   RR_LB=1          -> spread the 12 clients across all tservers (load-balance=true).
#                       Default is load-balance=false: every client pins to $HOST.
#   RR_NO_COLO=1     -> non-colocated control (db nocoloyb; expect 0 violations)
#   RR_NO_EXPR_IDX=1 -> drop the expression index
set -u
DUR=${1:-30}
ARG2=${2:-}
if [[ "$ARG2" =~ ^[0-9]+$ ]]; then HOST=127.0.0.1; PORT=$ARG2; else HOST=${ARG2:-127.0.0.1}; PORT=${3:-5433}; fi

YB=${YB_BUILD_ROOT:-$(cd "$(dirname "$0")/.." && pwd)/build/latest}
JAVA=${JAVA:-java}
DB=$([ "${RR_NO_COLO:-0}" = 1 ] && echo nocoloyb || echo coloyb)
LB=$([ "${RR_LB:-0}" = 1 ] && echo true || echo false)

echo "target: $HOST:$PORT db=$DB load-balance=$LB clients=${RR_CLIENTS:-12} rows=${RR_ROWS:-16} dur=${DUR}s"
"$YB/postgres/bin/ysqlsh" -h "$HOST" -p "$PORT" -U yugabyte -d yugabyte -v ON_ERROR_STOP=0 -c \
  "create database $DB $([ "$DB" = coloyb ] && echo 'with colocation = true')" 2>&1 | tail -1

# Session GUCs for debugging, set on the database so every new connection picks
# them up (avoids yb-ctl comma-splitting on ysql_pg_conf_csv). RR_DEBUG=1 to enable.
if [ "${RR_DEBUG:-0}" = 1 ]; then
  "$YB/postgres/bin/ysqlsh" -h "$HOST" -p "$PORT" -U yugabyte -d yugabyte -q -c \
    "alter database $DB set log_statement = 'all'" -c \
    "alter database $DB set yb_debug_log_internal_restarts = on"
  echo "debug GUCs: log_statement=all yb_debug_log_internal_restarts=on"
fi

exec "$JAVA" -cp jdbc-yugabytedb.jar:. RRRepro \
  "jdbc:yugabytedb://$HOST:$PORT/$DB?load-balance=$LB" "$DUR"
