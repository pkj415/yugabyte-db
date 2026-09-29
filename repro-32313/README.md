# Reproducer for #32313 — REPEATABLE READ snapshot violation on colocated tables

A read-only `REPEATABLE READ` transaction reads the same colocated table twice
and observes two different results, committing without a `40001` read restart.

See https://github.com/yugabyte/yugabyte-db/issues/32313 for the root cause
analysis. In short: the reader's iterators hold creation-time snapshots, but
transaction metadata is live state. If the iterators are created before a
writer's apply, and the intent is probed after that writer's metadata has been
cleaned up, the status lookup returns `NotFound`, the intent is treated as
never committed (`commit_ht = HybridTime::kMin`), and the committed row version
is in neither the regular-DB snapshot nor the intents. The read silently
returns a stale value.

## Setup

Single node is enough. Colocation is the trigger because one tablet carries all
the traffic, so applies and cleanups collide with in-flight scans constantly.

```
./bin/yb-ctl create --rf=1 --ip_start 4
```

Stretch the commit-to-apply window slightly. Keep the value small: a large one
(e.g. 200) suppresses the bug, because apply and cleanup then land after the
scan has finished and the probe finds metadata.

```
./bin/yb-ts-cli --server_address=127.0.0.4:9100 set_flag \
  TEST_txn_participant_inject_latency_on_apply_update_txn_ms 5
```

Fetch the YugabyteDB JDBC driver next to the reproducer:

```
cd repro-32313
curl -sSLO https://repo1.maven.org/maven2/com/yugabyte/jdbc-yugabytedb/42.7.3-yb-4/jdbc-yugabytedb-42.7.3-yb-4.jar
mv jdbc-yugabytedb-42.7.3-yb-4.jar jdbc-yugabytedb.jar
javac -cp jdbc-yugabytedb.jar RRRepro.java
```

## Run

```
RR_ROWS=500 RR_LB=1 ./run.sh 60 127.0.0.4
```

Expect roughly 5 violations per minute. Each is printed as it happens with the
backend pid and statement boundaries, so it can be matched against the tserver
log:

```
*** VIOLATION backend_pid=42573
    R1  start=2026-09-10 06:12:15.736 end=2026-09-10 06:12:15.818
    R2  start=2026-09-10 06:12:15.819 end=2026-09-10 06:12:15.828
    COMMIT at 2026-09-10 06:12:15.829
    diff: id=159 r1=1000 r2=1025;
```

`RR_ROWS` matters most. Long scans are the amplifier: the violation needs an
apply to land between iterator creation and the intent probe, and the 16-row
default scans complete too quickly to hit it often (roughly 1 violation per
1900 double-reads).

### Options

| Variable | Default | Meaning |
|---|---|---|
| `RR_ROWS` | 16 | Rows in `rr_acct`. Use 500 for long scans. |
| `RR_CLIENTS` | 12 | Concurrent client threads. |
| `RR_LB` | 0 | `1` spreads connections across tservers. |
| `RR_DEBUG` | 0 | `1` sets `log_statement=all` and `yb_debug_log_internal_restarts=on` on the database. |
| `RR_NO_COLO` | 0 | `1` runs against a non-colocated database (control; expect no violations). |
| `RR_NO_EXPR_IDX` | 0 | `1` drops the expression index. |
| `YB_BUILD_ROOT` | `../build/latest` | Build directory holding `postgres/bin/ysqlsh`. |
| `JAVA` | `java` | JDK 8+ to use. |

Arguments are `./run.sh [duration_secs] [host] [port]`.

## Observing the mechanism

```
./bin/yb-ts-cli --server_address=127.0.0.4:9100 set_flag vmodule \
  "pg_client_session=3,transaction_participant=2,transaction_status_cache=4,intent_format=4"
```

For a violating read, the tserver log shows a transaction whose `commit_ht`
precedes the reader's read time being resolved as `intent w/o metadata`, and the
matching decode line for that key reporting `commit ht: <min>`. Note these logs
are large; keep runs short.
