# Test that standalone read-only statements at serializable isolation do not block on
# concurrent writes. When a standalone (outside a transaction block) read-only statement is
# executed with default_transaction_isolation = 'serializable', it should fall back to
# snapshot isolation and avoid taking read locks.
#
# Permutation 1: Standalone SELECT does NOT block on an in-progress concurrent UPDATE.
# Permutation 2: SELECT inside an explicit serializable transaction block DOES block
#                (baseline to show existing behavior is preserved).

setup
{
  CREATE TABLE test (k INT PRIMARY KEY, v INT);
  INSERT INTO test VALUES (1, 1);
}

teardown
{
  DROP TABLE test;
}

session "s1"
step "s1_begin" { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step "s1_update" { UPDATE test SET v = 2 WHERE k = 1; }
step "s1_commit" { COMMIT; }

session "s2"
setup { SET default_transaction_isolation = 'serializable'; }
step "s2_select" { SELECT * FROM test WHERE k = 1; }
step "s2_begin" { BEGIN; }
step "s2_select_in_txn" { SELECT * FROM test WHERE k = 1; }
step "s2_commit" { COMMIT; }
teardown { RESET default_transaction_isolation; }

# Standalone read-only SELECT should not block on the concurrent update.
permutation "s1_begin" "s1_update" "s2_select" "s1_commit"

# SELECT inside explicit txn block should block (existing serializable behavior).
permutation "s1_begin" "s1_update" "s2_begin" "s2_select_in_txn" "s1_commit" "s2_commit"
