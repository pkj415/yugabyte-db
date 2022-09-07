setup
{
  CREATE TABLE test (
     k INT PRIMARY KEY,
     v INT
  );

  INSERT INTO test VALUES (1, 2);
}

teardown
{
  DROP TABLE test;
}

session "s1"
setup { SET yb_transaction_priority_lower_bound = 0.6; }

# Commands for 4 methods to start a transaction in a desired isolation level
step "s1_begin_rc_method1" { BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s1_begin_rr_method1" { BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s1_begin_sr_method1" { BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; }

step "s1_begin_rc_method2" { BEGIN; SET TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s1_begin_rr_method2" { BEGIN; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s1_begin_sr_method2" { BEGIN; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; }

step "s1_begin_rc_method3_part1" { SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s1_begin_rr_method3_part1" { SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s1_begin_sr_method3_part1" { SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE; }
# "BEGIN;" is added as a separate step to ensure this picks the new default isolation level that is set.
# If it is run as a statement as part of the same client issued request, it doesn't pick the newly set isolation level.
step "s1_method3_part2" { BEGIN; }

step "s1_begin_rc_method4" { SET default_transaction_isolation='READ COMMITTED'; BEGIN; }
step "s1_begin_rr_method4" { SET default_transaction_isolation='REPEATABLE READ'; BEGIN; }
step "s1_begin_sr_method4" { SET default_transaction_isolation='SERIALIZABLE'; BEGIN; }

step "s1_switch_to_rc" { SET TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s1_switch_to_rr" { SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s1_switch_to_sr" { SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; }

step "s1_select"		{ SELECT * FROM test; }
step "s1_update"		{ UPDATE test SET v=v+1 WHERE k=1; }
step "s1_savepoint"		{ SAVEPOINT a; }
step "s1_deferrable" { SET TRANSACTION DEFERRABLE; }
step "s1_commit"		{ COMMIT; }
step "s1_rollback"		{ ROLLBACK; }

session "s2"
setup { SET yb_transaction_priority_upper_bound = 0.4; }

# Commands for 4 methods to start a transaction in a desired isolation level
step "s2_begin_rc_method1" { BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s2_begin_rr_method1" { BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s2_begin_sr_method1" { BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; }

step "s2_begin_rc_method2" { BEGIN; SET TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s2_begin_rr_method2" { BEGIN; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s2_begin_sr_method2" { BEGIN; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; }

step "s2_begin_rc_method3_part1" { SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s2_begin_rr_method3_part1" { SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s2_begin_sr_method3_part1" { SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE; }
step "s2_method3_part2" { BEGIN; }

step "s2_begin_rc_method4" { SET default_transaction_isolation='READ COMMITTED'; BEGIN; }
step "s2_begin_rr_method4" { SET default_transaction_isolation='REPEATABLE READ'; BEGIN; }
step "s2_begin_sr_method4" { SET default_transaction_isolation='SERIALIZABLE'; BEGIN; }

step "s2_switch_to_rc" { SET TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s2_switch_to_rr" { SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s2_switch_to_sr" { SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; }

step "s2_select"		{ SELECT * FROM test; }
step "s2_update"		{ UPDATE test SET v=v*2 WHERE k=1; }
step "s2_deferrable" { SET TRANSACTION DEFERRABLE; }
step "s2_read_only" { SET TRANSACTION READ ONLY; }
step "s2_read_write" { SET TRANSACTION READ WRITE; }
step "s2_commit"		{ COMMIT; }
step "s2_rollback"		{ ROLLBACK; }

# Test all possibilities of switching from RC/ SR to RR.
permutation "s1_begin_rc_method1" "s2_begin_sr_method1" "s1_switch_to_rr" "s2_switch_to_rr" "s1_select" "s2_select" "s1_update" "s2_update" "s1_commit" "s2_rollback" "s2_select"
permutation "s1_begin_rc_method2" "s2_begin_sr_method2" "s1_switch_to_rr" "s2_switch_to_rr" "s1_select" "s2_select" "s1_update" "s2_update" "s1_commit" "s2_rollback" "s2_select"
permutation "s1_begin_rc_method3_part1" "s1_method3_part2" "s2_begin_sr_method3_part1" "s2_method3_part2" "s1_switch_to_rr" "s2_switch_to_rr" "s1_select" "s2_select" "s1_update" "s2_update" "s1_commit" "s2_rollback" "s2_select"
permutation "s1_begin_rc_method4" "s2_begin_sr_method4" "s1_switch_to_rr" "s2_switch_to_rr" "s1_select" "s2_select" "s1_update" "s2_update" "s1_commit" "s2_rollback" "s2_select"

# Test all possibilities of switching from RR/ SR to RC.
permutation "s1_begin_rr_method1" "s2_begin_sr_method1" "s1_switch_to_rc" "s2_switch_to_rc" "s1_select" "s2_select" "s1_update" "s2_update" "s1_commit" "s2_commit" "s2_select"
permutation "s1_begin_rr_method2" "s2_begin_sr_method2" "s1_switch_to_rc" "s2_switch_to_rc" "s1_select" "s2_select" "s1_update" "s2_update" "s1_commit" "s2_commit" "s2_select"
permutation "s1_begin_rr_method3_part1" "s1_method3_part2" "s2_begin_sr_method3_part1" "s2_method3_part2" "s1_switch_to_rc" "s2_switch_to_rc" "s1_select" "s2_select" "s1_update" "s2_update" "s1_commit" "s2_commit" "s2_select"
permutation "s1_begin_rr_method4" "s2_begin_sr_method4" "s1_switch_to_rc" "s2_switch_to_rc" "s1_select" "s2_select" "s1_update" "s2_update" "s1_commit" "s2_commit" "s2_select"

# Test all possibilities of switching from RC/ RR to SR.
permutation "s1_begin_rc_method1" "s2_begin_rr_method1" "s1_switch_to_sr" "s2_switch_to_sr" "s1_select" "s2_select" "s1_update" "s2_update" "s1_commit" "s2_rollback" "s2_select"
permutation "s1_begin_rc_method2" "s2_begin_rr_method2" "s1_switch_to_sr" "s2_switch_to_sr" "s1_select" "s2_select" "s1_update" "s2_update" "s1_commit" "s2_rollback" "s2_select"
permutation "s1_begin_rc_method3_part1" "s1_method3_part2" "s2_begin_rr_method3_part1" "s2_method3_part2" "s1_switch_to_sr" "s2_switch_to_sr" "s1_select" "s2_select" "s1_update" "s2_update" "s1_commit" "s2_rollback" "s2_select"
permutation "s1_begin_rc_method4" "s2_begin_rr_method4" "s1_switch_to_sr" "s2_switch_to_sr" "s1_select" "s2_select" "s1_update" "s2_update" "s1_commit" "s2_rollback" "s2_select"

# Ensure that switching isolation level is not allowed after a statement, other than the ones that change transaction characteristics, has been executed.
permutation "s1_begin_rc_method1" "s1_savepoint" "s1_switch_to_rr" "s1_rollback"
permutation "s1_begin_rr_method1" "s1_savepoint" "s1_switch_to_rc" "s1_rollback"
permutation "s1_begin_sr_method1" "s1_savepoint" "s1_switch_to_rc" "s1_rollback"

permutation "s1_begin_rc_method2" "s1_savepoint" "s1_switch_to_rr" "s1_rollback"
permutation "s1_begin_rr_method2" "s1_savepoint" "s1_switch_to_rc" "s1_rollback"
permutation "s1_begin_sr_method2" "s1_savepoint" "s1_switch_to_rc" "s1_rollback"

permutation "s1_begin_rc_method3_part1" "s1_method3_part2" "s1_savepoint" "s1_switch_to_rr" "s1_rollback"
permutation "s1_begin_rr_method3_part1" "s1_method3_part2" "s1_savepoint" "s1_switch_to_rc" "s1_rollback"
permutation "s1_begin_sr_method3_part1" "s1_method3_part2" "s1_savepoint" "s1_switch_to_rc" "s1_rollback"

permutation "s1_begin_rc_method4" "s1_savepoint" "s1_switch_to_rr" "s1_rollback"
permutation "s1_begin_rr_method4" "s1_savepoint" "s1_switch_to_rc" "s1_rollback"
permutation "s1_begin_sr_method4" "s1_savepoint" "s1_switch_to_rc" "s1_rollback"

# Ensure change to DEFERRABLE characteristic is still allowed if no other statement, other than ones that that change transaction characteristics, has executed.
# Also, note that DEFERRABLE doesn't make any sense for any mode other than SERIALIZABLE + READ ONLY, hence testing only for that (ref: https://www.postgresql.org/docs/current/sql-set-transaction.html)

permutation "s2_begin_rc_method1" "s2_switch_to_sr" "s2_read_only" "s2_deferrable" "s1_begin_sr_method1" "s1_update" "s2_select" "s2_commit" "s1_commit"
permutation "s2_begin_rc_method2" "s2_switch_to_sr" "s2_read_only" "s2_deferrable" "s1_begin_sr_method1" "s1_update" "s2_select" "s2_commit" "s1_commit"
permutation "s2_begin_rc_method3_part1" "s2_method3_part2" "s2_switch_to_sr" "s2_read_only" "s2_deferrable" "s1_begin_sr_method1" "s1_update" "s2_select" "s2_commit" "s1_commit"
permutation "s2_begin_rc_method4" "s2_switch_to_sr" "s2_read_only" "s2_deferrable" "s1_begin_sr_method1" "s1_update" "s2_select" "s2_commit" "s1_commit"

permutation "s2_begin_rr_method1" "s2_switch_to_sr" "s2_read_only" "s2_deferrable" "s1_begin_sr_method1" "s1_update" "s2_select" "s2_commit" "s1_commit"
permutation "s2_begin_rr_method2" "s2_switch_to_sr" "s2_read_only" "s2_deferrable" "s1_begin_sr_method1" "s1_update" "s2_select" "s2_commit" "s1_commit"
permutation "s2_begin_rr_method3_part1" "s2_method3_part2" "s2_switch_to_sr" "s2_read_only" "s2_deferrable" "s1_begin_sr_method1" "s1_update" "s2_select" "s2_commit" "s1_commit"
permutation "s2_begin_rr_method4" "s2_switch_to_sr" "s2_read_only" "s2_deferrable" "s1_begin_sr_method1" "s1_update" "s2_select" "s2_commit" "s1_commit"

permutation "s2_begin_sr_method1" "s2_switch_to_sr" "s2_read_only" "s2_deferrable" "s1_begin_sr_method1" "s1_update" "s2_select" "s2_commit" "s1_commit"
permutation "s2_begin_sr_method2" "s2_switch_to_sr" "s2_read_only" "s2_deferrable" "s1_begin_sr_method1" "s1_update" "s2_select" "s2_commit" "s1_commit"
permutation "s2_begin_sr_method3_part1" "s2_method3_part2" "s2_switch_to_sr" "s2_read_only" "s2_deferrable" "s1_begin_sr_method1" "s1_update" "s2_select" "s2_commit" "s1_commit"
permutation "s2_begin_sr_method4" "s2_switch_to_sr" "s2_read_only" "s2_deferrable" "s1_begin_sr_method1" "s1_update" "s2_select" "s2_commit" "s1_commit"

# Ensure change to READ-WRITE characteristic is allowed if no other statement, other than ones that that change transaction characteristics, has executed.

permutation "s2_begin_rc_method1" "s2_switch_to_sr" "s2_read_only" "s2_read_write" "s1_begin_sr_method1" "s1_update" "s2_update" "s1_commit" "s2_commit"
permutation "s2_begin_rc_method2" "s2_switch_to_sr" "s2_read_only" "s2_read_write" "s1_begin_sr_method1" "s1_update" "s2_update" "s1_commit" "s2_commit"
permutation "s2_begin_rc_method3_part1" "s2_method3_part2" "s2_switch_to_sr" "s2_read_only" "s2_read_write" "s1_begin_sr_method1" "s1_update" "s2_update" "s1_commit" "s2_commit"
permutation "s2_begin_rc_method4" "s2_switch_to_sr" "s2_read_only" "s2_read_write" "s1_begin_sr_method1" "s1_update" "s2_update" "s1_commit" "s2_commit"

permutation "s2_begin_rr_method1" "s2_switch_to_sr" "s2_read_only" "s2_read_write" "s1_begin_sr_method1" "s1_update" "s2_update" "s1_commit" "s2_commit"
permutation "s2_begin_rr_method2" "s2_switch_to_sr" "s2_read_only" "s2_read_write" "s1_begin_sr_method1" "s1_update" "s2_update" "s1_commit" "s2_commit"
permutation "s2_begin_rr_method3_part1" "s2_method3_part2" "s2_switch_to_sr" "s2_read_only" "s2_read_write" "s1_begin_sr_method1" "s1_update" "s2_update" "s1_commit" "s2_commit"
permutation "s2_begin_rr_method4" "s2_switch_to_sr" "s2_read_only" "s2_read_write" "s1_begin_sr_method1" "s1_update" "s2_update" "s1_commit" "s2_commit"

permutation "s2_begin_sr_method1" "s2_switch_to_sr" "s2_read_only" "s2_read_write" "s1_begin_sr_method1" "s1_update" "s2_update" "s1_commit" "s2_commit"
permutation "s2_begin_sr_method2" "s2_switch_to_sr" "s2_read_only" "s2_read_write" "s1_begin_sr_method1" "s1_update" "s2_update" "s1_commit" "s2_commit"
permutation "s2_begin_sr_method3_part1" "s2_method3_part2" "s2_switch_to_sr" "s2_read_only" "s2_read_write" "s1_begin_sr_method1" "s1_update" "s2_update" "s1_commit" "s2_commit"
permutation "s2_begin_sr_method4" "s2_switch_to_sr" "s2_read_only" "s2_read_write" "s1_begin_sr_method1" "s1_update" "s2_update" "s1_commit" "s2_commit"
