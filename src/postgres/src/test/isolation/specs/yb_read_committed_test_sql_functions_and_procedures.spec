# Tests to ensure that Read Committed semantics apply to SQL functions and procedures
# session 1 is the session of importance since that is used to test conflict handling retry logic.

setup
{
 CREATE TABLE test (k int primary key, v int);
 INSERT INTO test VALUES (1, 1);
 INSERT INTO test VALUES (2, 1);
 CREATE PROCEDURE update_k1_in_s1() AS $$
  UPDATE test SET v=v+2 WHERE k=1;
  $$ LANGUAGE SQL;

 CREATE PROCEDURE update_k1_k2_in_s1() AS $$
  UPDATE test SET v=v+2 WHERE k=1;
  UPDATE test SET v=v+2 WHERE k=2;
  $$ LANGUAGE SQL;

 CREATE PROCEDURE update_all_rows_in_s1() AS $$
  UPDATE test SET v=v+2 WHERE k>=1;
  $$ LANGUAGE SQL;

 CREATE FUNCTION select_all_rows_for_update_s1() RETURNS SETOF test AS $$
  SELECT * FROM test FOR UPDATE;
  $$ LANGUAGE SQL;

 CREATE PROCEDURE insert_k1_in_s1() AS $$
  INSERT INTO test VALUES (1, 1);
  $$ LANGUAGE SQL;

 CREATE PROCEDURE update_k2_inner_func_s1() AS $$
  UPDATE test SET v=v+2 WHERE k=2;
  $$ LANGUAGE SQL;

 CREATE PROCEDURE update_k1_k2_outer_func_s1() AS $$
  UPDATE test SET v=v+2 WHERE k=1;
  CALL update_k2_inner_func_s1();
  $$ LANGUAGE SQL;

 CREATE PROCEDURE update_k1_k2_with_non_txnal_side_effect_in_s1() AS $$
  PREPARE dummy_stmt AS INSERT INTO test VALUES(1, 1);
  UPDATE test SET v=v+2 WHERE k=1;
  UPDATE test SET v=v+2 WHERE k=2;
  $$ LANGUAGE SQL;

 CREATE PROCEDURE update_k1_in_s2() AS $$
  UPDATE test SET v=v*5 WHERE k=1;
  $$ LANGUAGE SQL;

 CREATE PROCEDURE update_k2_in_s3() AS $$
  UPDATE test SET v=v*4 WHERE k=2;
  $$ LANGUAGE SQL;
}

teardown
{
 DROP PROCEDURE update_k1_in_s1;
 DROP PROCEDURE update_k1_k2_in_s1;
 DROP PROCEDURE update_all_rows_in_s1;
 DROP FUNCTION select_all_rows_for_update_s1;
 DROP PROCEDURE insert_k1_in_s1;
 DROP PROCEDURE update_k1_k2_outer_func_s1;
 DROP PROCEDURE update_k2_inner_func_s1;
 DROP PROCEDURE update_k1_k2_with_non_txnal_side_effect_in_s1;
 DROP PROCEDURE update_k1_in_s2;
 DROP PROCEDURE update_k2_in_s3;
 DROP TABLE test;
}

session "s1"
setup	{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step "update_k1_in_s1" { CALL update_k1_in_s1(); }
step "update_k1_k2_in_s1" { CALL update_k1_k2_in_s1(); }
step "update_all_rows_in_s1" { CALL update_all_rows_in_s1(); }
step "lazily_select_all_rows_for_update_s1" { SELECT * FROM (SELECT select_all_rows_for_update_s1()) AS x; }
step "insert_k1_in_s1" { CALL insert_k1_in_s1(); }
step "select_s1" { SELECT * FROM test; }
step "update_k1_k2_outer_func_s1" { CALL update_k1_k2_outer_func_s1(); }
step "update_k1_k2_with_non_txnal_side_effect_in_s1" { CALL update_k1_k2_with_non_txnal_side_effect_in_s1(); }
step "commit_s1" { COMMIT; }
step "rollback_s1" { ROLLBACK; }

session "s2"
setup { BEGIN ISOLATION LEVEL READ COMMITTED; }
step "update_k1_in_s2" { CALL update_k1_in_s2(); }
step "commit_s2" { COMMIT; }

session "s3"
setup	{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step "update_k2_in_s3" { CALL update_k2_in_s3(); }
step "commit_s3" { COMMIT; }

# Test basic conflict handling in a procedure
permutation "update_k1_in_s2" "update_k1_in_s1" "commit_s2" "select_s1" "commit_s1" "commit_s3"

# Test conflicts in different statements of the procedure
permutation "update_k1_in_s2" "update_k2_in_s3" "update_k1_k2_in_s1" "commit_s2" "commit_s3" "select_s1" "commit_s1"

# Test multiple conflicts in same statement of the procedure
permutation "update_k1_in_s2" "update_k2_in_s3" "update_all_rows_in_s1" "commit_s2" "commit_s3" "select_s1" "commit_s1"

# Test where rows might be lazily evaluated (i.e., SELECTs on functions which have a SELECT as the
# last statement). Lazy evaluation is not supported by READ COMMITTED isolation, instead all rows
# are read at once and stored in a tuple store. So, this should work similar to a case without
# possibility of lazy evaluation.
permutation "update_k2_in_s3" "lazily_select_all_rows_for_update_s1" "commit_s3" "commit_s2" "commit_s1"

# Test to ensure that errors other than conflicts and read restarts are not retried
permutation "insert_k1_in_s1" "rollback_s1" "commit_s2" "commit_s3"

# Test retries in nested functions
permutation "update_k1_in_s2" "update_k2_in_s3" "update_k1_k2_outer_func_s1" "commit_s2" "commit_s3" "select_s1" "commit_s1"

# Test to ensure that non-transactional side-effects occur only once i.e., the whole function is not
# retried when a conflict occurs, only the statement that faces a conflict is retried
permutation "update_k1_in_s2" "update_k2_in_s3" "update_k1_k2_with_non_txnal_side_effect_in_s1" "commit_s2" "commit_s3" "select_s1" "commit_s1"
