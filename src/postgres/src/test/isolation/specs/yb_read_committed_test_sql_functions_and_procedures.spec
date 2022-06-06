# Tests to ensure that Read Committed semantics apply to SQL functions and procedures
# session 1 is the session of importance since that is used to test conflict handling retry logic.

setup
{
 create table test (k int primary key, v int);
 insert into test values (1, 1);
 insert into test values (2, 1);
 create procedure update_k1_in_s1() AS $$
  update test set v=v+2 where k=1;
  $$ LANGUAGE SQL;

 create procedure update_k1_k2_in_s1() AS $$
  update test set v=v+2 where k=1;
  update test set v=v+2 where k=2;
  $$ LANGUAGE SQL;

 create procedure update_all_rows_in_s1() AS $$
  update test set v=v+2 where k>=1;
  $$ LANGUAGE SQL;

 create function select_all_rows_for_update_s1() returns setof test AS $$
  select * from test for update;
  $$ LANGUAGE SQL;

 create procedure insert_k1_in_s1() AS $$
  insert into test values (1, 1);
  $$ LANGUAGE SQL;

 create procedure update_k2_inner_func_s1() AS $$
  update test set v=v+2 where k=2;
  $$ LANGUAGE SQL;

 create procedure update_k1_k2_outer_func_s1() AS $$
  update test set v=v+2 where k=1;
  call update_k2_inner_func_s1();
  $$ LANGUAGE SQL;

 create procedure update_k1_k2_with_non_txnal_side_effect_in_s1() AS $$
  PREPARE dummy_stmt AS INSERT INTO test VALUES(1, 1);
  update test set v=v+2 where k=1;
  update test set v=v+2 where k=2;
  $$ LANGUAGE SQL;

 create procedure update_k1_in_s2() AS $$
  update test set v=v*5 where k=1;
  $$ LANGUAGE SQL;

 create procedure update_k2_in_s3() AS $$
  update test set v=v*4 where k=2;
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
step "lazily_select_all_rows_for_update_s1" { SELECT * from (SELECT select_all_rows_for_update_s1()) as x; }
step "insert_k1_in_s1" { CALL insert_k1_in_s1(); }
step "select_s1" { select * from test; }
step "update_k1_k2_outer_func_s1" { call update_k1_k2_outer_func_s1(); }
step "update_k1_k2_with_non_txnal_side_effect_in_s1" { call update_k1_k2_with_non_txnal_side_effect_in_s1(); }
step "commit_s1" { commit; }
step "rollback_s1" { rollback; }

session "s2"
setup { BEGIN ISOLATION LEVEL READ COMMITTED; }
step "update_k1_in_s2" { CALL update_k1_in_s2(); }
step "commit_s2" { commit; }

session "s3"
setup	{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step "update_k2_in_s3" { CALL update_k2_in_s3(); }
step "commit_s3" { commit; }

# Test basic conflict handling in a function
permutation "update_k1_in_s2" "update_k1_in_s1" "commit_s2" "select_s1" "commit_s1" "commit_s3"

# Test conflicts in different statements of the function
permutation "update_k1_in_s2" "update_k2_in_s3" "update_k1_k2_in_s1" "commit_s2" "commit_s3" "select_s1" "commit_s1"

# Test multiple conflicts in same statement of the function
permutation "update_k1_in_s2" "update_k2_in_s3" "update_all_rows_in_s1" "commit_s2" "commit_s3" "select_s1" "commit_s1"

# Test to ensure that retries can't work on a statement that is being lazily evaluated.
permutation "update_k2_in_s3" "lazily_select_all_rows_for_update_s1" "rollback_s1" "commit_s3" "commit_s2"

# Test to ensure that errors other than conflicts and read restarts are not retried.
permutation "insert_k1_in_s1" "rollback_s1" "commit_s2" "commit_s3"

# Test retries in nested functions
permutation "update_k1_in_s2" "update_k2_in_s3" "update_k1_k2_outer_func_s1" "commit_s2" "commit_s3" "select_s1" "commit_s1"

# Test to ensure that non-transactional side-effects occur only once i.e., the whole function is not
# retried when a conflict occurs, only the statement that faces a conflict is retried.
permutation "update_k1_in_s2" "update_k2_in_s3" "update_k1_k2_with_non_txnal_side_effect_in_s1" "commit_s2" "commit_s3" "select_s1" "commit_s1"
