--
-- Tests to ensure YSQL buffering (i.e., logic in pg_operation_buffer.cc and
-- related files) doesn't break any semantics of PLPGSQL.
--
-- *****************************************************************************
-- * Exception handling in PLPGSQL
-- *****************************************************************************
--
-- PLPGSQL exception blocks allow a user to execute a block of statements such
-- that if an error occurs, the changes to the database are undone/ reverted and
-- user-specified error-handling is invoked.
--
-- The changes to the database are undone as follows - an internal savepoint is
-- registered before any code in the exception block is executed. If any error
-- occurs, it is caught and the savepoint is rolled back and released. This
-- helps revert any modifications to the database.
--
-- However, there are some statements which don't modify the database, but have
-- other side-effects. These are not undone even if an exception occurs. A
-- simple example of this is the "return next;" statement in plpgsql. Once data
-- is sent to the user it can't be undone.
-- *****************************************************************************
--
-- *****************************************************************************
-- * YSQL Buffering
-- *****************************************************************************
-- YSQL buffers operations to tserver (writes specifically) unless it hits some
-- condition that forces it to flush the buffer and wait for the response. Some
-- conditions that force waiting for a buffer response are - completion of a txn,
-- completion of an exception handling block in plpgsql, a read operation, or a
-- write to a key which already has a buffered write.
--
-- With buffering, execution can move on to later statements unless a flush and
-- response wait is required based on the conditions above. For example - with
-- autocommit mode off, writes for a statement (like INSERT/UPDATE) are not
-- flushed until required. Instead, they are buffered. This is okay because,
-- even before flushing, we would know the number of inserts/updates to be done
-- and return that number to the user client (as "INSERT x"). If an error occurs
-- in the rpc, it will anyway be caught in some later flush, but before the txn
-- commits.
--
-- Allowing execution to move on to later statements without waiting for the
-- actual work to be done on the tablet servers helps improve performance by
-- buffering and reduce latency.
-- *****************************************************************************
--
--
-- As seen in gh issue #12184, incorrect behaviour is observed with YSQL
-- buffering when an exception that occurs due to some statement's rpcs is seen
-- after a later statement which has non-reversible side-effect(s) has also been
-- executed. Buffered operations should be flushed and waited for before
-- executing any statements with non-reversible side effects in the same
-- transaction. The following tests ensure this for the various cases.
--
-- 1(a) PL/pgsql: ensure statements with non-reversible side effects (i.e., non
--      transactional work) are not executed if an ealier statement caused an
--      exception.

create table t(k serial primary key, v varchar(100) not null);
create unique index t_v_unq on t(v);
insert into t(v) values ('dog'), ('cat'), ('frog');

create or replace function f(new_v in text)
  returns table(z text)
  language plpgsql
as $body$
begin
  begin
    z := 'return next was executed after insert, this was not expected';
    insert into t(v) values (new_v);
    return next;
  exception
    when unique_violation then
      z := 'unique_violation'; return next;
    when others then
      raise;
  end;
end;
$body$;

select f('dog');

-- 1(b) PL/pgsql: same case as 1(a) but the statement that does non-reversible
--      side effects (i.e., non-transactional work) is in a nested function.

create or replace function f_outer()
  returns table(z text)
  language plpgsql
as $body$
begin
  begin
    insert into t(v) select f('dog');
  exception
    when unique_violation then
      z := 'unique_violation'; return next;
    when others then
      raise;
  end;
end;
$body$;

select f_outer();
select * from t;

-- 2. SQL functions: ensure statements with non-reversible side effects (i.e.,
--    non transactional work) are not executed if an ealier statement caused an
--    exception.

prepare dummy_query as select * from t;

create or replace function f(new_v in text)
  returns table(z text)
  language sql
as $body$
  insert into t(v) values ('dog');
  deallocate dummy_query;
  select v from t;
$body$;

select f('dog');
execute dummy_query; -- this should find the prepared statement and run fine
