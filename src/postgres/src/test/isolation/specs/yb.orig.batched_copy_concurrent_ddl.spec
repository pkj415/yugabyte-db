# A DDL must not commit while a batched COPY (rows_per_transaction > 0) is in
# progress: COPY builds its ResultRelInfo (triggers, constraints, index list)
# once, so batches written after the DDL would bypass whatever the DDL added
# (GH #34963).
#
# COPY reads rows 1-400 with b = 1, sleeps, then reads rows 401-800 that
# violate what s2 adds. Rows are padded so that rows 1-400 exceed COPY's 64kB
# read buffer: COPY blocks in fread() until the buffer fills, so without the
# padding it would still be in its first batch during the sleep. With 10 rows
# per transaction, s2 therefore runs after several intermediate commits. The
# sleep must exceed YB_NUM_SECONDS_TO_WAIT_TO_ASSUME_SESSION_BLOCKED, since
# isolationtester polls the running COPY that long before launching s2's DDL.
# s2 must block until COPY completes; validating DDLs then fail against the
# rows COPY inserted.

setup
{
  CREATE TABLE p (id int PRIMARY KEY);
  INSERT INTO p VALUES (1);
  CREATE TABLE t (k int PRIMARY KEY, b int, pad text);
  CREATE FUNCTION reject_big_b() RETURNS trigger LANGUAGE plpgsql AS
    $$ BEGIN IF NEW.b > 100 THEN RAISE EXCEPTION 'rejected b=%', NEW.b; END IF; RETURN NEW; END $$;
}

teardown
{
  DROP TABLE t;
  DROP TABLE p;
  DROP FUNCTION reject_big_b();
}

session s1
step s1_copy_big		{ COPY t FROM PROGRAM $$P=$(printf %0300d 0); seq -f "%g,1,$P" 1 400; sleep 8; seq -f "%g,999,$P" 401 800$$ WITH (FORMAT csv, ROWS_PER_TRANSACTION 10); }
step s1_copy_null		{ COPY t FROM PROGRAM $$P=$(printf %0300d 0); seq -f "%g,1,$P" 1 400; sleep 8; seq -f "%g,,$P" 401 800$$ WITH (FORMAT csv, ROWS_PER_TRANSACTION 10); }
step s1_sync			{ SELECT 1; }

session s2
# Wait until COPY has committed some batches so that s2 always runs between
# batches rather than before COPY's first batch.
step s2_wait			{ DO $$ BEGIN WHILE (SELECT count(*) FROM t) < 100 LOOP PERFORM pg_sleep(0.1); COMMIT; END LOOP; END $$; }
step s2_create_trigger	{ CREATE TRIGGER reject_big_b BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION reject_big_b(); }
step s2_add_check		{ ALTER TABLE t ADD CONSTRAINT b_small CHECK (b < 100); }
step s2_set_not_null	{ ALTER TABLE t ALTER COLUMN b SET NOT NULL; }
step s2_add_fk			{ ALTER TABLE t ADD CONSTRAINT b_fk FOREIGN KEY (b) REFERENCES p (id); }
step s2_create_index	{ CREATE INDEX NONCONCURRENTLY t_b_idx ON t (b); }
step s2_counts			{ SELECT b, count(*) FROM t GROUP BY b ORDER BY b; }
step s2_constraints		{ SELECT conname, convalidated FROM pg_constraint WHERE conrelid = 't'::regclass AND contype IN ('c', 'f') ORDER BY conname; }
step s2_not_null		{ SELECT attnotnull FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'b'; }
step s2_triggers		{ SELECT tgname FROM pg_trigger WHERE tgrelid = 't'::regclass AND NOT tgisinternal; }
step s2_index_check		{ SELECT yb_index_check('t_b_idx'::regclass); }

# Rows 401-800 (b = 999) must not be inserted after the BEFORE INSERT trigger
# rejecting them has committed.
permutation s1_copy_big(*) s2_wait s2_create_trigger s1_sync s2_triggers s2_counts

# A validated CHECK constraint must hold for every row.
permutation s1_copy_big(*) s2_wait s2_add_check s1_sync s2_constraints s2_counts

# A NOT NULL column must not contain NULLs.
permutation s1_copy_null(*) s2_wait s2_set_not_null s1_sync s2_not_null s2_counts

# A validated foreign key must not have orphan rows.
permutation s1_copy_big(*) s2_wait s2_add_fk s1_sync s2_constraints s2_counts

# An index must contain every row of its table.
permutation s1_copy_big(*) s2_wait s2_create_index s1_sync s2_index_check s2_counts
