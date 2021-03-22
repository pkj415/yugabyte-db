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
package org.yb.cql;

import java.util.*;

import org.junit.BeforeClass;
import org.junit.Test;

import org.yb.minicluster.BaseMiniClusterTest;

import static org.yb.AssertionWrappers.assertTrue;

import org.yb.YBTestRunner;

import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Write {
  public boolean predicate;
  public int ref_write_index;
  public List<String> matching_cols;
  public List<List<String>> differing_cols;
  public List<String> row;
  public boolean should_fail;
  public boolean use_insert_stmt; // Use UPDATE if false.

  public Write(boolean predicate,
        int ref_write_index,
        List<String> matching_cols,
        List<List<String>> differing_cols,
        boolean should_fail,
        boolean use_insert_stmt) throws Exception {
    this.predicate = predicate;
    this.ref_write_index = ref_write_index;
    this.matching_cols = matching_cols;
    this.differing_cols = differing_cols;
    this.should_fail = should_fail;
    this.use_insert_stmt = use_insert_stmt;
  }

  public void setRow(List<String> row) {
    this.row = new ArrayList<String>(row);
  }
}

@RunWith(value=YBTestRunner.class)
public class TestPartialIndex extends TestIndex {

  // TODO(Piyush):
  //
  //   1. I have seen all tests in TestIndex.java. Again, check if any of the tests there make
  //      sense here (i.e., those which surely need to be tested for partial indexes as well).
  //   2. Test clustering indexes with predicates
  //   3. For tests that require inserting a few rows before assertions, try to insert rows
  //      via INSERT and UPDATE statements both i.e., all tests will then have 2 variations, those
  //      that use INSERT/those that use UPDATE to insert a row.
  //   4. Run tests in batch mode (OR) in a transaction block.
  //   5. Ensure each flag is true atleast once.

  private static final Logger LOG = LoggerFactory.getLogger(TestPartialIndex.class);

  public String test_table_name = "test_partial_index";
  private int pk_col_cnt; // Number of pk cols in table.
  private int col_cnt; // Number of cols in table.
  private List<String> col_names; // pk cols first
  // some user-provided rows that satisfy the predicate
  private List<List<String>> pred_true_rows;
  // some user-provided rows that don't satisfy the predicate
  private List<List<String>> pred_false_rows;
  // rows from pred_true_rows inserted into main table. Used for assertions.
  private List<Integer> already_inserted_true_rows;
  // rows from pred_false_rows inserted into main table. Used for assertions.
  private List<Integer> already_inserted_false_rows;

  // For a given choice of table, index, its indexed cols, its covering cols, and its predicate,
  // the below flags are set to help decide if some test cases are possible for partial indexes
  // or not.
  //
  // Name convention: [pk => primary key cols, i => indexed cols, c => covering cols]
  // E.g.:
  // 1. same_pk_i_c_both_pred_true_false_rows means there exists two rows with same pk,
  //    indexed cols, and covering cols but one with pred=true and another with pred=false.
  //    For instance, if there is an indexed col v1 and index has predicate v1=null, then this
  //    flag should be set to false.
  // 2. same_pk_c_diff_i_mulitple_pred_false_rows means there exists more than one row with
  //    same pk, covering cols but different indexed cols such that all have pred=false.

  private boolean same_pk_i_c_both_pred_true_false_rows;
  private boolean same_pk_c_diff_i_both_pred_true_false_rows;
  private boolean same_pk_i_diff_c_both_pred_true_false_rows;
  private boolean same_pk_i_c_multiple_pred_false_rows;
  private boolean same_pk_c_diff_i_mulitple_pred_false_rows;
  private boolean same_pk_i_diff_c_mulitple_pred_false_rows;
  private boolean same_pk_i_c_multiple_pred_true_rows;
  private boolean same_pk_c_diff_i_mulitple_pred_true_rows;
  private boolean same_pk_i_diff_c_mulitple_pred_true_rows;

  // For a given choice of table, index, its indexed cols, its covering cols, and its predicate,
  // the below flags are set to help decide if some test cases are possible for unique partial
  // indexes or not.
  private boolean same_i_diff_pk_mulitple_pred_true_rows;
  private boolean same_i_diff_pk_both_pred_true_false_rows;

  @BeforeClass
  public static void SetUpBeforeClass() throws Exception {
    BaseMiniClusterTest.tserverArgs.add("--allow_index_table_read_write");
    BaseMiniClusterTest.tserverArgs.add(
        "--index_backfill_upperbound_for_user_enforced_txn_duration_ms=1000");
    BaseMiniClusterTest.tserverArgs.add(
        "--index_backfill_wait_for_old_txns_ms=100");

    // Enable partial indexes.
    BaseMiniClusterTest.tserverArgs.add("--cql_raise_index_where_clause_error=false");
    BaseCQLTest.setUpBeforeClass();
  }

  private int idxOfCol(String colName) {
    for (int i=0; i<this.col_names.size(); i++) {
      if (this.col_names.get(i).equals(colName))
        return i;
    }
    assertTrue(false); // Something wrong.
    return -1; // To avoid compiler error.
  }

  /**
   * Get projection of row for certain cols.
   *
   * @param row list of col values in row.
   * @param proj_cols list of col names to be projected.
   */
  private List<String> createRowProjection(List<String> row, List<String> proj_cols) {
    List<String> proj_row = new ArrayList<String>();
    assertTrue(row.size() == this.col_cnt); // Only allow projection of full rows.
    for (int i=0; i<proj_cols.size(); i++) {
      proj_row.add(row.get(idxOfCol(proj_cols.get(i))));
    }
    return proj_row;
  }

  /**
   * Check if two rows satisfy the following conditions -
   *   1. They have the same value for all columns in match_cols.
   *   2. They differ on at least 1 column for each column group in differ_cols_list.
   *
   * E.g.:
   *   table temp(h1 int, r1 int, v1 int, v2 int, v3 int, v4 int, primary key (h1,r1));
   *   index on temp(v1,v2) include (v3,v4);
   *
   * Tow check if two rows share the same pk but differ in set of indexed cols and differ in set of
   * covering cols:
   *   match_rows(row1, row2, [h1,r1], [[v1,v2], [v3,v4]])
   *
   * @param row1
   * @param row2
   * @param match_cols list of column names to match.
   * @param differ_cols_list list of column groups required to have different values.
   */
  private boolean match_rows(List<String> row1, List<String> row2, List<String> match_cols,
                             List<List<String>> differ_cols_list) {
    Set<String> differing_cols = new HashSet<String>();
    assertTrue(row1.size() == this.col_cnt);
    assertTrue(row2.size() == this.col_cnt);

    for (int i=0; i<this.col_cnt; i++) {
      if (match_cols.contains(col_names.get(i)))
        if (!row1.get(i).equals(row2.get(i))) return false;

      if (!row1.get(i).equals(row2.get(i)))
        differing_cols.add(col_names.get(i));
    }

    for (int i=0; i<differ_cols_list.size(); i++) {
      List<String> differ_cols = differ_cols_list.get(i);
      boolean found_differ_col = false;
      for (int j=0; j<differ_cols.size(); j++) {
        if (differing_cols.contains(differ_cols.get(j))) {
          found_differ_col = true;
          break;
        }
      }
      if (!found_differ_col) return false;
    }
    return true;
  }

  private void markPredTrueRowUnused(List<String> row) {
    int j;
    for (j=0; j<already_inserted_true_rows.size(); j++) {
      if (pred_true_rows.get(already_inserted_true_rows.get(j)).equals(row))
        break;
    }
    assert(j<already_inserted_true_rows.size());
    already_inserted_true_rows.remove(j);
  }

  private void markOtherPredTrueRowUnused(List<String> row) {
    List<Integer> already_inserted_true_rows_temp = new ArrayList<Integer>();
    for (int j=0; j<already_inserted_true_rows.size(); j++) {
      List<String> other_row = pred_true_rows.get(already_inserted_true_rows.get(j));
      if (getPk(other_row).equals(getPk(row)) && !row.equals(other_row))
        continue;
      already_inserted_true_rows_temp.add(j);
    }
    this.already_inserted_true_rows = already_inserted_true_rows_temp;
  }

  /* Pick any row from user-provided pred=true rows that hasn't been picked yet */
  private List<String> getUnusedPredTrueRow() {
    for (int i=0; i < pred_true_rows.size(); i++) {
      if (!already_inserted_true_rows.contains(i)) {
        already_inserted_true_rows.add(i);
        markOtherPredTrueRowUnused(pred_true_rows.get(i));
        return pred_true_rows.get(i);
      }
    }
    assertTrue(false); // Add enough rows so that the test works.
    return new ArrayList<String>();
  }

  private List<String> getUnusedPredFalseRow() {
    for (int i=0; i < pred_false_rows.size(); i++) {
      if (!already_inserted_false_rows.contains(i)) {
        already_inserted_false_rows.add(i);
        markOtherPredTrueRowUnused(pred_false_rows.get(i));
        return pred_false_rows.get(i);
      }
    }
    assertTrue(false); // Add enough rows so that the test works.
    return new ArrayList<String>();
  }

  /* Pick any row from user-provided pred=true rows that hasn't been picked yet such that it
   * matches (and differs) with the given row on the specified columns (and column groups).
   */
  private List<String> getUnusedPredTrueRow(List<String> row, List<String> match_cols,
                                            List<List<String>> differ_cols) {
    for (int i=0; i<pred_true_rows.size(); i++) {
      if (match_rows(pred_true_rows.get(i), row, match_cols, differ_cols)) {
        if (!already_inserted_true_rows.contains(i)) {
          already_inserted_true_rows.add(i);
          markOtherPredTrueRowUnused(pred_true_rows.get(i));
          return pred_true_rows.get(i);
        }
      }
    }
    assertTrue(false); // Add enough rows so that the test works.
    return new ArrayList<String>();
  }

  private List<String> getUnusedPredFalseRow(List<String> row, List<String> match_cols,
                                             List<List<String>> differ_cols) {
    for (int i=0; i<pred_false_rows.size(); i++) {
      if (match_rows(pred_false_rows.get(i), row, match_cols, differ_cols)) {
        if (!already_inserted_false_rows.contains(i)) {
          already_inserted_false_rows.add(i);
          markOtherPredTrueRowUnused(pred_false_rows.get(i));
          return pred_false_rows.get(i);
        }
      }
    }
    assertTrue(false); // Add enough rows so that the test works.
    return new ArrayList<String>();
  }

  private List<String> getPk(List<String> row) {
    return row.subList(0, this.pk_col_cnt);
  }

  private void assertIndex(List<String> cols_in_index) {
    List<String> mangled_cols_in_index = new ArrayList<String>();
    for (String col : cols_in_index) {
      mangled_cols_in_index.add("\"C$_" + col + "\"");
    }

    Set<String> idx_tuples = queryTable("idx", String.join(", ", mangled_cols_in_index));

    for (int i=0; i<already_inserted_true_rows.size(); i++) {
      String row = String.join(", ",
        createRowProjection(pred_true_rows.get(already_inserted_true_rows.get(i)), cols_in_index));
      assertTrue(idx_tuples.contains("Row[" + row + "]"));
    }
    assertTrue(idx_tuples.size() == already_inserted_true_rows.size());
  }

  private void resetTableAndIndex() {
    session.execute(String.format("truncate table %s", test_table_name));
    already_inserted_true_rows.clear();
    already_inserted_false_rows.clear();
  }

  /*
   * Helper function to test scenarios of writes performed in sequence. The writes can be
   * done either via INSERT or UPDATE.
   *
   * @param cols_in_index names of column in index table.
   * @param writes list of Write objects. Each Write object has info on the type
   *        of row to be used - its predicate value, cols tha have to match
   *        with a reference row, groups of cols that have to differ with the
   *        ref row, if it should fail, and whether to use INSERT/UPDATE.
   */
  void performWrites(List<String> cols_in_index, List<Write> writes) {
    for (int i=0; i<writes.size(); i++) {
      Write write = writes.get(i);
      List<String> row;
      if (write.ref_write_index != -1) {
        assert(write.ref_write_index < i); // Can't reference anything before this write.
        if (write.predicate) {
          row = getUnusedPredTrueRow(
            writes.get(write.ref_write_index).row,
            write.matching_cols,
            write.differing_cols);
        } else {
          row = getUnusedPredFalseRow(
            writes.get(write.ref_write_index).row,
            write.matching_cols,
            write.differing_cols);
        }
      } else {
        row = write.predicate ? getUnusedPredTrueRow() : getUnusedPredFalseRow();
      }
      write.setRow(row);

      String stmt = String.format("INSERT INTO %s(%s) VALUES (%s)",
                                    test_table_name,
                                    String.join(",", col_names),
                                    String.join(",", row));
      if (write.should_fail) {
        runInvalidStmt(stmt);
        break;
      } else {
        session.execute(stmt);
      }

      assertIndex(cols_in_index);
    }

    // Drop the index, truncate the table.
    resetTableAndIndex();


    // TODO(Piyush): Integrate code to use UPDATE statement
    // List<String> row = getUnusedPredTrueRow();
    // List<String> where_clause_elems = new ArrayList<String>();
    // for (int i=0; i<this.pk_col_cnt; i++) {
    //   where_clause_elems.add(col_names.get(i) + "=" + row.get(i));
    // }

    // String set_clause = "";
    // for (int i=this.pk_col_cnt; i<this.col_cnt; i++) {
    //   set_clause += col_names.get(i) + "=" + row.get(i);
    // }

    // session.execute(
    //   String.format("UPDATE %s SET %s WHERE %s",
    //     test_table_name,
    //     set_clause,
    //     String.join(" and ", where_clause_elems)));

    // // If all non-pk non-static cols were NULL, then the UPDATE actually
    // // wouldn't result in an insert. In that case un mark the use true pred row.
    // boolean all_null = true;
    // for (int i=this.pk_col_cnt; i<this.col_cnt; i++) {
    //   if (!row.get(i).equalsIgnoreCase("null")) {
    //     all_null = false;
    //     break;
    //   }
    // }
    // if (all_null)
    //   markPredTrueRowUnused(row);
  }

  /**
   * The most imporant internal method to test partial indexes for a specific choice of predicate,
   * indexed columns and covering columns. This method exhaustively tests INSERT/UPDATE (see the
   * matrices in the function for each detailed case) for the combination of predicate, indexed cols
   * and covering cols provided.
   * 
   * Note that we require the caller to specify required object variables (like some pred=true/false
   * rows, some properties of the specific combination i.e., the same_pk* flags, etc) before calling
   * this because it is a hard problem to generate rows that satisfy predicates and decipher
   * properties of a combination. Instead it is easier for a human to give all this information.
   *
   * The following test cases are included in this -
   *
   *  1. Insert (semantically; not talking about INSERT statement i.e., write a row with pk
   *             that doesn't exist in table)
   *  2. Update (semantically; not talking about UPDATE statement i.e., write a row with pk
   *              that already exists in table)
   *
   * Both of the above involve writes which can be performed by using either -
   *      a) INSERT statement.
   *      b) UPDATE statement.
   *
   * So each test case with n writes is internally executed 2^n times with different combinations
   * of INSERT/UPDATE statements.
   *
   * @param predicate
   * @param indexed_cols the columns which are to be indexed.
   * @param covering_cols the columns to be covered.
   * @param strongConsistency
   * @param is_unique test on a unique index
   */
  public void testPartialIndexWritesInternal(
      String predicate, List<String> indexed_cols, List<String> covering_cols,
      boolean strongConsistency, boolean is_unique) throws Exception {

    String include_clause = "";
    if (covering_cols.size() > 0) {
      include_clause = String.format("INCLUDE (%s)", covering_cols);
    }

    List<String> cols_in_index = new ArrayList<String>();
    cols_in_index.addAll(indexed_cols);
    cols_in_index.remove(covering_cols); // Remove duplicates.
    cols_in_index.addAll(covering_cols);
    cols_in_index.remove(getPk(this.col_names)); // Remove duplicates.
    cols_in_index.addAll(getPk(this.col_names));

    // Create index.
    createIndex(
      String.format("CREATE %s INDEX idx ON %s(%s) %s WHERE %s",
        is_unique ? "UNIQUE" : "", test_table_name, String.join(", ", indexed_cols),
        include_clause, predicate),
      strongConsistency);

    // Insert (No existing row with same pk)
    // --------------------------------------
    //       ______________________________________________
    //      |New row's pred|                               |
    //      |--------------+-------------------------------|
    //      |  pred=true   |   Insert into Partial Index   |
    //      |--------------+-------------------------------|
    //      |  pred=false  |    No-op                      |
    //      +--------------+-------------------------------+

    // Case with pred=true.
    this.performWrites(cols_in_index,
      Arrays.asList(
        new Write(
          true, /* predicate */
          -1, /* ref_write_index */
          new ArrayList<String>(), /* matching_cols */
          new ArrayList<List<String>>(), /* differing_cols_list */
          false, /* should_fail */
          true /* use_insert_stmt */)
      )
    );

    // Case with pred=false.
    this.performWrites(cols_in_index,
      Arrays.asList(
        new Write(
          false, /* predicate */
          -1, /* ref_write_index */
          new ArrayList<String>(), /* matching_cols */
          new ArrayList<List<String>>(), /* differing_cols_list */
          false, /* should_fail */
          true /* use_insert_stmt */)
      )
    );

    // Insert - extra cases applicable only to unique partial indexes.
    // ---------------------------------------------------------------
    //      _______________________________________________________________________________________
    //     |               |  Table has row with same indexed  |  Table has no row with same       |
    //     |New row's pred |  col values and pred=true         |  indexed col values and pred=true |
    //     |---------------|-----------------------------------|-----------------------------------|
    //     | pred=true     |   FAIL OP                         |     Insert into UPI               |
    //     |---------------+-----------------------------------+-----------------------------------|
    //     | pred=false    |   No-op                           |     No-op                         |
    //     ----------------------------------------------------+------------------------------------

    // pred=true, Exists a row with same index col values and pred=true
    if (is_unique && this.same_i_diff_pk_mulitple_pred_true_rows) {
      this.performWrites(cols_in_index,
        Arrays.asList(
          new Write(
            true, /* predicate */
            -1, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            true, /* predicate */
            0, /* ref_write_index */
            indexed_cols, /* matching_cols */
            Arrays.asList(new ArrayList<String>(getPk(col_names))), /* differing_cols_list */
            true, /* should_fail */
            true /* use_insert_stmt */)
        )
      );
    }

    // pred=false, Exists a row with same index col values and pred=true
    if (is_unique && this.same_i_diff_pk_both_pred_true_false_rows) {
      this.performWrites(cols_in_index,
        Arrays.asList(
          new Write(
            true, /* predicate */
            -1, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            false, /* predicate */
            0, /* ref_write_index */
            indexed_cols, /* matching_cols */
            Arrays.asList(new ArrayList<String>(getPk(col_names))), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */)
        )
      );
    }

    // Update (There is an existing row with same pk)
    // ----------------------------------------------
    //
    //                                      _________________________________________________
    //                                      | Table has same pk row | Table has same pk row |
    //                                      | with pred=false       | with pred=true        |
    //     |--------------------------------|-----------------------|-----------------------|
    //     | pred=true (Same I && C cols)   | Insert into PI        | No-op                 |
    //     |--------------------------------+-----------------------+-----------------------|
    //     | pred=true (Diff I || C cols)   | Insert into PI        | Update PI             |
    //     |--------------------------------+-----------------------+-----------------------|
    //     | pred=false (Same I && C cols)  | No-op                 | Delete from PI        |
    //     |--------------------------------+-----------------------+-----------------------|
    //     | pred=false (Diff I || C cols)  | No-op                 | Delete from PI        |
    //     |--------------------------------------------------------------------------------|

    // pred=true (Same I && C cols), Same pk row exists with pred=false.
    if (this.same_pk_i_c_both_pred_true_false_rows) {
      List<String> matching_cols = new ArrayList<String>(getPk(col_names));
      matching_cols.addAll(indexed_cols);
      matching_cols.addAll(covering_cols);
      this.performWrites(cols_in_index,
        Arrays.asList(
          new Write(
            false, /* predicate */
            -1, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            true, /* predicate */
            0, /* ref_write_index */
            matching_cols, /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */)
        )
      );
    }

    // pred=true (Diff I || C cols), Same pk row exists with pred=false.
    //   1. Diff I col
    //   2. Diff C col

    // Case 1
    if (this.same_pk_c_diff_i_both_pred_true_false_rows) {
      List<String> matching_cols = new ArrayList<String>(getPk(col_names));
      matching_cols.addAll(covering_cols);

      this.performWrites(cols_in_index,
        Arrays.asList(
          new Write(
            false, /* predicate */
            -1, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            true, /* predicate */
            0, /* ref_write_index */
            matching_cols, /* matching_cols */
            Arrays.asList(new ArrayList<String>(indexed_cols)), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */)
        )
      );
    }

    // Case 2
    if (!covering_cols.isEmpty() && this.same_pk_i_diff_c_both_pred_true_false_rows) {
      List<String> matching_cols = new ArrayList<String>(getPk(col_names));
      matching_cols.addAll(indexed_cols);

      this.performWrites(cols_in_index,
        Arrays.asList(
          new Write(
            false, /* predicate */
            -1, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            true, /* predicate */
            0, /* ref_write_index */
            matching_cols, /* matching_cols */
            Arrays.asList(new ArrayList<String>(covering_cols)), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */)
        )
      );
    }

    // pred=false (Same I && C cols), Same pk row exists with pred=false.
    if (this.same_pk_i_c_multiple_pred_false_rows) {
      // Without if check it would amount to adding the exact same row. We don't have to test that.
      List<String> matching_cols = new ArrayList<String>(getPk(this.col_names));
      matching_cols.addAll(indexed_cols);
      matching_cols.addAll(covering_cols);

      this.performWrites(cols_in_index,
        Arrays.asList(
          new Write(
            false, /* predicate */
            -1, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            false, /* predicate */
            0, /* ref_write_index */
            matching_cols, /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */)
        )
      );
    }

    // pred=false (Diff I || C cols), Same pk row exists with pred=false.
    //   1. Diff I
    //   2. Diff C

    // Case 1
    if (this.same_pk_c_diff_i_mulitple_pred_false_rows) {
      List<String> matching_cols = new ArrayList<String>(getPk(this.col_names));
      matching_cols.addAll(covering_cols);
      
      this.performWrites(cols_in_index,
        Arrays.asList(
          new Write(
            false, /* predicate */
            -1, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            false, /* predicate */
            0, /* ref_write_index */
            matching_cols, /* matching_cols */
            Arrays.asList(new ArrayList<String>(indexed_cols)), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */)
        )
      );
    }

    // Case 2
    if (!covering_cols.isEmpty() && this.same_pk_i_diff_c_mulitple_pred_false_rows) {
      List<String> matching_cols = new ArrayList<String>(getPk(this.col_names));
      matching_cols.addAll(indexed_cols);

      this.performWrites(cols_in_index,
        Arrays.asList(
          new Write(
            false, /* predicate */
            -1, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            false, /* predicate */
            0, /* ref_write_index */
            matching_cols, /* matching_cols */
            Arrays.asList(new ArrayList<String>(covering_cols)), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */)
        )
      );
    }

    // pred=true (Same I && C cols), Same pk row exists with pred=true.
    if (this.same_pk_i_c_multiple_pred_true_rows) {
      List<String> matching_cols = new ArrayList<String>(getPk(col_names));
      matching_cols.addAll(indexed_cols);
      matching_cols.addAll(covering_cols);

      this.performWrites(cols_in_index,
        Arrays.asList(
          new Write(
            true, /* predicate */
            -1, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            true, /* predicate */
            0, /* ref_write_index */
            matching_cols, /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */)
        )
      );
    }

    // pred=true (Diff I || C cols), Same pk row exists with pred=true.
    //   1. Diff I
    //   2. Diff C
    
    // Case 1
    if (this.same_pk_c_diff_i_mulitple_pred_true_rows) {
      List<String> matching_cols = new ArrayList<String>(getPk(col_names));
      matching_cols.addAll(covering_cols);

      this.performWrites(cols_in_index,
        Arrays.asList(
          new Write(
            true, /* predicate */
            -1, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            true, /* predicate */
            0, /* ref_write_index */
            matching_cols, /* matching_cols */
            Arrays.asList(new ArrayList<String>(indexed_cols)), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */)
        )
      );
    }

    // Case 2
    if (!covering_cols.isEmpty() && this.same_pk_i_diff_c_mulitple_pred_true_rows) {
      List<String> matching_cols = new ArrayList<String>(getPk(col_names));
      matching_cols.addAll(indexed_cols);

      this.performWrites(cols_in_index,
        Arrays.asList(
          new Write(
            true, /* predicate */
            -1, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            true, /* predicate */
            0, /* ref_write_index */
            matching_cols, /* matching_cols */
            Arrays.asList(new ArrayList<String>(covering_cols)), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */)
        )
      );
    }

    // pred=false (Same I && C cols), Same pk row exists with pred=true.
    if (this.same_pk_i_c_both_pred_true_false_rows) {
      this.performWrites(cols_in_index,
        Arrays.asList(
          new Write(
            true, /* predicate */
            -1, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            false, /* predicate */
            0, /* ref_write_index */
            getPk(this.col_names), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */)
        )
      );
    }

    // pred=false (Diff I || C cols), Same pk row exists with pred=true.
    //   1. Diff I
    //   2. Diff C
    
    // Case 1
    if (this.same_pk_c_diff_i_both_pred_true_false_rows) {
      List<String> matching_cols = new ArrayList<String>(getPk(col_names));
      matching_cols.addAll(covering_cols);

      this.performWrites(cols_in_index,
        Arrays.asList(
          new Write(
            true, /* predicate */
            -1, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            false, /* predicate */
            0, /* ref_write_index */
            matching_cols, /* matching_cols */
            Arrays.asList(new ArrayList<String>(indexed_cols)), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */)
        )
      );
    }

    // Case 2:
    if (!covering_cols.isEmpty() && this.same_pk_i_diff_c_both_pred_true_false_rows) {
      List<String> matching_cols = new ArrayList<String>(getPk(col_names));
      matching_cols.addAll(indexed_cols);

      this.performWrites(cols_in_index,
        Arrays.asList(
          new Write(
            true, /* predicate */
            -1, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            false, /* predicate */
            0, /* ref_write_index */
            matching_cols, /* matching_cols */
            Arrays.asList(new ArrayList<String>(covering_cols)), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */)
        )
      );
    }

    // Update - extra cases applicable only to unique partial indexes.
    // ---------------------------------------------------------------
    //                   ___________________________________________________________________
    //                  | Table has arow with diff pk, same indexed columns, with pred=true |
    //                  | and another row with same pk with pred=false                      |
    //     |------------|-------------------------------------------------------------------|
    //     | pred=true  |    FAIL OP                                                        |
    //     |------------+-------------------------------------------------------------------|
    //     | pred=false |    No-op                                                          |
    //     |--------------------------------------------------------------------------------|

    if (is_unique && this.same_i_diff_pk_mulitple_pred_true_rows) { // For the second existing row and new row
      this.performWrites(cols_in_index,
        Arrays.asList(
          new Write(
            true, /* predicate */
            -1, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            false, /* predicate */
            0, /* ref_write_index */
            new ArrayList<String>(), /* matching_cols */
            Arrays.asList(getPk(col_names)), /* differing_cols_list */
            false, /* should_fail */
            true /* use_insert_stmt */),
          new Write(
            true, /* predicate */
            1, /* ref_write_index */
            getPk(col_names), /* matching_cols */
            new ArrayList<List<String>>(), /* differing_cols_list */
            true, /* should_fail */
            true /* use_insert_stmt */)
        )
      );
    }
  }

  public void testPartialIndexDeletesInternal() throws Exception {

  }

  public void testPartialIndexWrites(boolean strongConsistency, boolean is_unique) throws Exception {
    // Pred: regular column v1=NULL | Indexed cols: [v1] | Covering cols: []
    createTable(
      String.format("create table %s " +
        "(h1 int, h2 int, r1 int, r2 int, v1 int, " +
        "primary key ((h1, h2), r1, r2))", test_table_name),
      strongConsistency);

    this.pk_col_cnt = 4;
    this.col_cnt = 5;
    this.col_names = Arrays.asList("h1", "h2", "r1", "r2", "v1"); // pk cols first, followed by others

    this.same_pk_i_c_both_pred_true_false_rows = false;
    this.same_pk_c_diff_i_both_pred_true_false_rows = true;
    this.same_pk_i_diff_c_both_pred_true_false_rows = false;
    this.same_pk_i_c_multiple_pred_false_rows = false;
    this.same_pk_c_diff_i_mulitple_pred_false_rows = true;
    this.same_pk_i_diff_c_mulitple_pred_false_rows = false;
    this.same_pk_i_c_multiple_pred_true_rows = false;
    this.same_pk_c_diff_i_mulitple_pred_true_rows = false;
    this.same_pk_i_diff_c_mulitple_pred_true_rows = false;

    // Flags for unique partial indexes.
    this.same_i_diff_pk_mulitple_pred_true_rows = true;
    this.same_i_diff_pk_both_pred_true_false_rows = false;

    this.pred_true_rows = Arrays.asList(
      Arrays.asList("1", "1", "1", "1", "NULL"),
      Arrays.asList("1", "1", "1", "2", "NULL")
    );
    this.pred_false_rows = Arrays.asList(
      Arrays.asList("1", "1", "1", "1", "10"),
      Arrays.asList("1", "1", "1", "1", "11"),
      Arrays.asList("1", "1", "1", "2", "10")
    );
    this.already_inserted_true_rows = new ArrayList<Integer>();
    this.already_inserted_false_rows = new ArrayList<Integer>();

    testPartialIndexWritesInternal(
      "v1=NULL", /* predicate */
      Arrays.asList("v1"), /* indexed_cols */
      Arrays.asList(), /* covering_cols */
      strongConsistency,
      is_unique);

    session.execute(String.format("drop table %s", test_table_name));

    // Pred: multi col NULL v1=NULL and v2=NULL | Indexed cols: [v1] | Covering cols: []
    // createTable(
    //   String.format("create table %s " +
    //     "(h1 int, h2 int, r1 int, r2 int, v1 int, v2 int, " +
    //     "primary key ((h1, h2), r1, r2))", test_table_name),
    //   strongConsistency);

    // this.pk_col_cnt = 4;
    // this.col_cnt = 6;
    // this.col_names = Arrays.asList("h1", "h2", "r1", "r2", "v1", "v2"); // pk cols first, followed by others

    // this.same_pk_i_c_both_pred_true_false_rows = true;
    // this.same_pk_c_diff_i_both_pred_true_false_rows = true;
    // this.same_pk_i_diff_c_both_pred_true_false_rows = false;
    // this.same_pk_i_c_multiple_pred_false_rows = true;
    // this.same_pk_c_diff_i_mulitple_pred_false_rows = true;
    // this.same_pk_i_diff_c_mulitple_pred_false_rows = false;
    // this.same_pk_i_c_multiple_pred_true_rows = false;
    // this.same_pk_c_diff_i_mulitple_pred_true_rows = false;
    // this.same_pk_i_diff_c_mulitple_pred_true_rows = false;

    // this.pred_true_rows = Arrays.asList(
    //   Arrays.asList("1", "1", "1", "1", "NULL", "NULL")
    // );
    // this.pred_false_rows = Arrays.asList(
    //   Arrays.asList("1", "1", "1", "1", "NULL", "10")
    // );

    // this.already_inserted_true_rows = new ArrayList<Integer>();
    // this.already_inserted_false_rows = new ArrayList<Integer>();

    // testPartialIndexWritesInternal(
    //     "v1=NULL and v2=NULL", /* predicate */
    //     Arrays.asList("v1"), /* indexed_cols */
    //     Arrays.asList(), /* covering_cols */
    //     strongConsistency,
    //     false /* is_unique */);

    // session.execute(String.format("drop table %s", test_table_name));

    // Pred: regular column v1>5 | Indexed cols: [v1] | Covering cols: []
    createTable(
      String.format("create table %s " +
        "(h1 int, h2 int, r1 int, r2 int, v1 int, " +
        "primary key ((h1, h2), r1, r2))", test_table_name),
      strongConsistency);

    this.pk_col_cnt = 4;
    this.col_cnt = 5;
    this.col_names = Arrays.asList("h1", "h2", "r1", "r2", "v1"); // pk cols first, followed by others

    this.same_pk_i_c_both_pred_true_false_rows = false;
    this.same_pk_c_diff_i_both_pred_true_false_rows = true;
    this.same_pk_i_diff_c_both_pred_true_false_rows = false;
    this.same_pk_i_c_multiple_pred_false_rows = false;
    this.same_pk_c_diff_i_mulitple_pred_false_rows = true;
    this.same_pk_i_diff_c_mulitple_pred_false_rows = false;
    this.same_pk_i_c_multiple_pred_true_rows = false;
    this.same_pk_c_diff_i_mulitple_pred_true_rows = true;
    this.same_pk_i_diff_c_mulitple_pred_true_rows = false;

    // Flags for unique partial indexes.
    this.same_i_diff_pk_mulitple_pred_true_rows = true;
    this.same_i_diff_pk_both_pred_true_false_rows = false;

    this.pred_true_rows = Arrays.asList(
      Arrays.asList("1", "1", "1", "1", "6"),
      Arrays.asList("1", "1", "1", "1", "7"),
      Arrays.asList("1", "1", "1", "2", "6")
    );
    this.pred_false_rows = Arrays.asList(
      Arrays.asList("1", "1", "1", "1", "4"),
      Arrays.asList("1", "1", "1", "1", "3"),
      Arrays.asList("1", "1", "1", "2", "3")
    );
    this.already_inserted_true_rows = new ArrayList<Integer>();
    this.already_inserted_false_rows = new ArrayList<Integer>();

    testPartialIndexWritesInternal(
      "v1>5", /* predicate */
      Arrays.asList("v1"), /* indexed_cols */
      Arrays.asList(), /* covering_cols */
      strongConsistency,
      is_unique /* is_unique */);
  }

  @Test
  public void testPartialIndexWrites() throws Exception {
    testPartialIndexWrites(true /* strongConsistency */, false /* is_unique */);
  }

  @Test
  public void testWeakPartialIndexWrites() throws Exception {
    testPartialIndexWrites(false /* strongConsistency */, false /* is_unique */);
  }

  @Test
  public void testUniquePartialIndexWrites() throws Exception {
    testPartialIndexWrites(true /* strongConsistency */, true /* is_unique */);
  }

  @Test
  public void testWeakUniquePartialIndexWrites() throws Exception {
    testPartialIndexWrites(false /* strongConsistency */, true /* is_unique */);
  }
}
