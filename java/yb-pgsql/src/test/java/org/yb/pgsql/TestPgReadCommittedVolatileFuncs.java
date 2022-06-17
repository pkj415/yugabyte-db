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

package org.yb.pgsql;

import static org.yb.AssertionWrappers.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.yb.util.ThreadUtil;
import org.yb.util.YBTestRunnerNonTsanOnly;

/**
 *
 * Test to ensure that each statement in a volatile plpgsql function uses a
 * new snapshot and all statements in immutable and stable functions use the
 * same snapshot.
 * 
 * Quoting from Pg docs (https://www.postgresql.org/docs/current/xfunc-volatility.html):
 *   "STABLE and IMMUTABLE functions use a snapshot established as of the start of the calling
 *    query, whereas VOLATILE functions obtain a fresh snapshot at the start of each
 *    query they execute."
 */
@RunWith(value = YBTestRunnerNonTsanOnly.class)
public class TestPgReadCommittedVolatileFuncs extends BasePgSQLTest {
  private static final Logger LOG =
    LoggerFactory.getLogger(TestPgReadCommittedVolatileFuncs.class);

  @Override
  protected Map<String, String> getTServerFlags() {
    Map<String, String> flags = super.getTServerFlags();
    flags.put("yb_enable_read_committed_isolation", "true");
    return flags;
  }

  String get_function_definition_str(String volatility_class) {
    return
      "create or replace function " + volatility_class + "_plpgsql_func() returns TABLE(v int) " +
      "  AS $$" +
      "    BEGIN " +
      "    RETURN QUERY select v from test where k=1;" +
      "    perform pg_sleep(2);" +
      "    RETURN QUERY select v from test where k=1;" +
      "    END;" +
      "  $$ LANGUAGE PLPGSQL " + volatility_class;
  }

  @Test
  public void testFunctionSemantics() throws Exception {
    String[] volatility_classes = {"VOLATILE", "IMMUTABLE", "STABLE"};
    try (Statement statement = connection.createStatement()) {
      statement.execute("create table test (k int primary key, v int)");
      statement.execute("insert into test values (1, 0)");
      statement.execute(
        "create or replace procedure update_row() " +
        "  AS $$" +
        "    DECLARE " +
        "      end_time timestamp;" +
        "    BEGIN " +
        "    end_time := NOW() + interval '5 seconds';" +
        "    while end_time > NOW() LOOP" +
        "       update test set v = EXTRACT(EPOCH FROM AGE(end_time, NOW())) where k=1;" +
        "       commit;" +
        "    END LOOP;" +
        "    END;" +
        "  $$ LANGUAGE PLPGSQL");
      for (String volatility_class : volatility_classes) {
        statement.execute(get_function_definition_str(volatility_class));
      }
    }

    ExecutorService es = Executors.newFixedThreadPool(4);
    List<Future<?>> futures = new ArrayList<>();
    List<Runnable> runnables = new ArrayList<>();

    runnables.add(() -> {
      try (Connection conn =
              getConnectionBuilder().withIsolationLevel(IsolationLevel.READ_COMMITTED)
              .withAutoCommit(AutoCommit.ENABLED).connect();
            Statement stmt = conn.createStatement();) {
        stmt.execute("CALL update_row();");
      }
      catch (Exception ex) {
        fail("Failed due to exception: " + ex.getMessage());
      }
    });

    for (String volatility_class : volatility_classes) {
      runnables.add(() -> {
        try (Connection conn =
                getConnectionBuilder().withIsolationLevel(IsolationLevel.READ_COMMITTED)
                .withAutoCommit(AutoCommit.ENABLED).connect();
              Statement stmt = conn.createStatement();) {
          ResultSet rs =  stmt.executeQuery("SELECT " + volatility_class + "_plpgsql_func();");
          assertTrue(rs.next());
          int firstVal = rs.getInt("v");
          assertTrue(rs.next());
          int secondVal = rs.getInt("v");
          if (volatility_class.equalsIgnoreCase("VOLATILE"))
            assertTrue(secondVal != firstVal);
          else
            assertTrue(secondVal != firstVal);
        }
        catch (Exception ex) {
          fail("Failed due to exception: " + ex.getMessage());
        }
      });
    }

    for (Runnable r : runnables) {
      futures.add(es.submit(r));
    }

    try {
      LOG.info("Waiting for all threads");
      for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
    } catch (TimeoutException ex) {
      LOG.warn("Threads info:\n\n" + ThreadUtil.getAllThreadsInfo());
      fail("Waiting for threads timed out, this is unexpected!");
    }

    try (Statement statement = connection.createStatement()) {
      statement.execute("DROP TABLE test");
      statement.execute("DROP PROCEDURE update_row");
      for (String volatility_class : volatility_classes) {
        statement.execute("DROP FUNCTION " + volatility_class + "_plpgsql_func");
      }
    }
  }
}
