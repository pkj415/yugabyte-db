// Copyright (c) YugabyteDB, Inc.
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
package org.yb.pgsql;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.YBTestRunner;

import static org.yb.AssertionWrappers.assertLessThan;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Verifies that REFRESH MATERIALIZED VIEW with VARCHAR columns does not leak
 * memory through unnecessary palloc allocations in YbDatumToVarchar.
 *
 * Background:
 *   The old YbDatumToVarchar called TextDatumGetCString(datum), which invokes
 *   text_to_cstring -> palloc to create a new null-terminated C string for
 *   every row.  After DatumToQLValue copies the bytes into the protobuf arena
 *   via dup_string_value(), the palloc'd original was never freed.  During a
 *   REFRESH MATERIALIZED VIEW the executor processes all rows in a single
 *   query-level MemoryContext, so the leaked copies accumulated for the
 *   entire operation.  The memory IS freed once the statement completes, so
 *   the leak is transient -- but it can cause OOM before the statement
 *   finishes.
 *
 *   The fix changes YbDatumToVarchar to behave like YbDatumToText: return a
 *   direct pointer into the (detoasted) varlena datum instead of palloc'ing a
 *   copy.
 *
 * What this test does:
 *   1. Creates a table with a VARCHAR column and inserts rows with large
 *      (100 KB) string values.
 *   2. Creates a materialized view over that table.
 *   3. Runs REFRESH in a background thread while the main thread polls the
 *      backend's RSS to capture the transient peak.
 *   4. Logs baseline, peak, and post-refresh RSS for inspection.
 *
 * With the old code the peak RSS during refresh would exceed the baseline by
 * roughly NUM_ROWS * VARCHAR_SIZE (the leaked copies).  With the fix the peak
 * stays much closer to the baseline.
 */
@RunWith(value = YBTestRunner.class)
public class TestPgMatviewRefreshMemory extends BasePgSQLTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestPgMatviewRefreshMemory.class);

  private static final int NUM_ROWS = 500;
  private static final int VARCHAR_SIZE = 100_000; // 100 KB per row
  private static final long RSS_POLL_INTERVAL_MS = 2;

  @Override
  protected int getReplicationFactor() {
    return 1;
  }

  @Override
  protected int getInitialNumMasters() {
    return 1;
  }

  @Override
  protected int getInitialNumTServers() {
    return 1;
  }

  @Test
  public void testRefreshMatviewVarcharMemory() throws Exception {
    final long totalVarcharDataMB = (long) NUM_ROWS * VARCHAR_SIZE / (1024 * 1024);

    // Use a dedicated connection for the refresh so its backend PID is stable.
    try (Connection refreshConn = getConnectionBuilder().connect();
         Statement setupStmt = refreshConn.createStatement()) {

      // --- Setup: source table with large VARCHAR values ---
      LOG.info("Creating source table with {} rows of {} byte VARCHAR values " +
               "(~{} MB total varchar data)", NUM_ROWS, VARCHAR_SIZE, totalVarcharDataMB);

      setupStmt.execute("CREATE TABLE mv_src (id INT PRIMARY KEY, val VARCHAR)");
      setupStmt.execute(
          "INSERT INTO mv_src SELECT i, repeat('x', " + VARCHAR_SIZE + ") " +
          "FROM generate_series(1, " + NUM_ROWS + ") i");

      // --- Create the materialized view (initial populate) ---
      setupStmt.execute("CREATE MATERIALIZED VIEW mv_test AS SELECT * FROM mv_src");

      // --- Get the backend PID for RSS measurement ---
      ResultSet rs = setupStmt.executeQuery("SELECT pg_backend_pid()");
      rs.next();
      final int pgPid = rs.getInt(1);
      rs.close();
      LOG.info("PostgreSQL backend PID for refresh connection: {}", pgPid);

      final long rssBaseline = getRssForPid(pgPid);
      LOG.info("RSS baseline (after initial populate, before refresh): {} kB ({} MB)",
               rssBaseline, rssBaseline / 1024);

      // --- Run REFRESH in a background thread, poll RSS from main thread ---
      long peakRss = rssBaseline;
      final Exception[] refreshError = {null};

      Thread refreshThread = new Thread(() -> {
        try (Statement stmt = refreshConn.createStatement()) {
          stmt.execute("REFRESH MATERIALIZED VIEW mv_test");
        } catch (Exception e) {
          refreshError[0] = e;
        }
      });

      refreshThread.start();

      // Poll RSS while the refresh is running.
      int samples = 0;
      while (refreshThread.isAlive()) {
        try {
          long rss = getRssForPid(pgPid);
          samples++;
          if (rss > peakRss) {
            peakRss = rss;
          }
        } catch (Exception e) {
          // Process may momentarily be unavailable; ignore.
        }
        Thread.sleep(RSS_POLL_INTERVAL_MS);
      }

      refreshThread.join();

      if (refreshError[0] != null) {
        throw new RuntimeException("REFRESH MATERIALIZED VIEW failed", refreshError[0]);
      }

      final long rssAfterRefresh = getRssForPid(pgPid);
      final long peakGrowthKB = peakRss - rssBaseline;

      LOG.info("RSS polling captured {} samples during refresh", samples);
      LOG.info("RSS baseline:       {} kB ({} MB)", rssBaseline, rssBaseline / 1024);
      LOG.info("RSS peak (during):  {} kB ({} MB)", peakRss, peakRss / 1024);
      LOG.info("RSS after refresh:  {} kB ({} MB)", rssAfterRefresh, rssAfterRefresh / 1024);
      LOG.info("Peak growth over baseline: {} kB ({} MB)",
               peakGrowthKB, peakGrowthKB / 1024);
      LOG.info("Total varchar data in table: ~{} MB", totalVarcharDataMB);

      // With the fix, peak RSS growth during refresh should be well under the total
      // varchar data size. Allow up to half as headroom for other allocations.
      long maxAllowedGrowthKB = (totalVarcharDataMB * 1024) / 2;
      assertLessThan(
          "Peak RSS growth during REFRESH should be much less than total varchar data size "
          + "(" + totalVarcharDataMB + " MB). Leaked palloc copies likely not fixed.",
          peakGrowthKB, maxAllowedGrowthKB);
    }
  }
}
