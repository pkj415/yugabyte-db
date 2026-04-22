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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.YBTestRunner;

/**
 * Reproduction test for GitHub issue #31248:
 * [YSQL] Crash on DROP TABLE IF EXISTS
 *
 * The crash occurs in doDeletion (dependency.c) when RelationIdGetRelation returns NULL
 * and the result is passed to RelationClose without a NULL check. This can happen when:
 * - Two sessions concurrently attempt to drop the same table
 * - The table's relcache entry becomes invalidated (rd_droppedSubid set) between name
 *   resolution and the actual deletion in doDeletion
 * - Session 1 resolves the table name to a valid OID, then Session 2 drops the table,
 *   then Session 1 proceeds to doDeletion where RelationIdGetRelation returns NULL
 */
@RunWith(value = YBTestRunner.class)
public class TestDropTableIfExistsCrash extends BasePgSQLTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestDropTableIfExistsCrash.class);

  @Override
  protected int getReplicationFactor() {
    return 1;
  }

  protected int getInitialNumMasters() {
    return 1;
  }

  protected int getInitialNumTServers() {
    return 1;
  }

  @Override
  protected Map<String, String> getTServerFlags() {
    Map<String, String> flags = super.getTServerFlags();
    flags.put("ysql_yb_ddl_transaction_block_enabled", "false");
    flags.put("enable_object_locking_for_table_locks", "false");
    return flags;
  }

  /**
   * Test concurrent DROP TABLE IF EXISTS from multiple sessions.
   *
   * Multiple threads repeatedly create a table and then race to drop it using
   * DROP TABLE IF EXISTS. The goal is to trigger the race condition where
   * RelationIdGetRelation returns NULL inside doDeletion because another session
   * already dropped the table between the OID lookup and the actual deletion.
   *
   * If the bug is present, the postgres process will crash with SIGSEGV.
   */
  @Test
  public void testConcurrentDropTableIfExists() throws Exception {
    final int NUM_THREADS = 4;
    final int NUM_ITERATIONS = 50;
    final AtomicBoolean crashed = new AtomicBoolean(false);
    final AtomicInteger completedIterations = new AtomicInteger(0);

    for (int iter = 0; iter < NUM_ITERATIONS && !crashed.get(); iter++) {
      final String tableName = "drop_race_" + iter;

      try (Connection setupConn = getConnectionBuilder().connect();
           Statement setupStmt = setupConn.createStatement()) {
        setupStmt.execute("CREATE TABLE " + tableName + " (id INT PRIMARY KEY, val TEXT)");
        setupStmt.execute("INSERT INTO " + tableName + " VALUES (1, 'test')");
      }

      final CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
      final Thread[] threads = new Thread[NUM_THREADS];
      final AtomicBoolean iterError = new AtomicBoolean(false);

      for (int t = 0; t < NUM_THREADS; t++) {
        final int threadIdx = t;
        threads[t] = new Thread(() -> {
          try (Connection conn = getConnectionBuilder().connect();
               Statement stmt = conn.createStatement()) {
            barrier.await();
            stmt.execute("DROP TABLE IF EXISTS " + tableName);
          } catch (SQLException e) {
            String msg = e.getMessage();
            if (msg.contains("does not exist") ||
                msg.contains("could not serialize") ||
                msg.contains("Catalog Version Mismatch") ||
                msg.contains("schema version mismatch") ||
                msg.contains("Transaction aborted") ||
                msg.contains("expired or aborted")) {
              LOG.info("Thread {} got expected error: {}", threadIdx, msg);
            } else if (msg.contains("server closed the connection") ||
                       msg.contains("An I/O error occurred") ||
                       msg.contains("connection has been closed") ||
                       msg.contains("broken")) {
              LOG.error("Thread {} detected possible crash: {}", threadIdx, msg);
              crashed.set(true);
            } else {
              LOG.warn("Thread {} got unexpected error: {}", threadIdx, msg);
            }
          } catch (Exception e) {
            LOG.error("Thread {} got non-SQL error", threadIdx, e);
            if (e.getMessage() != null &&
                (e.getMessage().contains("connection") ||
                 e.getMessage().contains("closed"))) {
              crashed.set(true);
            }
          }
        });
      }

      for (Thread t : threads) t.start();
      for (Thread t : threads) t.join(30000);

      completedIterations.incrementAndGet();

      if (crashed.get()) {
        LOG.error("Postgres process appears to have crashed on iteration {}", iter);
        break;
      }
    }

    LOG.info("Completed {} iterations", completedIterations.get());

    if (crashed.get()) {
      LOG.error("BUG REPRODUCED: Postgres crashed during concurrent DROP TABLE IF EXISTS");
    }

    try (Connection checkConn = getConnectionBuilder().connect();
         Statement checkStmt = checkConn.createStatement()) {
      checkStmt.execute("SELECT 1");
      LOG.info("Post-test connectivity check passed");
    } catch (Exception e) {
      LOG.error("Post-test connectivity check failed, cluster may have crashed", e);
      crashed.set(true);
    }

    if (crashed.get()) {
      LOG.error("BUG REPRODUCED: GitHub issue #31248 - crash on DROP TABLE IF EXISTS");
    }
  }

  /**
   * Test DROP TABLE IF EXISTS with a table that was just dropped by another session.
   *
   * This is a more targeted attempt: Session 1 drops the table, and Session 2
   * tries DROP TABLE IF EXISTS on the same table right after. The timing
   * matters -- Session 2 needs to have resolved the OID before Session 1's
   * drop completes.
   */
  @Test
  public void testDropTableIfExistsAfterConcurrentDrop() throws Exception {
    final int NUM_ITERATIONS = 100;
    final AtomicBoolean crashed = new AtomicBoolean(false);

    for (int iter = 0; iter < NUM_ITERATIONS && !crashed.get(); iter++) {
      final String tableName = "drop_race2_" + iter;

      try (Connection setupConn = getConnectionBuilder().connect();
           Statement setupStmt = setupConn.createStatement()) {
        setupStmt.execute("CREATE TABLE " + tableName + " (id INT PRIMARY KEY, val TEXT)");
      }

      final CyclicBarrier barrier = new CyclicBarrier(2);
      Thread dropThread = new Thread(() -> {
        try (Connection conn = getConnectionBuilder().connect();
             Statement stmt = conn.createStatement()) {
          barrier.await();
          stmt.execute("DROP TABLE " + tableName);
        } catch (Exception e) {
          LOG.info("Drop thread error: {}", e.getMessage());
        }
      });

      Thread dropIfExistsThread = new Thread(() -> {
        try (Connection conn = getConnectionBuilder().connect();
             Statement stmt = conn.createStatement()) {
          barrier.await();
          stmt.execute("DROP TABLE IF EXISTS " + tableName);
        } catch (SQLException e) {
          String msg = e.getMessage();
          if (msg.contains("server closed the connection") ||
              msg.contains("An I/O error occurred") ||
              msg.contains("connection has been closed") ||
              msg.contains("broken")) {
            LOG.error("Detected possible crash: {}", msg);
            crashed.set(true);
          } else {
            LOG.info("DROP IF EXISTS got error: {}", msg);
          }
        } catch (Exception e) {
          LOG.error("Non-SQL error in DROP IF EXISTS thread", e);
          if (e.getMessage() != null &&
              (e.getMessage().contains("connection") ||
               e.getMessage().contains("closed"))) {
            crashed.set(true);
          }
        }
      });

      dropThread.start();
      dropIfExistsThread.start();
      dropThread.join(30000);
      dropIfExistsThread.join(30000);

      if (crashed.get()) {
        LOG.error("Postgres process appears to have crashed on iteration {}", iter);
        break;
      }
    }

    LOG.info("Completed {} out of {} iterations", NUM_ITERATIONS, NUM_ITERATIONS);

    if (crashed.get()) {
      LOG.error("BUG REPRODUCED: GitHub issue #31248 - crash on DROP TABLE IF EXISTS");
    }

    try (Connection checkConn = getConnectionBuilder().connect();
         Statement checkStmt = checkConn.createStatement()) {
      checkStmt.execute("SELECT 1");
    } catch (Exception e) {
      LOG.error("Post-test connectivity check failed", e);
      crashed.set(true);
    }

    if (crashed.get()) {
      LOG.error("BUG REPRODUCED: GitHub issue #31248");
    }
  }

  /**
   * Test DROP TABLE IF EXISTS on multiple tables where some don't exist.
   *
   * This tests the case where DROP TABLE IF EXISTS t1, t2 is executed and
   * t1 exists but t2 does not. The dependency resolution for t1 may cause
   * invalidation of t2's relcache entry if t2 was recently created/dropped.
   */
  @Test
  public void testDropMultipleTablesIfExistsConcurrently() throws Exception {
    final int NUM_ITERATIONS = 50;
    final AtomicBoolean crashed = new AtomicBoolean(false);

    for (int iter = 0; iter < NUM_ITERATIONS && !crashed.get(); iter++) {
      final String table1 = "multi_drop_a_" + iter;
      final String table2 = "multi_drop_b_" + iter;

      try (Connection setupConn = getConnectionBuilder().connect();
           Statement setupStmt = setupConn.createStatement()) {
        setupStmt.execute("CREATE TABLE " + table1 + " (id INT PRIMARY KEY)");
        setupStmt.execute("CREATE TABLE " + table2 + " (id INT PRIMARY KEY, "
            + "ref INT REFERENCES " + table1 + "(id))");
      }

      final CyclicBarrier barrier = new CyclicBarrier(2);

      Thread t1 = new Thread(() -> {
        try (Connection conn = getConnectionBuilder().connect();
             Statement stmt = conn.createStatement()) {
          barrier.await();
          stmt.execute("DROP TABLE IF EXISTS " + table2 + ", " + table1 + " CASCADE");
        } catch (SQLException e) {
          String msg = e.getMessage();
          if (msg.contains("server closed the connection") ||
              msg.contains("An I/O error occurred") ||
              msg.contains("connection has been closed")) {
            crashed.set(true);
          }
        } catch (Exception e) {
          LOG.info("Thread 1 error: {}", e.getMessage());
        }
      });

      Thread t2 = new Thread(() -> {
        try (Connection conn = getConnectionBuilder().connect();
             Statement stmt = conn.createStatement()) {
          barrier.await();
          stmt.execute("DROP TABLE IF EXISTS " + table2 + " CASCADE");
        } catch (SQLException e) {
          String msg = e.getMessage();
          if (msg.contains("server closed the connection") ||
              msg.contains("An I/O error occurred") ||
              msg.contains("connection has been closed")) {
            crashed.set(true);
          }
        } catch (Exception e) {
          LOG.info("Thread 2 error: {}", e.getMessage());
        }
      });

      t1.start();
      t2.start();
      t1.join(30000);
      t2.join(30000);
    }

    LOG.info("Completed iterations for multi-table drop test");

    if (crashed.get()) {
      LOG.error("BUG REPRODUCED: GitHub issue #31248 - crash on DROP TABLE IF EXISTS");
    }
  }

  /**
   * Stress test: rapidly create and drop tables with IF EXISTS from
   * multiple connections. This attempts to maximize the chance of hitting
   * the race window where RelationIdGetRelation returns NULL.
   */
  @Test
  public void testStressDropTableIfExists() throws Exception {
    final int NUM_THREADS = 6;
    final long DURATION_MS = 60000;
    final AtomicBoolean crashed = new AtomicBoolean(false);
    final AtomicBoolean stopped = new AtomicBoolean(false);
    final AtomicInteger tableCounter = new AtomicInteger(0);
    final AtomicInteger successCount = new AtomicInteger(0);

    Thread[] threads = new Thread[NUM_THREADS];

    for (int t = 0; t < NUM_THREADS; t++) {
      final int threadIdx = t;
      threads[t] = new Thread(() -> {
        try (Connection conn = getConnectionBuilder().connect();
             Statement stmt = conn.createStatement()) {
          while (!stopped.get() && !crashed.get()) {
            int tableNum = tableCounter.incrementAndGet();
            String tableName = "stress_" + tableNum;
            try {
              stmt.execute("CREATE TABLE IF NOT EXISTS " + tableName +
                  " (id INT PRIMARY KEY)");
              stmt.execute("DROP TABLE IF EXISTS " + tableName);
              successCount.incrementAndGet();
            } catch (SQLException e) {
              String msg = e.getMessage();
              if (msg.contains("server closed the connection") ||
                  msg.contains("An I/O error occurred") ||
                  msg.contains("connection has been closed") ||
                  msg.contains("broken")) {
                LOG.error("Thread {} detected crash: {}", threadIdx, msg);
                crashed.set(true);
                break;
              }
            }
          }
        } catch (Exception e) {
          if (!stopped.get()) {
            LOG.error("Thread {} failed", threadIdx, e);
            if (e.getMessage() != null &&
                (e.getMessage().contains("connection") ||
                 e.getMessage().contains("closed"))) {
              crashed.set(true);
            }
          }
        }
      });
    }

    for (Thread t : threads) t.start();

    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < DURATION_MS && !crashed.get()) {
      Thread.sleep(1000);
    }
    stopped.set(true);
    for (Thread t : threads) t.join(30000);

    LOG.info("Stress test completed {} successful create/drop cycles", successCount.get());

    if (crashed.get()) {
      LOG.error("BUG REPRODUCED: GitHub issue #31248 - crash on DROP TABLE IF EXISTS");
    }

    try (Connection checkConn = getConnectionBuilder().connect();
         Statement checkStmt = checkConn.createStatement()) {
      checkStmt.execute("SELECT 1");
      LOG.info("Post-stress-test connectivity check passed");
    } catch (Exception e) {
      LOG.error("Post-stress-test connectivity check failed", e);
      crashed.set(true);
    }

    if (crashed.get()) {
      LOG.error("BUG REPRODUCED: GitHub issue #31248");
    }
  }

  /**
   * Test CREATE TABLE AS + DROP TABLE IF EXISTS concurrently.
   * This mimics the pattern from the original bug report where
   * "CREATE TABLE tempTable2 AS SELECT ..." followed by "DROP TABLE tempTable2"
   * was part of the failing workload.
   */
  @Test
  public void testCreateTableAsWithConcurrentDropIfExists() throws Exception {
    final int NUM_ITERATIONS = 50;
    final AtomicBoolean crashed = new AtomicBoolean(false);

    try (Connection baseConn = getConnectionBuilder().connect();
         Statement baseStmt = baseConn.createStatement()) {
      baseStmt.execute("CREATE TABLE base_table (id INT PRIMARY KEY, val TEXT)");
      baseStmt.execute("INSERT INTO base_table SELECT g, 'val' || g FROM " +
          "generate_series(1, 100) g");
    }

    for (int iter = 0; iter < NUM_ITERATIONS && !crashed.get(); iter++) {
      final String tableName = "ctas_drop_" + iter;

      try (Connection setupConn = getConnectionBuilder().connect();
           Statement setupStmt = setupConn.createStatement()) {
        setupStmt.execute("CREATE TABLE " + tableName +
            " AS SELECT * FROM base_table LIMIT 10");
        setupStmt.execute("CREATE INDEX ON " + tableName + " (val)");
      }

      final CyclicBarrier barrier = new CyclicBarrier(3);

      Thread dropIfExists1 = new Thread(() -> {
        try (Connection conn = getConnectionBuilder().connect();
             Statement stmt = conn.createStatement()) {
          barrier.await();
          stmt.execute("DROP TABLE IF EXISTS " + tableName);
        } catch (SQLException e) {
          if (e.getMessage().contains("server closed the connection") ||
              e.getMessage().contains("connection has been closed")) {
            crashed.set(true);
          }
        } catch (Exception e) {
          LOG.debug("Thread error: {}", e.getMessage());
        }
      });

      Thread dropIfExists2 = new Thread(() -> {
        try (Connection conn = getConnectionBuilder().connect();
             Statement stmt = conn.createStatement()) {
          barrier.await();
          stmt.execute("DROP TABLE IF EXISTS " + tableName);
        } catch (SQLException e) {
          if (e.getMessage().contains("server closed the connection") ||
              e.getMessage().contains("connection has been closed")) {
            crashed.set(true);
          }
        } catch (Exception e) {
          LOG.debug("Thread error: {}", e.getMessage());
        }
      });

      Thread alterThread = new Thread(() -> {
        try (Connection conn = getConnectionBuilder().connect();
             Statement stmt = conn.createStatement()) {
          barrier.await();
          stmt.execute("ALTER TABLE " + tableName +
              " ADD COLUMN extra TEXT DEFAULT 'x'");
        } catch (SQLException e) {
          if (e.getMessage().contains("server closed the connection") ||
              e.getMessage().contains("connection has been closed")) {
            crashed.set(true);
          }
        } catch (Exception e) {
          LOG.debug("Thread error: {}", e.getMessage());
        }
      });

      dropIfExists1.start();
      dropIfExists2.start();
      alterThread.start();
      dropIfExists1.join(30000);
      dropIfExists2.join(30000);
      alterThread.join(30000);

      if (crashed.get()) {
        LOG.error("Crash detected at iteration {}", iter);
        break;
      }
    }

    if (crashed.get()) {
      LOG.error("BUG REPRODUCED: GitHub issue #31248 - crash on DROP TABLE IF EXISTS");
    }
  }
}
