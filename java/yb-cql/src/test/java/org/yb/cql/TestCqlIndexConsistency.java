// Copyright (c) Yugabyte, Inc.
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

import com.datastax.driver.core.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.YBTestRunner;
import org.yb.minicluster.BaseMiniClusterTest;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Random;

import static org.yb.AssertionWrappers.assertEquals;
import static org.yb.AssertionWrappers.assertFalse;

@RunWith(value=YBTestRunner.class)
public class TestCqlIndexConsistency extends BaseCQLTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestCqlIndexConsistency.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseMiniClusterTest.tserverArgs.add("--allow_index_table_read_write");
    BaseCQLTest.setUpBeforeClass();
  }

  private static final int MIN_KEY_ID = 100_000_000;
  private static final int NUM_KEY_IDS = 100;
  private static final int NUM_VALUES = 200;

  private static int NUM_KEY_TYPES = 2;
  private static String KEYTYPE_PREFIX = "KEYTYPE";

  private static String DELIMITER = "_";

  private static final int NUM_LOCKS = NUM_KEY_IDS * NUM_KEY_TYPES;

  @Override
  public int getTestMethodTimeoutSec() {
    return 600;
  }

  private static String getKeyByIndex(int keyIndex) {
    return String.valueOf(MIN_KEY_ID + keyIndex);
  }

  private static String genRandomKeyId() {
    return getKeyByIndex(ThreadLocalRandom.current().nextInt(NUM_KEY_IDS));
  }

  public static int keyIdToIndex(String keyId) {
    return (int) (Long.valueOf(keyId) - MIN_KEY_ID);
  }

  public static int keyTypeToIndex(String keyType) {
    return Integer.valueOf(keyType.substring(KEYTYPE_PREFIX.length())) - 1;
  }

  public static String indexToKeyType(int keyTypeIndex) {
    return KEYTYPE_PREFIX + (keyTypeIndex + 1);
  }

  private static String genRandomKeyType() {
    return indexToKeyType(ThreadLocalRandom.current().nextInt(NUM_KEY_TYPES));
  }

  private static String genRandomValue() {
    int minSuffix = 100_000_000;
    return "AAA" + ThreadLocalRandom.current().nextLong(minSuffix, minSuffix + NUM_VALUES);
  }

  private static String getKeyAndTypeStr(String k, String keyType) {
    return k + DELIMITER + keyType;
  }

  private int keyAndTypeToLockIndex(String key, String keyType) {
    return keyIdToIndex(key) * NUM_KEY_TYPES + keyTypeToIndex(keyType);

  }

  @Test
  public void testCqlIndexConsistency() throws Exception {
    try (Cluster cluster = getDefaultClusterBuilder().build();
         final Session session = cluster.connect()) {
      session.execute("CREATE KEYSPACE my_keyspace");
      String createTable =
          "CREATE TABLE my_keyspace.my_mapping_table (" +
          "key_id text, " +
          "key_type text, " +
          "key_value text, " +
          "modified_at timestamp, " +
          "version bigint, " +
          "PRIMARY KEY (key_id, key_type) " +
          ") WITH CLUSTERING ORDER BY (key_type ASC) AND default_time_to_live = 0 AND " +
          "transactions = { 'enabled' : true };";
      String createIndex =
          "CREATE UNIQUE INDEX my_index ON " +
          "my_keyspace.my_mapping_table (key_value, key_type);";
      session.execute(createTable);
      session.execute(createIndex);
      final int numWriterThreads = 5;
      final int numDeletionThreads = 5;
      final int numReaderThread = 10;
      ExecutorCompletionService ecs = new ExecutorCompletionService(
          Executors.newFixedThreadPool(
              numWriterThreads + numReaderThread + numDeletionThreads));
      final AtomicBoolean stop = new AtomicBoolean(false);

      final int INSERTS_PER_TXN = 3;

      List<Future<Void>> futures = new ArrayList<>();

      final AtomicInteger numInsertTxnAttempts = new AtomicInteger(0);
      final AtomicInteger numInsertTxnSuccesses = new AtomicInteger(0);

      final AtomicInteger numDeletionAttempts = new AtomicInteger(0);
      final AtomicInteger numDeletionSuccesses = new AtomicInteger(0);

      final AtomicInteger numReadsRowNotFound = new AtomicInteger(0);
      final AtomicInteger numReadsRowFound = new AtomicInteger(0);
      final AtomicInteger numReadsRowAndIndexFound = new AtomicInteger(0);

      final List<Lock> locks = new ArrayList<>();
      for (int i = 0; i < NUM_LOCKS; ++i) {
        locks.add(new ReentrantLock());
      }
      final List<AtomicInteger> nextVersions = new ArrayList<>();
      for (int i = 0; i < NUM_LOCKS; ++i) {
        nextVersions.add(new AtomicInteger());
      }
      final AtomicBoolean failed = new AtomicBoolean(false);

      // Insertion / overwrite threads.
      for (int wThreadIndex = 1; wThreadIndex <= numWriterThreads; ++wThreadIndex) {
        final String threadName = "Workload writer thread " + wThreadIndex;
        futures.add(ecs.submit(() -> {
          Thread.currentThread().setName(threadName);
          LOG.info("Thread starting: {}", threadName);
          SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
          while (!stop.get()) {
            try {
              StringBuilder sb = new StringBuilder();
              
              sb.append("BEGIN TRANSACTION ");
              Set<String> keyAndTypeSet = new TreeSet<>();
              Set<String> valueAndTypeSet = new TreeSet<>();

              List<Integer> lockIndexes = new ArrayList<Integer>();

              for (int i = 0; i < INSERTS_PER_TXN; ++i) {
                String k = genRandomKeyId();
                String v = genRandomValue();
                String keyType = genRandomKeyType();
                String keyAndType = getKeyAndTypeStr(k, keyType);
                int lockIndex = keyAndTypeToLockIndex(k, keyType);
                lockIndexes.add(lockIndex);
                String valueAndType = v + "_" + keyType;
                AtomicInteger versionAtomic = nextVersions.get(lockIndex);
                int expectedVersion = versionAtomic.getAndIncrement();
                int newVersion = expectedVersion + 1;

                if (!keyAndTypeSet.contains(keyAndType) &&
                    !valueAndTypeSet.contains(valueAndType)) {
                  keyAndTypeSet.add(keyAndType);
                  valueAndTypeSet.add(valueAndType);
                  sb.append(
                    String.format(
                      "INSERT INTO my_keyspace.my_mapping_table " +
                      "(key_id, key_type, key_value, modified_at, version) VALUES " +
                      "('%s', '%s', '%s', '%s', %d) " +
                      "IF version = null OR version <= %d ELSE ERROR; ",
                      k, keyType, v, simpleDateFormat.format(new Date()), newVersion,
                      expectedVersion
                    )
                  );
                }
              }
              sb.append("END TRANSACTION;");

              numInsertTxnAttempts.incrementAndGet();
              Collections.sort(lockIndexes);

              int lockedUntil = 0;
              try {
                for (int i = 0; i < lockIndexes.size(); ++i) {
                  locks.get(lockIndexes.get(i)).lock();
                  lockedUntil = i + 1;
                }

                session.execute(sb.toString());
                numInsertTxnSuccesses.incrementAndGet();
              } finally {
                for (int i = lockedUntil - 1; i >= 0; --i) {
                  locks.get(lockIndexes.get(i)).unlock();
                }
              }

            } catch (com.datastax.driver.core.exceptions.InvalidQueryException ex) {
              String msg = ex.getMessage();
              if (msg.contains("Duplicate value disallowed") ||
                  msg.contains("Duplicate request") ||
                  msg.contains("Condition on table ")) {
                continue;
              }
              LOG.error("Exception in: {}", threadName, ex);
              stop.set(true);
              failed.set(true);
              break;
            } catch (Exception ex) {
              LOG.error("Exception in: {}", threadName, ex);
              stop.set(true);
              failed.set(true);
              break;
            }
          }
          return null;
        }));
      }

      // Deletion threads.
      final PreparedStatement preparedDeleteStatement = session.prepare(
          "DELETE FROM my_keyspace.my_mapping_table " +
          "WHERE key_id = ? AND key_type = ?");

      for (int dThreadIndex = 1; dThreadIndex <= numDeletionThreads; ++dThreadIndex) {
        final String threadName = "Workload deletion thread " + dThreadIndex;
        futures.add(ecs.submit(() -> {
          Thread.currentThread().setName(threadName);
          while (!stop.get()) {
            try {
              StringBuilder sb = new StringBuilder();
              String k = genRandomKeyId();
              String keyType = genRandomKeyType();

              BoundStatement boundDeleteStatement = preparedDeleteStatement.bind(k, keyType);
              numDeletionAttempts.incrementAndGet();

              int lockIndex = keyAndTypeToLockIndex(k, keyType);
              Lock lock = locks.get(lockIndex);
              lock.lock();

              try {
                session.execute(boundDeleteStatement);
                numDeletionSuccesses.incrementAndGet();
              } finally {
                lock.unlock();
              }
            } catch (Exception ex) {
              LOG.error("Exception in: {}", threadName, ex);
              stop.set(true);
              failed.set(true);
              break;
            }
          }
          return null;
        }));
      }

      // Reader (verification) threads.
      for (int rThreadIndex = 1; rThreadIndex <= numReaderThread; ++rThreadIndex) {
        final String threadName = "Workload reader thread " + rThreadIndex;
        futures.add(ecs.submit(() -> {
          Thread.currentThread().setName(threadName);
          LOG.info("Thread starting: {}", threadName);
          while (!stop.get()) {
            try {
              String keyType = genRandomKeyType();
              String keyId = genRandomKeyId();
              int lockIndex = keyAndTypeToLockIndex(keyId, keyType);
              locks.get(lockIndex).lock();
              try {
                String selectPrimaryRow = String.format(
                    "SELECT key_value FROM my_keyspace.my_mapping_table " +
                        "WHERE key_id = '%s' AND key_type = '%s'",
                    keyId, keyType);
                ResultSet primaryRowResult = session.execute(selectPrimaryRow);
                List<Row> rows = primaryRowResult.all();
                if (rows.isEmpty()) {
                  numReadsRowNotFound.incrementAndGet();
                } else {
                  assertEquals(1, rows.size());
                  numReadsRowFound.incrementAndGet();
                  String keyValue = rows.get(0).getString(0);
                  String selectFromIndex = String.format(
                      "SELECT key_id, key_value, key_type " +
                          "FROM my_keyspace.my_mapping_table " +
                          "WHERE key_value = '%s' AND key_type = '%s'",
                      keyValue, keyType);
                  ResultSet indexResult = session.execute(selectFromIndex);
                  List<Row> indexRows = indexResult.all();
                  assertEquals(1, indexRows.size());
                  assertEquals(keyId, indexRows.get(0).getString(0));
                  numReadsRowAndIndexFound.incrementAndGet();
                }
              } finally {
                locks.get(lockIndex).unlock();;
              }
            } catch (Exception ex) {
              LOG.error("Exception in: {}", threadName, ex);
              stop.set(true);
              failed.set(true);
              break;
            }

          }
          return null;
        }));
      }

      LOG.info("Workload started");
      long WORKLOAD_TIME_MS = 180000;
      long startTimeMs = System.currentTimeMillis();
      while (!stop.get() && System.currentTimeMillis() < startTimeMs + WORKLOAD_TIME_MS) {
        Thread.sleep(500);
      }
      LOG.info("Workload finishing after " + (System.currentTimeMillis() - startTimeMs) + " ms");
      stop.set(true);
      for (Future<Void> future : futures) {
        future.get();
      }
      LOG.info(String.format(
          "Workload stopped. Total workload time: %.1f sec",
          (System.currentTimeMillis() - startTimeMs) / 1000.0
      ));

      LOG.info("Number of insert transaction attempts: " + numInsertTxnAttempts.get());
      LOG.info("Number of insert transaction successes: " + numInsertTxnSuccesses.get());
      LOG.info("Number of deletion attempts: " + numDeletionAttempts.get());
      LOG.info("Number of deletion successes: " + numDeletionSuccesses.get());
      LOG.info("Number of reads where the row is not found: " + numReadsRowNotFound.get());
      LOG.info("Number of reads where the row is found: " + numReadsRowFound.get());
      LOG.info("Number of reads where row and index entry are found: " +
          numReadsRowAndIndexFound.get());
      assertFalse(failed.get());
    }
  }

  // INSERT INTO my_mapping_table(k,v) values (1,10);
  // BEGIN TRANSACTION;
  // INSERT INTO my_mapping_table(k,v) values (1,11); -- insert 11, delete 10
  // INSERT INTO my_mapping_table(k,v) values (2,10); -- insert 10
  // END TRANSACTION;

  @Test
  public void testCqlIndexConsistency2() throws Exception {
    int failedTxnsCnt = 0;
    int successCnt = 0;
    session.execute("CREATE KEYSPACE my_keyspace;");
    session.execute("USE my_keyspace;");
    Random rand = new Random(); //instance of random class

    while (successCnt + failedTxnsCnt < 20) {
      session.execute(
        "CREATE TABLE my_mapping_table(k int, v int, PRIMARY KEY (k)" +
        ") WITH transactions = { 'enabled' : true };");
      session.execute("CREATE UNIQUE INDEX my_index ON my_mapping_table (k, v);");

      int k1 = rand.nextInt(100_000_000);
      int k2 = rand.nextInt(100_000_000);
      session.execute(String.format("INSERT INTO my_mapping_table(k,v) values (%s,10);", k1));
      StringBuilder sb = new StringBuilder();
      sb.append("BEGIN TRANSACTION ");
      while (k1 == k2) {
        k1 = rand.nextInt(100_000_000);
        k2 = rand.nextInt(100_000_000);
      } 
      sb.append(String.format("INSERT INTO my_mapping_table(k,v) values (%s,11);", k1)); // -- insert 11, delete 10
      sb.append(String.format("INSERT INTO my_mapping_table(k,v) values (%s,10);", k2)); // -- insert 10
      sb.append("END TRANSACTION;");
      try {
        session.execute(sb.toString());
      } catch (Exception ex) {
        failedTxnsCnt++;
        if ((failedTxnsCnt % 10) == 0)
          LOG.info("failedTxnsCnt=" + failedTxnsCnt);
        continue;
      }
      assertQueryRowsUnorderedWithoutDups("select k, v from my_mapping_table",
        "Row[" + k1 + ", 11" + "]",
        "Row[" + k2 + ", 10" + "]");
      assertQueryRowsUnorderedWithoutDups("select \"C$_k\", \"C$_v\" from my_index",
        "Row[" + k1 + ", 11" + "]",
        "Row[" + k2 + ", 10" + "]");
      successCnt++;
      if ((successCnt % 10) == 0)
        LOG.info("successCnt=" + successCnt);

      session.execute("DROP TABLE my_mapping_table");
    }
  }
}
