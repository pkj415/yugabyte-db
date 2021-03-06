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

import java.util.Collections;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.yb.AssertionWrappers;
import org.yb.YBTestRunner;
import org.yb.minicluster.BaseMiniClusterTest;
import org.yb.minicluster.MiniYBCluster;
import org.yb.minicluster.MiniYBDaemon;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.google.common.net.HostAndPort;

@RunWith(value=YBTestRunner.class)
public class TestBasicStatements extends BaseCQLTest {
  @Test
  public void testCreateTable() throws Exception {
    LOG.info("Create table ...");
    session.execute("CREATE TABLE human_resource1(id int primary key, name varchar);");
  }

  // We need to work on reporting error from SQL before activating this test.
  @Test
  public void testInvalidStatement() throws Exception {
    LOG.info("Execute nothing ...");
    thrown.expect(com.datastax.driver.core.exceptions.SyntaxError.class);
    thrown.expectMessage("Invalid SQL Statement");
    session.execute("NOTHING");
  }

  @Test
  public void testUnsupportedProtocol() throws Exception {
    thrown.expect(com.datastax.driver.core.exceptions.UnsupportedProtocolVersionException.class);
    Session s = getDefaultClusterBuilder()
                .allowBetaProtocolVersion()
                .build()
                .connect();
  }

  @Test
  public void testCQLTimesOut() throws Exception {
    // Set a smaller timeout for this test, so that we don't have to wait for
    // 120s.
    destroyMiniCluster();
    BaseMiniClusterTest.tserverArgs.removeIf(
        opt -> opt.startsWith("--client_read_write_timeout_ms="));
    BaseMiniClusterTest.tserverArgs.add("--client_read_write_timeout_ms=10000");
    createMiniCluster();
    setUpCqlClient();

    LOG.info("Creating table ...");
    session.execute("CREATE TABLE test(id int primary key, name varchar);");
    LOG.info("Inserting one row into table ...");
    session.execute("INSERT into test (id, name) values (1, 'foo');");
    assertQuery(new SimpleStatement("select id, name from test;", true),
                "Row[1, foo]");

    // Kill 2 tablet servers.
    LOG.info("Killing 2 of the 3 TServers ...");
    Map<HostAndPort, MiniYBDaemon> tservers = miniCluster.getTabletServers();
    AssertionWrappers.assertTrue(tservers.size() == 3);
    int numKilled = 0;
    for (HostAndPort entry : tservers.keySet()) {
      Process ts = tservers.get(entry).getProcess();
      miniCluster.killTabletServerOnHostPort(entry);
      LOG.info("Waiting for exit");
      ts.waitFor();
      LOG.info("Done waiting for.");

      ++numKilled;
      LOG.info("Killed " + numKilled + " tserver processes.");
      if (numKilled == 2) {
        break;
      }
    }

    // Wait for the cassandra client to find out about the dead nodes.
    Thread.sleep(MiniYBCluster.CQL_NODE_LIST_REFRESH_SECS * 1000);

    LOG.info("Try to insert again. But expect it to fail.");
    // Expect to see a timeout since the operation cannot complete.
    try {
      session.execute("INSERT into test (id, name) values (2, 'bar');");
      AssertionWrappers.fail("Expected not to get here");
    } catch (
        com.datastax.driver.core.exceptions.OperationTimedOutException oe) {
      LOG.info("Caught execption ", oe);
      AssertionWrappers.fail("Not expecting a client side timeout.");
    } catch (RuntimeException re) {
      LOG.info("Caught execption ", re);
      AssertionWrappers.assertTrue(re.getMessage().contains("passed its deadline"));
    }

    // destroy the cluster, without trying to clean up the tables etc.
    destroyMiniCluster();
  }
}
