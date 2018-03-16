/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.registry.impl.ZkRegistryBase;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.server.HS2ActivePassiveHARegistry;
import org.apache.hive.service.server.HS2ActivePassiveHARegistryClient;
import org.apache.hive.service.server.HiveServer2Instance;
import org.apache.hive.service.servlet.HS2Peers;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestActivePassiveHA {
  private MiniHS2 miniHS2_1 = null;
  private MiniHS2 miniHS2_2 = null;
  private static TestingServer zkServer;
  private Connection hs2Conn = null;
  private static String zkHANamespace = "hs2ActivePassiveHATest";
  private HiveConf hiveConf1;
  private HiveConf hiveConf2;
  private static Path kvDataFilePath;

  @BeforeClass
  public static void beforeTest() throws Exception {
    MiniHS2.cleanupLocalDir();
    zkServer = new TestingServer();
    Class.forName(MiniHS2.getJdbcDriverName());
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (zkServer != null) {
      zkServer.close();
      zkServer = null;
    }
    MiniHS2.cleanupLocalDir();
  }

  @Before
  public void setUp() throws Exception {
    hiveConf1 = new HiveConf();
    hiveConf1.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    // Set up zookeeper dynamic service discovery configs
    setHAConfigs(hiveConf1);
    miniHS2_1 = new MiniHS2.Builder().withConf(hiveConf1).cleanupLocalDirOnStartup(false).build();
    hiveConf2 = new HiveConf();
    hiveConf2.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    // Set up zookeeper dynamic service discovery configs
    setHAConfigs(hiveConf2);
    miniHS2_2 = new MiniHS2.Builder().withConf(hiveConf2).cleanupLocalDirOnStartup(false).build();
    final String dataFileDir = hiveConf1.get("test.data.files").replace('\\', '/').replace("c:", "");
    kvDataFilePath = new Path(dataFileDir, "kv1.txt");
  }

  @After
  public void tearDown() throws Exception {
    if (hs2Conn != null) {
      hs2Conn.close();
    }
    if ((miniHS2_1 != null) && miniHS2_1.isStarted()) {
      miniHS2_1.stop();
    }
    if ((miniHS2_2 != null) && miniHS2_2.isStarted()) {
      miniHS2_2.stop();
    }
  }

  private static void setHAConfigs(Configuration conf) {
    conf.setBoolean(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY.varname, true);
    conf.set(ConfVars.HIVE_ZOOKEEPER_QUORUM.varname, zkServer.getConnectString());
    conf.setBoolean(ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_ENABLE.varname, true);
    conf.set(ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_REGISTRY_NAMESPACE.varname, zkHANamespace);
    conf.setTimeDuration(ConfVars.HIVE_ZOOKEEPER_CONNECTION_TIMEOUT.varname, 2, TimeUnit.SECONDS);
    conf.setTimeDuration(ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME.varname, 100, TimeUnit.MILLISECONDS);
    conf.setInt(ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES.varname, 1);
  }

  @Test(timeout = 60000)
  public void testActivePassiveHA() throws Exception {
    String instanceId1 = UUID.randomUUID().toString();
    miniHS2_1.start(getConfOverlay(instanceId1));
    while (!miniHS2_1.isStarted()) {
      Thread.sleep(100);
    }

    String instanceId2 = UUID.randomUUID().toString();
    miniHS2_2.start(getConfOverlay(instanceId2));
    while (!miniHS2_2.isStarted()) {
      Thread.sleep(100);
    }

    assertEquals(true, miniHS2_1.isLeader());
    String url = "http://localhost:" + hiveConf1.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
    assertEquals("true", sendGet(url));

    assertEquals(false, miniHS2_2.isLeader());
    url = "http://localhost:" + hiveConf2.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
    assertEquals("false", sendGet(url));

    url = "http://localhost:" + hiveConf1.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/peers";
    String resp = sendGet(url);
    ObjectMapper objectMapper = new ObjectMapper();
    HS2Peers.HS2Instances hs2Peers = objectMapper.readValue(resp, HS2Peers.HS2Instances.class);
    int port1 = Integer.parseInt(hiveConf1.get(ConfVars.HIVE_SERVER2_THRIFT_PORT.varname));
    assertEquals(2, hs2Peers.getHiveServer2Instances().size());
    for (HiveServer2Instance hsi : hs2Peers.getHiveServer2Instances()) {
      if (hsi.getRpcPort() == port1 && hsi.getWorkerIdentity().equals(instanceId1)) {
        assertEquals(true, hsi.isLeader());
      } else {
        assertEquals(false, hsi.isLeader());
      }
    }

    Configuration conf = new Configuration();
    setHAConfigs(conf);
    HS2ActivePassiveHARegistry client = HS2ActivePassiveHARegistryClient.getClient(conf);
    List<HiveServer2Instance> hs2Instances = new ArrayList<>(client.getAll());
    assertEquals(2, hs2Instances.size());
    List<HiveServer2Instance> leaders = new ArrayList<>();
    List<HiveServer2Instance> standby = new ArrayList<>();
    for (HiveServer2Instance instance : hs2Instances) {
      if (instance.isLeader()) {
        leaders.add(instance);
      } else {
        standby.add(instance);
      }
    }
    assertEquals(1, leaders.size());
    assertEquals(1, standby.size());

    miniHS2_1.stop();

    while (!miniHS2_2.isStarted()) {
      Thread.sleep(100);
    }
    assertEquals(true, miniHS2_2.isLeader());
    url = "http://localhost:" + hiveConf2.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
    assertEquals("true", sendGet(url));

    while (client.getAll().size() != 1) {
      Thread.sleep(100);
    }

    client = HS2ActivePassiveHARegistryClient.getClient(conf);
    hs2Instances = new ArrayList<>(client.getAll());
    assertEquals(1, hs2Instances.size());
    leaders = new ArrayList<>();
    standby = new ArrayList<>();
    for (HiveServer2Instance instance : hs2Instances) {
      if (instance.isLeader()) {
        leaders.add(instance);
      } else {
        standby.add(instance);
      }
    }
    assertEquals(1, leaders.size());
    assertEquals(0, standby.size());

    url = "http://localhost:" + hiveConf2.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/peers";
    resp = sendGet(url);
    objectMapper = new ObjectMapper();
    hs2Peers = objectMapper.readValue(resp, HS2Peers.HS2Instances.class);
    int port2 = Integer.parseInt(hiveConf2.get(ConfVars.HIVE_SERVER2_THRIFT_PORT.varname));
    assertEquals(1, hs2Peers.getHiveServer2Instances().size());
    for (HiveServer2Instance hsi : hs2Peers.getHiveServer2Instances()) {
      if (hsi.getRpcPort() == port2 && hsi.getWorkerIdentity().equals(instanceId2)) {
        assertEquals(true, hsi.isLeader());
      } else {
        assertEquals(false, hsi.isLeader());
      }
    }

    // start 1st server again
    instanceId1 = UUID.randomUUID().toString();
    miniHS2_1.start(getConfOverlay(instanceId1));

    while (!miniHS2_1.isStarted()) {
      Thread.sleep(100);
    }
    assertEquals(false, miniHS2_1.isLeader());
    url = "http://localhost:" + hiveConf1.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
    assertEquals("false", sendGet(url));

    while (client.getAll().size() != 2) {
      Thread.sleep(100);
    }

    client = HS2ActivePassiveHARegistryClient.getClient(conf);
    hs2Instances = new ArrayList<>(client.getAll());
    assertEquals(2, hs2Instances.size());
    leaders = new ArrayList<>();
    standby = new ArrayList<>();
    for (HiveServer2Instance instance : hs2Instances) {
      if (instance.isLeader()) {
        leaders.add(instance);
      } else {
        standby.add(instance);
      }
    }
    assertEquals(1, leaders.size());
    assertEquals(1, standby.size());

    url = "http://localhost:" + hiveConf1.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/peers";
    resp = sendGet(url);
    objectMapper = new ObjectMapper();
    hs2Peers = objectMapper.readValue(resp, HS2Peers.HS2Instances.class);
    port2 = Integer.parseInt(hiveConf2.get(ConfVars.HIVE_SERVER2_THRIFT_PORT.varname));
    assertEquals(2, hs2Peers.getHiveServer2Instances().size());
    for (HiveServer2Instance hsi : hs2Peers.getHiveServer2Instances()) {
      if (hsi.getRpcPort() == port2 && hsi.getWorkerIdentity().equals(instanceId2)) {
        assertEquals(true, hsi.isLeader());
      } else {
        assertEquals(false, hsi.isLeader());
      }
    }
  }

  @Test(timeout = 60000)
  public void testConnectionActivePassiveHAServiceDiscovery() throws Exception {
    String instanceId1 = UUID.randomUUID().toString();
    miniHS2_1.start(getConfOverlay(instanceId1));
    while (!miniHS2_1.isStarted()) {
      Thread.sleep(100);
    }
    String instanceId2 = UUID.randomUUID().toString();
    Map<String, String> confOverlay = getConfOverlay(instanceId2);
    confOverlay.put(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, "http");
    confOverlay.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname, "clidriverTest");
    miniHS2_2.start(confOverlay);
    while (!miniHS2_2.isStarted()) {
      Thread.sleep(100);
    }

    assertEquals(true, miniHS2_1.isLeader());
    String url = "http://localhost:" + hiveConf1.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
    assertEquals("true", sendGet(url));

    assertEquals(false, miniHS2_2.isLeader());
    url = "http://localhost:" + hiveConf2.get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname) + "/leader";
    assertEquals("false", sendGet(url));

    // miniHS2_1 will be leader
    String zkConnectString = zkServer.getConnectString();
    String zkJdbcUrl = miniHS2_1.getJdbcURL();
    // getAllUrls will parse zkJdbcUrl and will plugin the active HS2's host:port
    String parsedUrl = HiveConnection.getAllUrls(zkJdbcUrl).get(0).getJdbcUriString();
    final String serviceDiscoveryMode = "zooKeeperHA";
    String hs2_1_directUrl = "jdbc:hive2://" + miniHS2_1.getHost() + ":" + miniHS2_1.getBinaryPort() +
      "/default;serviceDiscoveryMode=" + serviceDiscoveryMode + ";zooKeeperNamespace=" + zkHANamespace + ";";
    assertTrue(zkJdbcUrl.contains(zkConnectString));
    assertEquals(hs2_1_directUrl, parsedUrl);
    openConnectionAndRunQuery(zkJdbcUrl);

    // miniHS2_2 will become leader
    miniHS2_1.stop();
    parsedUrl = HiveConnection.getAllUrls(zkJdbcUrl).get(0).getJdbcUriString();
    String hs2_2_directUrl = "jdbc:hive2://" + miniHS2_2.getHost() + ":" + miniHS2_2.getHttpPort() +
      "/default;serviceDiscoveryMode=" + serviceDiscoveryMode + ";zooKeeperNamespace=" + zkHANamespace + ";";
    assertTrue(zkJdbcUrl.contains(zkConnectString));
    assertEquals(hs2_2_directUrl, parsedUrl);
    openConnectionAndRunQuery(zkJdbcUrl);

    // miniHS2_2 will continue to be leader
    instanceId1 = UUID.randomUUID().toString();
    miniHS2_1.start(getConfOverlay(instanceId1));
    parsedUrl = HiveConnection.getAllUrls(zkJdbcUrl).get(0).getJdbcUriString();
    assertTrue(zkJdbcUrl.contains(zkConnectString));
    assertEquals(hs2_2_directUrl, parsedUrl);
    openConnectionAndRunQuery(zkJdbcUrl);

    // miniHS2_1 will become leader
    miniHS2_2.stop();
    parsedUrl = HiveConnection.getAllUrls(zkJdbcUrl).get(0).getJdbcUriString();
    hs2_1_directUrl = "jdbc:hive2://" + miniHS2_1.getHost() + ":" + miniHS2_1.getBinaryPort() +
      "/default;serviceDiscoveryMode=" + serviceDiscoveryMode + ";zooKeeperNamespace=" + zkHANamespace + ";";
    assertTrue(zkJdbcUrl.contains(zkConnectString));
    assertEquals(hs2_1_directUrl, parsedUrl);
    openConnectionAndRunQuery(zkJdbcUrl);
  }

  private Connection getConnection(String jdbcURL, String user) throws SQLException {
    return DriverManager.getConnection(jdbcURL, user, "bar");
  }

  private void openConnectionAndRunQuery(String jdbcUrl) throws Exception {
    hs2Conn = getConnection(jdbcUrl, System.getProperty("user.name"));
    String tableName = "testTab1";
    Statement stmt = hs2Conn.createStatement();
    // create table
    stmt.execute("DROP TABLE IF EXISTS " + tableName);
    stmt.execute("CREATE TABLE " + tableName
      + " (under_col INT COMMENT 'the under column', value STRING) COMMENT ' test table'");
    // load data
    stmt.execute("load data local inpath '" + kvDataFilePath.toString() + "' into table "
      + tableName);
    ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
    assertTrue(res.next());
    assertEquals("val_238", res.getString(2));
    res.close();
    stmt.close();
  }

  private String sendGet(String url) throws Exception {
    URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("GET");
    BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
    String inputLine;
    StringBuilder response = new StringBuilder();
    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }
    in.close();
    return response.toString();
  }

  private Map<String,String> getConfOverlay(final String instanceId) {
    Map<String, String> confOverlay = new HashMap<>();
    confOverlay.put("hive.server2.zookeeper.publish.configs", "true");
    confOverlay.put(ZkRegistryBase.UNIQUE_IDENTIFIER, instanceId);
    return confOverlay;
  }
}