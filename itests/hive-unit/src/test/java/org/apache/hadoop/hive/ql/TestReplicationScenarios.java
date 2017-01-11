/**
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
package org.apache.hadoop.hive.ql;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSemanticAnalyzer;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.Shell;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestReplicationScenarios {

  final static String DBNOTIF_LISTENER_CLASSNAME = "org.apache.hive.hcatalog.listener.DbNotificationListener";
      // FIXME : replace with hive copy once that is copied
  final static String tid =
      TestReplicationScenarios.class.getCanonicalName().replace('.','_') + "_" + System.currentTimeMillis();
  final static String TEST_PATH = System.getProperty("test.warehouse.dir","/tmp") + Path.SEPARATOR + tid;

  static HiveConf hconf;
  static boolean useExternalMS = false;
  static int msPort;
  static Driver driver;
  static HiveMetaStoreClient metaStoreClient;

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationScenarios.class);
  private ArrayList<String> lastResults;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    hconf = new HiveConf(TestReplicationScenarios.class);
    String metastoreUri = System.getProperty("test."+HiveConf.ConfVars.METASTOREURIS.varname);
    if (metastoreUri != null) {
      hconf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
      useExternalMS = true;
      return;
    }
    if (Shell.WINDOWS) {
      WindowsPathUtil.convertPathsFromWindowsToHdfs(hconf);
    }

    System.setProperty(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS.varname,
        DBNOTIF_LISTENER_CLASSNAME); // turn on db notification listener on metastore
    msPort = MetaStoreUtils.startMetaStore();
    hconf.setVar(HiveConf.ConfVars.REPLDIR,TEST_PATH + "/hrepl/");
    hconf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
        + msPort);
    hconf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hconf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hconf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hconf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname,
        "false");
    System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");

    Path testPath = new Path(TEST_PATH);
    FileSystem fs = FileSystem.get(testPath.toUri(),hconf);
    fs.mkdirs(testPath);

    driver = new Driver(hconf);
    SessionState.start(new CliSessionState(hconf));
    metaStoreClient = new HiveMetaStoreClient(hconf);
  }

  @AfterClass
  public static void tearDownAfterClass(){
    // FIXME : should clean up TEST_PATH, but not doing it now, for debugging's sake
  }

  @Before
  public void setUp(){
    // before each test
  }

  @After
  public void tearDown(){
    // after each test
  }

  private static  int next = 0;
  private synchronized void advanceDumpDir() {
    next++;
    ReplicationSemanticAnalyzer.injectNextDumpDirForTest(String.valueOf(next));
  }

  /**
   * Tests basic operation - creates a db, with 4 tables, 2 ptned and 2 unptned.
   * Inserts data into one of the ptned tables, and one of the unptned tables,
   * and verifies that a REPL DUMP followed by a REPL LOAD is able to load it
   * appropriately.
   */
  @Test
  public void testBasic() throws IOException {

    String testName = "basic";
    LOG.info("Testing "+testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE");

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String unptn_locn = new Path(TEST_PATH , testName + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH , testName + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH , testName + "_ptn2").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned");
    run("SELECT * from " + dbName + ".unptned");
    verifyResults(unptn_data);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)");
    run("SELECT a from " + dbName + ".ptned WHERE b=1");
    verifyResults(ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)");
    run("SELECT a from " + dbName + ".ptned WHERE b=2");
    verifyResults(ptn_data_2);
    run("SELECT a from " + dbName + ".ptned_empty");
    verifyResults(empty);
    run("SELECT * from " + dbName + ".unptned_empty");
    verifyResults(empty);

    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String replDumpLocn = getResult(0,0);
    String replDumpId = getResult(0,1,true);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");

    run("REPL STATUS " + dbName + "_dupe");
    verifyResults(new String[] {replDumpId});

    run("SELECT * from " + dbName + "_dupe.unptned");
    verifyResults(unptn_data);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b=1");
    verifyResults(ptn_data_1);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b=2");
    verifyResults(ptn_data_2);
    run("SELECT a from " + dbName + ".ptned_empty");
    verifyResults(empty);
    run("SELECT * from " + dbName + ".unptned_empty");
    verifyResults(empty);
  }

  @Test
  public void testIncrementalAdds() throws IOException {
    String testName = "incrementalAdds";
    LOG.info("Testing "+testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE");

    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String replDumpLocn = getResult(0,0);
    String replDumpId = getResult(0,1,true);
    LOG.info("Dumped to {} with id {}",replDumpLocn,replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String unptn_locn = new Path(TEST_PATH , testName + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH , testName + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH , testName + "_ptn2").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    run("SELECT a from " + dbName + ".ptned_empty");
    verifyResults(empty);
    run("SELECT * from " + dbName + ".unptned_empty");
    verifyResults(empty);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned");
    run("SELECT * from " + dbName + ".unptned");
    verifyResults(unptn_data);
    run("CREATE TABLE " + dbName + ".unptned_late AS SELECT * from " + dbName + ".unptned");
    run("SELECT * from " + dbName + ".unptned_late");
    verifyResults(unptn_data);


    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)");
    run("SELECT a from " + dbName + ".ptned WHERE b=1");
    verifyResults(ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)");
    run("SELECT a from " + dbName + ".ptned WHERE b=2");
    verifyResults(ptn_data_2);

    run("CREATE TABLE " + dbName + ".ptned_late(a string) PARTITIONED BY (b int) STORED AS TEXTFILE");
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned_late PARTITION(b=1)");
    run("SELECT a from " + dbName + ".ptned_late WHERE b=1");
    verifyResults(ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned_late PARTITION(b=2)");
    run("SELECT a from " + dbName + ".ptned_late WHERE b=2");
    verifyResults(ptn_data_2);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId );
    String incrementalDumpLocn = getResult(0,0);
    String incrementalDumpId = getResult(0,1,true);
    LOG.info("Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '"+incrementalDumpLocn+"'");

    run("REPL STATUS " + dbName + "_dupe");
//    verifyResults(new String[] {incrementalDumpId});
    // TODO: this will currently not work because we need to add in ALTER_DB support into this
    // and queue in a dummy ALTER_DB to update the repl.last.id on the last event of every
    // incremental dump. Currently, the dump id fetched will be the last dump id at the time
    // the db was created from the bootstrap export dump

    run("SELECT * from " + dbName + "_dupe.unptned_empty");
    verifyResults(empty);
    run("SELECT a from " + dbName + ".ptned_empty");
    verifyResults(empty);


//    run("SELECT * from " + dbName + "_dupe.unptned");
//    verifyResults(unptn_data);
    // TODO :this does not work because LOAD DATA LOCAL INPATH into an unptned table seems
    // to use ALTER_TABLE only - it does not emit an INSERT or CREATE - re-enable after
    // fixing that.
    run("SELECT * from " + dbName + "_dupe.unptned_late");
    verifyResults(unptn_data);


    run("SELECT a from " + dbName + "_dupe.ptned WHERE b=1");
    verifyResults(ptn_data_1);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b=2");
    verifyResults(ptn_data_2);

    // verified up to here.
    run("SELECT a from " + dbName + "_dupe.ptned_late WHERE b=1");
    verifyResults(ptn_data_1);
    run("SELECT a from " + dbName + "_dupe.ptned_late WHERE b=2");
    verifyResults(ptn_data_2);

  }

  @Test
  public void testDrops() throws IOException {

    String testName = "drops";
    LOG.info("Testing "+testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned2(a string) partitioned by (b string) STORED AS TEXTFILE");

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String unptn_locn = new Path(TEST_PATH , testName + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH , testName + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH , testName + "_ptn2").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned");
    run("SELECT * from " + dbName + ".unptned");
    verifyResults(unptn_data);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='1')");
    run("SELECT a from " + dbName + ".ptned WHERE b='1'");
    verifyResults(ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='2')");
    run("SELECT a from " + dbName + ".ptned WHERE b='2'");
    verifyResults(ptn_data_2);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='1')");
    run("SELECT a from " + dbName + ".ptned2 WHERE b='1'");
    verifyResults(ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='2')");
    run("SELECT a from " + dbName + ".ptned2 WHERE b='2'");
    verifyResults(ptn_data_2);

    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String replDumpLocn = getResult(0,0);
    String replDumpId = getResult(0,1,true);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");

    run("REPL STATUS " + dbName + "_dupe");
    verifyResults(new String[] {replDumpId});

    run("SELECT * from " + dbName + "_dupe.unptned");
    verifyResults(unptn_data);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b='1'");
    verifyResults(ptn_data_1);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b='2'");
    verifyResults(ptn_data_2);
    run("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='1'");
    verifyResults(ptn_data_1);
    run("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='2'");
    verifyResults(ptn_data_2);

    run("DROP TABLE " + dbName + ".unptned");
    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION (b='2')");
    run("DROP TABLE " + dbName + ".ptned2");
    run("SELECT a from " + dbName + ".ptned WHERE b=2");
    verifyResults(empty);
    run("SELECT a from " + dbName + ".ptned");
    verifyResults(ptn_data_1);

    advanceDumpDir();;
    run("REPL DUMP " + dbName + " FROM " + replDumpId);
    String postDropReplDumpLocn = getResult(0,0);
    String postDropReplDumpId = getResult(0,1,true);
    LOG.info("Dumped to {} with id {}->{}", postDropReplDumpLocn, replDumpId, postDropReplDumpId);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + postDropReplDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + postDropReplDumpLocn + "'");

    Exception e = null;
    try {
      Table tbl = metaStoreClient.getTable(dbName + "_dupe", "unptned");
      assertNull(tbl);
    } catch (TException te) {
      e = te;
    }
    assertNotNull(e);
    assertEquals(NoSuchObjectException.class, e.getClass());

    run("SELECT a from " + dbName + "_dupe.ptned WHERE b=2");
    verifyResults(empty);
    run("SELECT a from " + dbName + "_dupe.ptned");
    verifyResults(ptn_data_1);

    Exception e2 = null;
    try {
      Table tbl = metaStoreClient.getTable(dbName+"_dupe","ptned2");
      assertNull(tbl);
    } catch (TException te) {
      e2 = te;
    }
    assertNotNull(e2);
    assertEquals(NoSuchObjectException.class, e.getClass());

  }


  @Test
  public void testAlters() throws IOException {

    String testName = "alters";
    LOG.info("Testing "+testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".unptned2(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned2(a string) partitioned by (b string) STORED AS TEXTFILE");

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String unptn_locn = new Path(TEST_PATH , testName + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH , testName + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH , testName + "_ptn2").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned");
    run("SELECT * from " + dbName + ".unptned");
    verifyResults(unptn_data);
    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned2");
    run("SELECT * from " + dbName + ".unptned2");
    verifyResults(unptn_data);

    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='1')");
    run("SELECT a from " + dbName + ".ptned WHERE b='1'");
    verifyResults(ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='2')");
    run("SELECT a from " + dbName + ".ptned WHERE b='2'");
    verifyResults(ptn_data_2);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='1')");
    run("SELECT a from " + dbName + ".ptned2 WHERE b='1'");
    verifyResults(ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='2')");
    run("SELECT a from " + dbName + ".ptned2 WHERE b='2'");
    verifyResults(ptn_data_2);

    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String replDumpLocn = getResult(0,0);
    String replDumpId = getResult(0,1,true);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");

    run("REPL STATUS " + dbName + "_dupe");
    verifyResults(new String[] {replDumpId});

    run("SELECT * from " + dbName + "_dupe.unptned");
    verifyResults(unptn_data);
    run("SELECT * from " + dbName + "_dupe.unptned2");
    verifyResults(unptn_data);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b='1'");
    verifyResults(ptn_data_1);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b='2'");
    verifyResults(ptn_data_2);
    run("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='1'");
    verifyResults(ptn_data_1);
    run("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='2'");
    verifyResults(ptn_data_2);

    run("ALTER TABLE " + dbName + ".unptned RENAME TO " + dbName + ".unptned_rn");
    run("SELECT * from " + dbName + ".unptned_rn");
    verifyResults(unptn_data);

    String testKey = "blah";
    String testVal = "foo";
    run("ALTER TABLE " + dbName + ".unptned2 SET TBLPROPERTIES ('" + testKey + "' = '" + testVal + "')");
    try {
      Table unptn2 = metaStoreClient.getTable(dbName,"unptned2");
      assertTrue(unptn2.getParameters().containsKey(testKey));
      assertEquals(testVal,unptn2.getParameters().get(testKey));
    } catch (TException e) {
      assertNull(e);
    }

    run("ALTER TABLE " + dbName + ".ptned PARTITION (b='2') RENAME TO PARTITION (b='22')");
    run("SELECT a from " + dbName + ".ptned WHERE b=2");
    verifyResults(empty);
    run("SELECT a from " + dbName + ".ptned WHERE b=22");
    verifyResults(ptn_data_2);

    run("ALTER TABLE " + dbName + ".ptned SET TBLPROPERTIES ('" + testKey + "' = '" + testVal + "')");
    try {
      Table ptned = metaStoreClient.getTable(dbName,"ptned");
      assertTrue(ptned.getParameters().containsKey(testKey));
      assertEquals(testVal,ptned.getParameters().get(testKey));
    } catch (TException e) {
      assertNull(e);
    }

    // No DDL way to alter a partition, so we use the MSC api directly.
    try {
      List<String> ptnVals1 = new ArrayList<String>();
      ptnVals1.add("1");
      Partition ptn1 = metaStoreClient.getPartition(dbName, "ptned", ptnVals1);
      ptn1.getParameters().put(testKey,testVal);
      metaStoreClient.alter_partition(dbName,"ptned",ptn1,null);
    } catch (TException e) {
      assertNull(e);
    }


    run("SELECT a from " + dbName + ".ptned2 WHERE b=2");
    verifyResults(ptn_data_2);
    run("ALTER TABLE " + dbName + ".ptned2 RENAME TO " + dbName + ".ptned2_rn");
    run("SELECT a from " + dbName + ".ptned2_rn WHERE b=2");
    verifyResults(ptn_data_2);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId);
    String postAlterReplDumpLocn = getResult(0,0);
    String postAlterReplDumpId = getResult(0,1,true);
    LOG.info("Dumped to {} with id {}->{}", postAlterReplDumpLocn, replDumpId, postAlterReplDumpId);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + postAlterReplDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + postAlterReplDumpLocn + "'");

    Exception e = null;
    try {
      Table tbl = metaStoreClient.getTable(dbName + "_dupe" , "unptned");
      assertNull(tbl);
    } catch (TException te) {
      e = te;
    }
    assertNotNull(e);
    assertEquals(NoSuchObjectException.class, e.getClass());

    try {
      Table unptn2 = metaStoreClient.getTable(dbName + "_dupe" , "unptned2");
      assertTrue(unptn2.getParameters().containsKey(testKey));
      assertEquals(testVal,unptn2.getParameters().get(testKey));
    } catch (TException te) {
      assertNull(te);
    }

    run("SELECT * from " + dbName + "_dupe.unptned_rn");
    verifyResults(unptn_data);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b=2");
    verifyResults(empty);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b=22");
    verifyResults(ptn_data_2);

    Exception e2 = null;
    try {
      Table tbl = metaStoreClient.getTable(dbName + "_dupe" , "ptned2");
      assertNull(tbl);
    } catch (TException te) {
      e2 = te;
    }
    assertNotNull(e2);
    assertEquals(NoSuchObjectException.class, e.getClass());

    try {
      Table ptned = metaStoreClient.getTable(dbName + "_dupe" , "ptned");
      assertTrue(ptned.getParameters().containsKey(testKey));
      assertEquals(testVal,ptned.getParameters().get(testKey));
    } catch (TException te) {
      assertNull(te);
    }

    try {
      List<String> ptnVals1 = new ArrayList<String>();
      ptnVals1.add("1");
      Partition ptn1 = metaStoreClient.getPartition(dbName + "_dupe", "ptned", ptnVals1);
      assertTrue(ptn1.getParameters().containsKey(testKey));
      assertEquals(testVal,ptn1.getParameters().get(testKey));
    } catch (TException te) {
      assertNull(te);
    }

    run("SELECT a from " + dbName + "_dupe.ptned2_rn WHERE b=2");
    verifyResults(ptn_data_2);
  }

  @Test
  public void testIncrementalInserts() throws IOException {
    String testName = "incrementalInserts";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName
        + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE");

    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String replDumpLocn = getResult(0, 0);
    String replDumpId = getResult(0, 1, true);
    LOG.info("Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");

    String[] unptn_data = new String[] { "eleven", "twelve" };
    String[] ptn_data_1 = new String[] { "thirteen", "fourteen", "fifteen" };
    String[] ptn_data_2 = new String[] { "fifteen", "sixteen", "seventeen" };
    String[] empty = new String[] {};

    String unptn_locn = new Path(TEST_PATH, testName + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH, testName + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH, testName + "_ptn2").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    run("SELECT a from " + dbName + ".ptned_empty");
    verifyResults(empty);
    run("SELECT * from " + dbName + ".unptned_empty");
    verifyResults(empty);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned");
    run("SELECT * from " + dbName + ".unptned");
    verifyResults(unptn_data);
    run("CREATE TABLE " + dbName + ".unptned_late LIKE " + dbName + ".unptned");
    run("INSERT INTO TABLE " + dbName + ".unptned_late SELECT * FROM " + dbName + ".unptned");
    run("SELECT * from " + dbName + ".unptned_late");
    verifyResults(unptn_data);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId);
    String incrementalDumpLocn = getResult(0, 0);
    String incrementalDumpId = getResult(0, 1, true);
    LOG.info("Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");
    run("SELECT * from " + dbName + "_dupe.unptned_late");
    verifyResults(unptn_data);

    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName
        + ".ptned PARTITION(b=1)");
    run("SELECT a from " + dbName + ".ptned WHERE b=1");
    verifyResults(ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName
        + ".ptned PARTITION(b=2)");
    run("SELECT a from " + dbName + ".ptned WHERE b=2");
    verifyResults(ptn_data_2);

    run("CREATE TABLE " + dbName
        + ".ptned_late(a string) PARTITIONED BY (b int) STORED AS TEXTFILE");
    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=1) SELECT a FROM " + dbName
        + ".ptned WHERE b=1");
    run("SELECT a from " + dbName + ".ptned_late WHERE b=1");
    verifyResults(ptn_data_1);

    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=2) SELECT a FROM " + dbName
        + ".ptned WHERE b=2");
    run("SELECT a from " + dbName + ".ptned_late WHERE b=2");
    verifyResults(ptn_data_2);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId);
    incrementalDumpLocn = getResult(0, 0);
    incrementalDumpId = getResult(0, 1, true);
    LOG.info("Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");
    run("SELECT a from " + dbName + "_dupe.ptned_late WHERE b=1");
    verifyResults(ptn_data_1);
    run("SELECT a from " + dbName + "_dupe.ptned_late WHERE b=2");
    verifyResults(ptn_data_2);
  }

  private String getResult(int rowNum, int colNum) throws IOException {
    return getResult(rowNum,colNum,false);
  }
  private String getResult(int rowNum, int colNum, boolean reuse) throws IOException {
    if (!reuse) {
      lastResults = new ArrayList<String>();
      try {
        driver.getResults(lastResults);
      } catch (CommandNeedRetryException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    // Split around the 'tab' character
    return (lastResults.get(rowNum).split("\\t"))[colNum];
  }

  private void verifyResults(String[] data) throws IOException {
    List<String> results = getOutput();
    LOG.info("Expecting {}",data);
    LOG.info("Got {}",results);
    assertEquals(data.length,results.size());
    for (int i = 0; i < data.length; i++){
      assertEquals(data[i],results.get(i));
    }
  }

  private List<String> getOutput() throws IOException {
    List<String> results = new ArrayList<String>();
    try {
      driver.getResults(results);
    } catch (CommandNeedRetryException e) {
      LOG.warn(e.getMessage(),e);
      throw new RuntimeException(e);
    }
    return results;
  }

  private void printOutput() throws IOException {
    for (String s : getOutput()){
      LOG.info(s);
    }
  }

  private static void run(String cmd) throws RuntimeException {
    try {
    run(cmd,false); // default arg-less run simply runs, and does not care about failure
    } catch (AssertionError ae){
      // Hive code has AssertionErrors in some cases - we want to record what happens
      LOG.warn("AssertionError:",ae);
      throw new RuntimeException(ae);
    }
  }

  private static boolean run(String cmd, boolean errorOnFail) throws RuntimeException {
    boolean success = false;
    try {
      CommandProcessorResponse ret = driver.run(cmd);
      success = (ret.getException() == null);
      if (!success){
        LOG.warn("Error {} : {} running [{}].", ret.getErrorCode(), ret.getErrorMessage(), cmd);
      }
    } catch (CommandNeedRetryException e) {
      if (errorOnFail){
        throw new RuntimeException(e);
      } else {
        LOG.warn(e.getMessage(),e);
        // do nothing else
      }
    }
    return success;
  }

  public static void createTestDataFile(String filename, String[] lines) throws IOException {
    FileWriter writer = null;
    try {
      File file = new File(filename);
      file.deleteOnExit();
      writer = new FileWriter(file);
      for (String line : lines) {
        writer.write(line + "\n");
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

}
