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

package org.apache.hadoop.hive.ql.exec.tez;


import static org.junit.Assert.*;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.*;

import org.apache.hadoop.hive.ql.exec.tez.UserPoolMapping.MappingInput;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;

import com.google.common.util.concurrent.SettableFuture;

import com.google.common.collect.Lists;
import java.lang.Thread.State;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestWorkloadManager {
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(TestWorkloadManager.class);

  private final class GetSessionRunnable implements Runnable {
    private final AtomicReference<WmTezSession> session;
    private final WorkloadManager wm;
    private final AtomicReference<Throwable> error;
    private final HiveConf conf;
    private final CountDownLatch cdl;
    private final String userName;

    private GetSessionRunnable(AtomicReference<WmTezSession> session, WorkloadManager wm,
        AtomicReference<Throwable> error, HiveConf conf, CountDownLatch cdl, String userName) {
      this.session = session;
      this.wm = wm;
      this.error = error;
      this.conf = conf;
      this.cdl = cdl;
      this.userName = userName;
    }

    @Override
    public void run() {
      WmTezSession old = session.get();
      session.set(null);
      if (cdl != null) {
        cdl.countDown();
      }
      try {
       session.set((WmTezSession) wm.getSession(old, new MappingInput(userName), conf));
      } catch (Throwable e) {
        error.compareAndSet(null, e);
      }
    }
  }

  public static class MockQam implements QueryAllocationManager {
    boolean isCalled = false;

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void updateSessionsAsync(Double totalMaxAlloc, List<WmTezSession> sessions) {
      isCalled = true;
    }

    void assertWasCalled() {
      assertTrue(isCalled);
      isCalled = false;
    }
  }

  public static WMResourcePlan plan() {
    return new WMResourcePlan("rp");
  }

  public static WMPool pool(String path, int qp, double alloc) {
    WMPool pool = new WMPool("rp", path);
    pool.setAllocFraction(alloc);
    pool.setQueryParallelism(qp);
    return pool;
  }

  public static WMMapping mapping(String user, String pool) {
    WMMapping mapping = new WMMapping("rp", "USER", user);
    mapping.setPoolName(pool);
    return mapping;
  }

  public static class WorkloadManagerForTest extends WorkloadManager {

    public WorkloadManagerForTest(String yarnQueue, HiveConf conf, int numSessions,
        QueryAllocationManager qam) {
      super(yarnQueue, conf, qam, createDummyPlan(numSessions));
    }

    public WorkloadManagerForTest(String yarnQueue, HiveConf conf,
        QueryAllocationManager qam, WMFullResourcePlan plan) {
      super(yarnQueue, conf, qam, plan);
    }

    private static WMFullResourcePlan createDummyPlan(int numSessions) {
      WMFullResourcePlan plan = new WMFullResourcePlan(new WMResourcePlan("rp"), 
          Lists.newArrayList(pool("llap", numSessions, 1.0f)));
      plan.getPlan().setDefaultPoolPath("llap");
      return plan;
    }

    @Override
    protected WmTezSession createSessionObject(String sessionId, HiveConf conf) {
      conf = conf == null ? new HiveConf(getConf()) : conf;
      return new SampleTezSessionState(sessionId, this, conf);
    }

    @Override
    public TezSessionState getSession(
        TezSessionState session, MappingInput input, HiveConf conf) throws Exception {
      // We want to wait for the iteration to finish and set the cluster fraction.
      TezSessionState state = super.getSession(session, input, conf);
      ensureWm();
      return state;
    }

    @Override
    public void destroy(TezSessionState session) throws Exception {
      super.destroy(session);
      ensureWm();
    }

    private void ensureWm() throws InterruptedException, ExecutionException {
      addTestEvent().get(); // Wait for the events to be processed.
    }

    @Override
    public void returnAfterUse(TezSessionPoolSession session) throws Exception {
      super.returnAfterUse(session);
      ensureWm();
    }

    @Override
    public TezSessionState reopen(
        TezSessionState session, Configuration conf, String[] additionalFiles) throws Exception {
      session = super.reopen(session, conf, additionalFiles);
      ensureWm();
      return session;
    }
  }

  @Test(timeout = 10000)
  public void testReuse() throws Exception {
    HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WorkloadManager wm = new WorkloadManagerForTest("test", conf, 1, qam);
    wm.start();
    TezSessionState nonPool = mock(TezSessionState.class);
    when(nonPool.getConf()).thenReturn(conf);
    doNothing().when(nonPool).close(anyBoolean());
    TezSessionState session = wm.getSession(nonPool, new MappingInput("user"), conf);
    verify(nonPool).close(anyBoolean());
    assertNotSame(nonPool, session);
    session.returnToSessionManager();
    TezSessionPoolSession diffPool = mock(TezSessionPoolSession.class);
    when(diffPool.getConf()).thenReturn(conf);
    doNothing().when(diffPool).returnToSessionManager();
    session = wm.getSession(diffPool, new MappingInput("user"), conf);
    verify(diffPool).returnToSessionManager();
    assertNotSame(diffPool, session);
    TezSessionState session2 = wm.getSession(session, new MappingInput("user"), conf);
    assertSame(session, session2);
  }

  @Test(timeout = 10000)
  public void testQueueName() throws Exception {
    HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WorkloadManager wm = new WorkloadManagerForTest("test", conf, 1, qam);
    wm.start();
    // The queue should be ignored.
    conf.set(TezConfiguration.TEZ_QUEUE_NAME, "test2");
    TezSessionState session = wm.getSession(null, new MappingInput("user"), conf);
    assertEquals("test", session.getQueueName());
    assertEquals("test", conf.get(TezConfiguration.TEZ_QUEUE_NAME));
    session.setQueueName("test2");
    session = wm.getSession(session, new MappingInput("user"), conf);
    assertEquals("test", session.getQueueName());
  }

  // Note (unrelated to epsilon): all the fraction checks are valid with the current logic in the
  //                              absence of policies. This will change when there are policies.
  private final static double EPSILON = 0.001;

  @Test(timeout = 10000)
  public void testReopen() throws Exception {
    // We should always get a different object, and cluster fraction should be propagated.
    HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WorkloadManager wm = new WorkloadManagerForTest("test", conf, 1, qam);
    wm.start();
    WmTezSession session = (WmTezSession) wm.getSession(null, new MappingInput("user"), conf);
    assertEquals(1.0, session.getClusterFraction(), EPSILON);
    qam.assertWasCalled();
    WmTezSession session2 = (WmTezSession) session.reopen(conf, null);
    assertNotSame(session, session2);
    wm.addTestEvent().get();
    assertEquals(session2.toString(), 1.0, session2.getClusterFraction(), EPSILON);
    assertEquals(0.0, session.getClusterFraction(), EPSILON);
    qam.assertWasCalled();
  }

  @Test(timeout = 10000)
  public void testDestroyAndReturn() throws Exception {
    // Session should not be lost; however the fraction should be discarded.
    HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WorkloadManager wm = new WorkloadManagerForTest("test", conf, 2, qam);
    wm.start();
    WmTezSession session = (WmTezSession) wm.getSession(null, new MappingInput("user"), conf);
    assertEquals(1.0, session.getClusterFraction(), EPSILON);
    qam.assertWasCalled();
    WmTezSession session2 = (WmTezSession) wm.getSession(null, new MappingInput("user"), conf);
    assertEquals(0.5, session.getClusterFraction(), EPSILON);
    assertEquals(0.5, session2.getClusterFraction(), EPSILON);
    qam.assertWasCalled();
    assertNotSame(session, session2);
    session.destroy(); // Destroy before returning to the pool.
    assertEquals(1.0, session2.getClusterFraction(), EPSILON);
    assertEquals(0.0, session.getClusterFraction(), EPSILON);
    qam.assertWasCalled();

    // We never lose pool session, so we should still be able to get.
    session = (WmTezSession) wm.getSession(null, new MappingInput("user"), conf);
    session.returnToSessionManager();
    assertEquals(1.0, session2.getClusterFraction(), EPSILON);
    assertEquals(0.0, session.getClusterFraction(), EPSILON);
    qam.assertWasCalled();
  }

  @Test(timeout = 10000)
  public void testClusterFractions() throws Exception {
    HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(),
        Lists.newArrayList(pool("r1", 1, 0.6f), pool("r2", 1, 0.4f),
            pool("r1.p1", 1, 0.5f), pool("r1.p2", 2, 0.3f)));
    plan.setMappings(Lists.newArrayList(mapping("p1", "r1.p1"),
        mapping("p2", "r1.p2"), mapping("r1", "r1"), mapping("r2", "r2")));
    WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    assertEquals(5, wm.getNumSessions());
    // Get all the 5 sessions; validate cluster fractions.
    WmTezSession session05of06 = (WmTezSession) wm.getSession(null, new MappingInput("p1"), conf);
    assertEquals(0.3, session05of06.getClusterFraction(), EPSILON);
    WmTezSession session03of06 = (WmTezSession) wm.getSession(null, new MappingInput("p2"), conf);
    assertEquals(0.18, session03of06.getClusterFraction(), EPSILON);
    WmTezSession session03of06_2 = (WmTezSession) wm.getSession(null, new MappingInput("p2"), conf);
    assertEquals(0.09, session03of06.getClusterFraction(), EPSILON);
    assertEquals(0.09, session03of06_2.getClusterFraction(), EPSILON);
    WmTezSession session02of06 = (WmTezSession) wm.getSession(null,new MappingInput("r1"), conf);
    assertEquals(0.12, session02of06.getClusterFraction(), EPSILON);
    WmTezSession session04 = (WmTezSession) wm.getSession(null, new MappingInput("r2"), conf);
    assertEquals(0.4, session04.getClusterFraction(), EPSILON);
    session05of06.returnToSessionManager();
    session03of06.returnToSessionManager();
    session03of06_2.returnToSessionManager();
    session02of06.returnToSessionManager();
    session04.returnToSessionManager();
  }

  @Test(timeout=10000)
  public void testQueueing() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
        pool("A", 2, 0.5f), pool("B", 2, 0.5f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A"), mapping("B", "B")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    WmTezSession sessionA1 = (WmTezSession) wm.getSession(null, new MappingInput("A"), conf),
        sessionA2 = (WmTezSession) wm.getSession(null, new MappingInput("A"), conf),
        sessionB1 = (WmTezSession) wm.getSession(null, new MappingInput("B"), conf);
    final AtomicReference<WmTezSession> sessionA3 = new AtomicReference<>(),
        sessionA4 = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final CountDownLatch cdl = new CountDownLatch(1);

    Thread t1 = new Thread(new GetSessionRunnable(sessionA3, wm, error, conf, cdl, "A")),
        t2 = new Thread(new GetSessionRunnable(sessionA4, wm, error, conf, null, "A"));
    waitForThreadToBlock(cdl, t1);
    t2.start();
    assertNull(sessionA3.get());
    assertNull(sessionA4.get());
    checkError(error);
    // While threads are blocked on A, we should still be able to get and return a B session.
    WmTezSession sessionB2 = (WmTezSession) wm.getSession(null, new MappingInput("B"), conf);
    sessionB1.returnToSessionManager();
    sessionB2.returnToSessionManager();
    assertNull(sessionA3.get());
    assertNull(sessionA4.get());
    checkError(error);
    // Now release a single session from A.
    sessionA1.returnToSessionManager();
    t1.join();
    checkError(error);
    assertNotNull(sessionA3.get());
    assertNull(sessionA4.get());
    sessionA3.get().returnToSessionManager();
    t2.join();
    checkError(error);
    assertNotNull(sessionA4.get());
    sessionA4.get().returnToSessionManager();
    sessionA2.returnToSessionManager();
  }

  @Test(timeout=10000)
  public void testReuseWithQueueing() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, 2, qam);
    wm.start();
    WmTezSession session1 = (WmTezSession) wm.getSession(null, new MappingInput("user"), conf);
    // First, try to reuse from the same pool - should "just work".
    WmTezSession session1a = (WmTezSession) wm.getSession(session1, new MappingInput("user"), conf);
    assertSame(session1, session1a);
    assertEquals(1.0, session1.getClusterFraction(), EPSILON);
    // Should still be able to get the 2nd session.
    WmTezSession session2 = (WmTezSession) wm.getSession(null, new MappingInput("user"), conf);

    // Now try to reuse with no other sessions remaining. Should still work.
    WmTezSession session2a = (WmTezSession) wm.getSession(session2, new MappingInput("user"), conf);
    assertSame(session2, session2a);
    assertEquals(0.5, session1.getClusterFraction(), EPSILON);
    assertEquals(0.5, session2.getClusterFraction(), EPSILON);

    // Finally try to reuse with something in the queue. Due to fairness this won't work.
    final AtomicReference<WmTezSession> session3 = new AtomicReference<>(),
    // We will try to reuse this, but session3 is queued before us.
        session4 = new AtomicReference<>(session2);
    final AtomicReference<Throwable> error = new AtomicReference<>();
    CountDownLatch cdl = new CountDownLatch(1), cdl2 = new CountDownLatch(1);
    Thread t1 = new Thread(new GetSessionRunnable(session3, wm, error, conf, cdl, null), "t1"),
        t2 = new Thread(new GetSessionRunnable(session4, wm, error, conf, cdl2, null), "t2");
    waitForThreadToBlock(cdl, t1);
    assertNull(session3.get());
    checkError(error);
    t2.start();
    cdl2.await();
    assertNull(session4.get());

    // We have released the session by trying to reuse it and going back into queue, s3 can start.
    t1.join();
    checkError(error);
    assertNotNull(session3.get());
    assertEquals(0.5, session3.get().getClusterFraction(), EPSILON);

    // Now release another session; the thread that gave up on reuse can proceed.
    session1.returnToSessionManager();
    t2.join();
    checkError(error);
    assertNotNull(session4.get());
    assertNotSame(session2, session4.get());
    assertEquals(0.5, session4.get().getClusterFraction(), EPSILON);
    session3.get().returnToSessionManager();
    session4.get().returnToSessionManager();
  }

  private void waitForThreadToBlock(CountDownLatch cdl, Thread t1) throws InterruptedException {
    t1.start();
    cdl.await();
    // Wait for t1 to block, just be sure. Not ideal...
    State s;
    do {
      s = t1.getState();
    } while (s != State.TIMED_WAITING && s != State.BLOCKED && s != State.WAITING);
  }


  @Test(timeout=10000)
  public void testReuseWithDifferentPool() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
        pool("A", 2, 0.6f), pool("B", 1, 0.4f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A"), mapping("B", "B")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    WmTezSession sessionA1 = (WmTezSession) wm.getSession(null, new MappingInput("A"), conf),
        sessionA2 = (WmTezSession) wm.getSession(null, new MappingInput("A"), conf);
    assertEquals("A", sessionA1.getPoolName());
    assertEquals(0.3f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA2.getPoolName());
    assertEquals(0.3f, sessionA2.getClusterFraction(), EPSILON);
    WmTezSession sessionB1 = (WmTezSession) wm.getSession(sessionA1, new MappingInput("B"), conf);
    assertSame(sessionA1, sessionB1);
    assertEquals("B", sessionB1.getPoolName());
    assertEquals(0.4f, sessionB1.getClusterFraction(), EPSILON);
    assertEquals(0.6f, sessionA2.getClusterFraction(), EPSILON); // A1 removed from A.
    // Make sure that we can still get a session from A.
    WmTezSession sessionA3 = (WmTezSession) wm.getSession(null, new MappingInput("A"), conf);
    assertEquals("A", sessionA3.getPoolName());
    assertEquals(0.3f, sessionA3.getClusterFraction(), EPSILON);
    assertEquals(0.3f, sessionA3.getClusterFraction(), EPSILON);
    sessionA3.returnToSessionManager();
    sessionB1.returnToSessionManager();
    sessionA2.returnToSessionManager();
  }

  @Test(timeout=10000)
  public void testApplyPlanUserMapping() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
        pool("A", 1, 0.5f), pool("B", 1, 0.5f)));
    plan.setMappings(Lists.newArrayList(mapping("U", "A")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
 
    // One session will be running, the other will be queued in "A"
    WmTezSession sessionA1 = (WmTezSession) wm.getSession(null, new MappingInput("U"), conf);
    assertEquals("A", sessionA1.getPoolName());
    assertEquals(0.5f, sessionA1.getClusterFraction(), EPSILON);
    final AtomicReference<WmTezSession> sessionA2 = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final CountDownLatch cdl = new CountDownLatch(1);
    Thread t1 = new Thread(new GetSessionRunnable(sessionA2, wm, error, conf, cdl, "U"));
    waitForThreadToBlock(cdl, t1);
    assertNull(sessionA2.get());
    checkError(error);

    // Now change the resource plan - change the mapping for the user.
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
        pool("A", 1, 0.6f), pool("B", 1, 0.4f)));
    plan.setMappings(Lists.newArrayList(mapping("U", "B")));
    wm.updateResourcePlanAsync(plan);

    // The session will go to B with the new mapping; check it.
    t1.join();
    checkError(error);
    assertNotNull(sessionA2.get());
    assertEquals("B", sessionA2.get().getPoolName());
    assertEquals(0.4f, sessionA2.get().getClusterFraction(), EPSILON);
    // The new session will also go to B now.
    sessionA2.get().returnToSessionManager();
    WmTezSession sessionB1 = (WmTezSession) wm.getSession(null, new MappingInput("U"), conf);
    assertEquals("B", sessionB1.getPoolName());
    assertEquals(0.4f, sessionB1.getClusterFraction(), EPSILON);
    sessionA1.returnToSessionManager();
    sessionB1.returnToSessionManager();
  }


  @Test(timeout=10000)
  public void testApplyPlanQpChanges() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
        pool("A", 1, 0.35f), pool("B", 2, 0.15f),
        pool("C", 2, 0.3f), pool("D", 1, 0.3f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A"), mapping("B", "B"),
            mapping("C", "C"), mapping("D", "D")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
 
    // A: 1/1 running, 1 queued; B: 2/2 running, C: 1/2 running, D: 1/1 running, 1 queued.
    // Total: 5/6 running.
    WmTezSession sessionA1 = (WmTezSession) wm.getSession(null, new MappingInput("A"), conf),
        sessionB1 = (WmTezSession) wm.getSession(null, new MappingInput("B"), conf),
        sessionB2 = (WmTezSession) wm.getSession(null, new MappingInput("B"), conf),
        sessionC1 = (WmTezSession) wm.getSession(null, new MappingInput("C"), conf),
        sessionD1 = (WmTezSession) wm.getSession(null, new MappingInput("D"), conf);
    final AtomicReference<WmTezSession> sessionA2 = new AtomicReference<>(),
        sessionD2 = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final CountDownLatch cdl1 = new CountDownLatch(1), cdl2 = new CountDownLatch(1);
    Thread t1 = new Thread(new GetSessionRunnable(sessionA2, wm, error, conf, cdl1, "A")),
        t2 = new Thread(new GetSessionRunnable(sessionD2, wm, error, conf, cdl2, "D"));
    waitForThreadToBlock(cdl1, t1);
    waitForThreadToBlock(cdl2, t2);
    checkError(error);
    assertEquals(0.3f, sessionC1.getClusterFraction(), EPSILON);
    assertEquals(0.3f, sessionD1.getClusterFraction(), EPSILON);

    // Change the resource plan - resize B and C down, D up, and remove A remapping users to B.
    // Everything will be killed in A and B, C won't change, D will start one more query from
    // the queue, and the query queued in A will be re-queued in B and started.
    // The fractions will also all change.
    // Total: 4/4 running.
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("B", 1, 0.3f),
        pool("C", 1, 0.2f), pool("D", 2, 0.5f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "B"), mapping("B", "B"),
            mapping("C", "C"), mapping("D", "D")));
    wm.updateResourcePlanAsync(plan);
    wm.addTestEvent().get();

    t1.join();
    t2.join();
    checkError(error);
    assertNotNull(sessionA2.get());
    assertNotNull(sessionD2.get());
    assertEquals("D", sessionD2.get().getPoolName());
    assertEquals("B", sessionA2.get().getPoolName());
    assertEquals("C", sessionC1.getPoolName());
    assertEquals(0.3f, sessionA2.get().getClusterFraction(), EPSILON);
    assertEquals(0.2f, sessionC1.getClusterFraction(), EPSILON);
    assertEquals(0.25f, sessionD1.getClusterFraction(), EPSILON);

    assertKilledByWm(sessionA1);
    assertKilledByWm(sessionB1);
    assertKilledByWm(sessionB2);

    // Wait for another iteration to make sure event gets processed for D2 to receive allocation.
    sessionA2.get().returnToSessionManager();
    assertEquals(0.25f, sessionD2.get().getClusterFraction(), EPSILON);
    sessionD2.get().returnToSessionManager();
    sessionC1.returnToSessionManager();
    sessionD1.returnToSessionManager();

    // Try to "return" stuff that was killed from "under" us. Should be a no-op.
    sessionA1.returnToSessionManager();
    sessionB1.returnToSessionManager();
    sessionB2.returnToSessionManager(); 
    assertEquals(4, wm.getTezAmPool().getCurrentSize());
  }

  @Test(timeout=10000)
  public void testAmPoolInteractions() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
        pool("A", 1, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    // Take away the only session, as if it was expiring.
    TezSessionPool<WmTezSession> pool = wm.getTezAmPool();
    WmTezSession oob = pool.getSession();
 
    final AtomicReference<WmTezSession> sessionA1 = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final CountDownLatch cdl1 = new CountDownLatch(1);
    Thread t1 = new Thread(new GetSessionRunnable(sessionA1, wm, error, conf, cdl1, "A"));
    waitForThreadToBlock(cdl1, t1);
    checkError(error);
    // Replacing it directly in the pool should unblock get.
    pool.replaceSession(oob, false, null);
    t1.join();
    assertNotNull(sessionA1.get());
    assertEquals("A", sessionA1.get().getPoolName());

    // Increase qp, check that the pool grows.
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
        pool("A", 4, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    wm.updateResourcePlanAsync(plan);
    WmTezSession oob2 = pool.getSession(),
        oob3 = pool.getSession(),
        oob4 = pool.getSession();
    pool.returnSession(oob2);
    assertEquals(1, pool.getCurrentSize());

    // Decrease qp, check that the pool shrinks incl. killing the unused and returned sessions.
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("A", 1, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    wm.updateResourcePlanAsync(plan);
    wm.addTestEvent().get();
    assertEquals(0, pool.getCurrentSize());
     sessionA1.get().returnToSessionManager();
    pool.returnSession(oob3);
    assertEquals(0, pool.getCurrentSize());
    pool.returnSession(oob4);
    assertEquals(1, pool.getCurrentSize());

    // Decrease, then increase qp - sessions should not be killed on return.
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("A", 2, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    wm.updateResourcePlanAsync(plan);
    oob2 = pool.getSession();
    oob3 = pool.getSession();
    assertEquals(0, pool.getCurrentSize());
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("A", 1, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    wm.updateResourcePlanAsync(plan);
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("A", 2, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    wm.updateResourcePlanAsync(plan);
    wm.addTestEvent().get();
    assertEquals(0, pool.getCurrentSize());
    pool.returnSession(oob3);
    pool.returnSession(oob4);
    assertEquals(2, pool.getCurrentSize());
  }

  @Test(timeout=10000)
  public void testAsyncSessionInitFailures() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(),
        Lists.newArrayList(pool("A", 1, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();

    // Make sure session init gets stuck in init.
    TezSessionPool<WmTezSession> pool = wm.getTezAmPool();
    SampleTezSessionState theOnlySession = (SampleTezSessionState) pool.getSession();
    SettableFuture<Boolean> blockedWait = SettableFuture.create();
    theOnlySession.setWaitForAmRegistryFuture(blockedWait);
    pool.returnSession(theOnlySession);
    assertEquals(1, pool.getCurrentSize());

    final AtomicReference<WmTezSession> sessionA = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    CountDownLatch cdl = new CountDownLatch(1);
    Thread t1 = new Thread(new GetSessionRunnable(sessionA, wm, error, conf, cdl, "A"));
    waitForThreadToBlock(cdl, t1);
    checkError(error);
    wm.addTestEvent().get();
    // The session is taken out of the pool, but is waiting for registration.
    assertEquals(0, pool.getCurrentSize());

    // Change the resource plan, so that the session gets killed.
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("B", 1, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "B")));
    wm.updateResourcePlanAsync(plan);
    wm.addTestEvent().get();
    blockedWait.set(true); // Meanwhile, the init succeeds!
    t1.join();
    try {
      sessionA.get();
      fail("Expected an error but got " + sessionA.get());
    } catch (Throwable t) {
      // Expected.
    }
    try {
      // The get-session call should also fail.
      checkError(error);
      fail("Expected an error");
    } catch (Exception ex) {
      // Expected.
    }
    error.set(null);
    theOnlySession = validatePoolAfterCleanup(theOnlySession, conf, wm, pool, "B");

    // Initialization fails, no resource plan change.
    SettableFuture<Boolean> failedWait = SettableFuture.create();
    failedWait.setException(new Exception("foo"));
    theOnlySession.setWaitForAmRegistryFuture(failedWait);
    try {
      TezSessionState r = wm.getSession(null, new MappingInput("A"), conf);
      fail("Expected an error but got " + r);
    } catch (Exception ex) {
      // Expected.
    }
    theOnlySession = validatePoolAfterCleanup(theOnlySession, conf, wm, pool, "B");

    // Init fails, but the session is also killed by WM before that.
    failedWait = SettableFuture.create();
    theOnlySession.setWaitForAmRegistryFuture(failedWait);
    sessionA.set(null);
    cdl = new CountDownLatch(1);
    t1 = new Thread(new GetSessionRunnable(sessionA, wm, error, conf, cdl, "A"));
    waitForThreadToBlock(cdl, t1);
    wm.addTestEvent().get();
    // The session is taken out of the pool, but is waiting for registration.
    assertEquals(0, pool.getCurrentSize());

    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("A", 1, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    wm.updateResourcePlanAsync(plan);
    wm.addTestEvent().get();
    failedWait.setException(new Exception("moo")); // Meanwhile, the init fails.
    t1.join();
    try {
      sessionA.get();
      fail("Expected an error but got " + sessionA.get());
    } catch (Throwable t) {
      // Expected.
    }
    try {
      // The get-session call should also fail.
      checkError(error);
      fail("Expected an error");
    } catch (Exception ex) {
      // Expected.
    }
    validatePoolAfterCleanup(theOnlySession, conf, wm, pool, "A");
  }

  private SampleTezSessionState validatePoolAfterCleanup(
      SampleTezSessionState oldSession, HiveConf conf, WorkloadManager wm,
      TezSessionPool<WmTezSession> pool, String sessionPoolName) throws Exception {
    // Make sure the cleanup doesn't leave the pool without a session.
    SampleTezSessionState theOnlySession = (SampleTezSessionState) pool.getSession();
    assertNotNull(theOnlySession);
    theOnlySession.setWaitForAmRegistryFuture(null);
    assertNull(oldSession.getPoolName());
    assertEquals(0f, oldSession.getClusterFraction(), EPSILON);
    pool.returnSession(theOnlySession);
    // Make sure we can actually get a session still - parallelism/etc. should not be affected.
    WmTezSession result = (WmTezSession) wm.getSession(null, new MappingInput("A"), conf);
    assertEquals(sessionPoolName, result.getPoolName());
    assertEquals(1f, result.getClusterFraction(), EPSILON);
    result.returnToSessionManager();
    return theOnlySession;
  }

  private void assertKilledByWm(WmTezSession session) {
    assertNull(session.getPoolName());
    assertEquals(0f, session.getClusterFraction(), EPSILON);
    assertTrue(session.isIrrelevantForWm());
  }

  private void checkError(final AtomicReference<Throwable> error) throws Exception {
    Throwable t = error.get();
    if (t == null) return;
    throw new Exception(t);
  }

  private HiveConf createConf() {
    HiveConf conf = new HiveConf();
    conf.set(ConfVars.HIVE_SERVER2_TEZ_SESSION_LIFETIME.varname, "-1");
    conf.set(ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
    conf.set(ConfVars.LLAP_TASK_SCHEDULER_AM_REGISTRY_NAME.varname, "");
    return conf;
  }
}
