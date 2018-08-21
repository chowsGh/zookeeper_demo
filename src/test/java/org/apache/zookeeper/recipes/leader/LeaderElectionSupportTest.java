/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.zookeeper.recipes.leader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderElectionSupportTest extends ClientBase {

    private static final Logger logger = LoggerFactory.getLogger(LeaderElectionSupportTest.class);
    private static final String testRootNode = "/" + System.currentTimeMillis() + "_";

    private ZooKeeper zooKeeper;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        zooKeeper = createClient();

        zooKeeper.create(testRootNode + Thread.currentThread().getId(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
    }

    @After
    public void tearDown() throws Exception {
        if (zooKeeper != null) {
            zooKeeper.delete(testRootNode + Thread.currentThread().getId(), -1);
        }

        super.tearDown();
    }

    @Test
    public void testNode() throws IOException, InterruptedException, KeeperException {

        LeaderElectionSupport electionSupport = createLeaderElectionSupport();

        electionSupport.start();
        Thread.sleep(3000);
        electionSupport.stop();
    }

    @Test
    public void testNodes3() throws IOException, InterruptedException, KeeperException {

        int testIterations = 3;
        final CountDownLatch latch = new CountDownLatch(testIterations);
        final AtomicInteger failureCounter = new AtomicInteger();

        for (int i = 0; i < testIterations; i++) {
            runElectionSupportThread(latch, failureCounter);
        }

        Assert.assertEquals(0, failureCounter.get());

        if (!latch.await(1000, TimeUnit.SECONDS)) {
            logger.info("Waited for all threads to start, but timed out. We had {} failures.", failureCounter);
        }
    }

    @Test
    public void testNodes9() throws IOException, InterruptedException, KeeperException {

        int testIterations = 9;
        final CountDownLatch latch = new CountDownLatch(testIterations);
        final AtomicInteger failureCounter = new AtomicInteger();

        for (int i = 0; i < testIterations; i++) {
            runElectionSupportThread(latch, failureCounter);
        }

        Assert.assertEquals(0, failureCounter.get());

        if (!latch.await(10, TimeUnit.SECONDS)) {
            logger.info("Waited for all threads to start, but timed out. We had {} failures.", failureCounter);
        }
    }

    @Test
    public void testNodes20() throws IOException, InterruptedException, KeeperException {

        int testIterations = 20;
        final CountDownLatch latch = new CountDownLatch(testIterations);
        final AtomicInteger failureCounter = new AtomicInteger();

        for (int i = 0; i < testIterations; i++) {
            runElectionSupportThread(latch, failureCounter);
        }

        Assert.assertEquals(0, failureCounter.get());

        if (!latch.await(10, TimeUnit.SECONDS)) {
            logger.info("Waited for all threads to start, but timed out. We had {} failures.", failureCounter);
        }
    }

    //
    class CreateThread extends Thread {
        public CreateThread(CountDownLatch latch, ZooKeeper zk, String path) {
            super();
            this.latch = latch;
            this.zk = zk;
            this.path = path;
        }

        public CountDownLatch latch;
        private ZooKeeper zk;
        private String path;

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            String resultPath = null;
            try {
                resultPath = zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (resultPath == null) {
                System.err.println("zk sessionid:" + zk.getSessionId() + ", create failed. ");
            } else {
                System.out.println("zk sessionid:" + zk.getSessionId() + ", create successed");
            }
            try {
                Thread.sleep(30 * 1000);
                zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public ZooKeeper getZk() {
            return zk;
        }

        public void setZk(ZooKeeper zk) {
            this.zk = zk;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }
    }

    /**
     * 模拟执行
     */
    @Test
    public void testCreate() {
        try {
            String path = "/test";
            CountDownLatch latch = new CountDownLatch(1);
            new CreateThread(latch, createClient(), path).start();
            new CreateThread(latch, createClient(), path).start();
            new CreateThread(latch, createClient(), path).start();
            latch.countDown();
            Thread.sleep(100 * 1000);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Test
    public void testNodes100() throws IOException, InterruptedException, KeeperException {

        int testIterations = 100;
        final CountDownLatch latch = new CountDownLatch(testIterations);
        final AtomicInteger failureCounter = new AtomicInteger();

        for (int i = 0; i < testIterations; i++) {
            runElectionSupportThread(latch, failureCounter);
        }

        Assert.assertEquals(0, failureCounter.get());

        if (!latch.await(20 * 60, TimeUnit.SECONDS)) {
            logger.info("Waited for all threads to start, but timed out. We had {} failures.", failureCounter);
        }
    }

    @Test
    public void testOfferShuffle() throws InterruptedException {
        int testIterations = 10;
        final CountDownLatch latch = new CountDownLatch(testIterations);
        final AtomicInteger failureCounter = new AtomicInteger();
        List<Thread> threads = new ArrayList<Thread>(testIterations);

        for (int i = 1; i <= testIterations; i++) {
            threads.add(runElectionSupportThread(latch, failureCounter, Math.min(i * 1200, 10000)));
        }

        if (!latch.await(60, TimeUnit.SECONDS)) {
            logger.info("Waited for all threads to start, but timed out. We had {} failures.", failureCounter);
        }
    }

    @Test
    public void testGetLeaderHostName() throws KeeperException, InterruptedException {

        LeaderElectionSupport electionSupport = createLeaderElectionSupport();

        electionSupport.start();

        // Sketchy: We assume there will be a leader (probably us) in 3 seconds.
        Thread.sleep(3000);

        String leaderHostName = electionSupport.getLeaderHostName();

        Assert.assertNotNull(leaderHostName);
        Assert.assertEquals("foohost", leaderHostName);

        electionSupport.stop();
    }

    private LeaderElectionSupport createLeaderElectionSupport() {
        LeaderElectionSupport electionSupport = new LeaderElectionSupport();

        electionSupport.setZooKeeper(zooKeeper);
        electionSupport.setRootNodeName(testRootNode + Thread.currentThread().getId());
        electionSupport.setHostName("foohost");

        return electionSupport;
    }

    private Thread runElectionSupportThread(final CountDownLatch latch, final AtomicInteger failureCounter) {
        return runElectionSupportThread(latch, failureCounter, 300 * 1000);
    }

    private Thread runElectionSupportThread(final CountDownLatch latch, final AtomicInteger failureCounter,
        final long sleepDuration) {

        final LeaderElectionSupport electionSupport = createLeaderElectionSupport();

        Thread t = new Thread() {

            @Override
            public void run() {
                try {
                    electionSupport.start();
                    Thread.sleep(sleepDuration);
                    electionSupport.stop();

                    latch.countDown();
                } catch (Exception e) {
                    logger.warn("Failed to run leader election due to: {}", e.getMessage());
                    failureCounter.incrementAndGet();
                }
            }
        };

        t.start();

        return t;
    }

}
