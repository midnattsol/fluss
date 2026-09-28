/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator;

import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.StopReplicaResponse;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.DeleteReplicaResponseReceivedEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.TestingEventManager;
import org.apache.fluss.server.entity.DeleteReplicaResultForBucket;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.testutils.common.ScheduledTask;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toTableBucket;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link CoordinatorRequestBatch}. */
class CoordinatorRequestBatchTest {

    private static final long RETRY_DELAY_MS = 30_000L;

    private CoordinatorContext coordinatorContext;

    @BeforeEach
    void beforeEach() {
        coordinatorContext = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);
    }

    /**
     * When the NotifyLeaderAndIsr request to the actual leader (leader == serverId) fails, the
     * pending leader activation entry that this request added must be cleared via an
     * AccessContextEvent dispatched on the event manager.
     */
    @Test
    void testNotifyLeaderAndIsrSendFailureClearsLeaderPending() {
        long tableId = 100L;
        TableBucket tb = new TableBucket(tableId, 0);
        TablePath tablePath = TablePath.of("db1", "t1");

        coordinatorContext.putTablePath(tableId, tablePath);
        putTableInfo(tableId, tablePath);
        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Collections.singletonList(0)));
        coordinatorContext.updateBucketReplicaAssignment(tb, Collections.singletonList(0));
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(0, 0, Collections.singletonList(0), Collections.emptyList(), 0, 0);
        coordinatorContext.putBucketLeaderAndIsr(tb, leaderAndIsr);

        TestCoordinatorChannelManager failingChannel = newAlwaysFailingChannelManager();
        EventManager eventManager = newSynchronousAccessContextEventManager();

        CoordinatorRequestBatch batch =
                new CoordinatorRequestBatch(
                        failingChannel,
                        eventManager,
                        coordinatorContext,
                        new ManualScheduler(),
                        RETRY_DELAY_MS);
        // server 0 IS the leader, so this request will mark tb pending.
        batch.addNotifyLeaderRequestForTabletServers(
                Collections.singleton(0),
                PhysicalTablePath.of(tablePath),
                tb,
                Collections.singletonList(0),
                leaderAndIsr);

        batch.sendRequestToTabletServers(0);

        // The failure callback must clear the pending entry that this request added.
        assertThat(coordinatorContext.getPendingLeaderActivationBuckets()).isEmpty();
    }

    /**
     * When the NotifyLeaderAndIsr request to a follower (leader != serverId) fails, the failure
     * callback must NOT clear any pending leader activation entry. Specifically, it must not remove
     * entries added by another in-flight sender targeting the actual leader.
     */
    @Test
    void testNotifyLeaderAndIsrSendFailureToFollowerDoesNotClearOtherPending() {
        long tableId = 200L;
        TableBucket followerTb = new TableBucket(tableId, 0);
        TableBucket otherLeaderTb = new TableBucket(tableId, 1);
        TablePath tablePath = TablePath.of("db1", "t2");

        coordinatorContext.putTablePath(tableId, tablePath);
        putTableInfo(tableId, tablePath);
        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Arrays.asList(0, 1)));
        coordinatorContext.updateBucketReplicaAssignment(followerTb, Arrays.asList(0, 1));
        coordinatorContext.updateBucketReplicaAssignment(otherLeaderTb, Arrays.asList(0, 1));
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(0, 0, Arrays.asList(0, 1), Collections.emptyList(), 0, 0);
        coordinatorContext.putBucketLeaderAndIsr(followerTb, leaderAndIsr);

        // Simulate another in-flight sender having already marked otherLeaderTb pending - we
        // must not touch this entry on the unrelated follower-send failure.
        coordinatorContext.addPendingLeaderActivation(otherLeaderTb);

        TestCoordinatorChannelManager failingChannel = newAlwaysFailingChannelManager();
        EventManager eventManager = newSynchronousAccessContextEventManager();

        CoordinatorRequestBatch batch =
                new CoordinatorRequestBatch(
                        failingChannel,
                        eventManager,
                        coordinatorContext,
                        new ManualScheduler(),
                        RETRY_DELAY_MS);
        // Send to server 1, which is a follower for followerTb (leader is 0). Because
        // leader != serverId, this request does NOT add followerTb to pending, so the failure
        // callback must short-circuit without dispatching an AccessContextEvent.
        batch.addNotifyLeaderRequestForTabletServers(
                Collections.singleton(1),
                PhysicalTablePath.of(tablePath),
                followerTb,
                Arrays.asList(0, 1),
                leaderAndIsr);

        batch.sendRequestToTabletServers(0);

        // The unrelated leader's pending entry must remain intact.
        assertThat(coordinatorContext.getPendingLeaderActivationBuckets())
                .containsExactly(otherLeaderTb);
    }

    @Test
    void testNotifyLeaderAndIsrAllowsMissingRoutingState() {
        long tableId = 300L;
        TableBucket tb = new TableBucket(tableId, 0);
        TablePath tablePath = TablePath.of("db1", "t3");
        coordinatorContext.putTablePath(tableId, tablePath);
        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Collections.singletonList(0)));
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(0, 0, Collections.singletonList(0), Collections.emptyList(), 0, 0);
        coordinatorContext.putBucketLeaderAndIsr(tb, leaderAndIsr);

        AtomicReference<NotifyLeaderAndIsrRequest> sentRequest = new AtomicReference<>();
        TestCoordinatorChannelManager channelManager =
                new TestCoordinatorChannelManager() {
                    @Override
                    public void sendBucketLeaderAndIsrRequest(
                            int receiveServerId,
                            NotifyLeaderAndIsrRequest request,
                            BiConsumer<NotifyLeaderAndIsrResponse, ? super Throwable>
                                    responseConsumer) {
                        sentRequest.set(request);
                        responseConsumer.accept(
                                null, new NetworkException("simulated send failure for test"));
                    }
                };
        CoordinatorRequestBatch batch =
                new CoordinatorRequestBatch(
                        channelManager,
                        newSynchronousAccessContextEventManager(),
                        coordinatorContext,
                        new ManualScheduler(),
                        RETRY_DELAY_MS);

        batch.addNotifyLeaderRequestForTabletServers(
                Collections.singleton(0),
                PhysicalTablePath.of(tablePath),
                tb,
                Collections.singletonList(0),
                leaderAndIsr);

        assertThat(coordinatorContext.getPendingLeaderActivationBuckets()).isEmpty();
        batch.sendRequestToTabletServers(0);

        assertThat(sentRequest.get()).isNotNull();
        assertThat(sentRequest.get().getNotifyBucketsLeaderReqsList()).hasSize(1);
        PbNotifyLeaderAndIsrReqForBucket bucketRequest =
                sentRequest.get().getNotifyBucketsLeaderReqsList().get(0);
        assertThat(bucketRequest.hasBucketCount()).isFalse();
        assertThat(bucketRequest.hasBucketCountEpoch()).isFalse();
        assertThat(coordinatorContext.getPendingLeaderActivationBuckets()).isEmpty();
    }

    /**
     * A failed notify-leader-and-isr send to a live server must schedule a one-shot retry, and
     * pumping the scheduler must resend the same buckets.
     */
    @Test
    void testNotifyLeaderAndIsrFailureSchedulesRetryWithSameBuckets() {
        long tableId = 400L;
        TableBucket tb = new TableBucket(tableId, 0);
        TablePath tablePath = TablePath.of("db1", "t4");

        coordinatorContext.putTablePath(tableId, tablePath);
        putTableInfo(tableId, tablePath);
        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Collections.singletonList(0)));
        coordinatorContext.updateBucketReplicaAssignment(tb, Collections.singletonList(0));
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(0, 0, Collections.singletonList(0), Collections.emptyList(), 0, 0);
        coordinatorContext.putBucketLeaderAndIsr(tb, leaderAndIsr);

        AtomicInteger sendCount = new AtomicInteger();
        List<NotifyLeaderAndIsrRequest> sentRequests = new ArrayList<>();
        TestCoordinatorChannelManager flakyChannel =
                new TestCoordinatorChannelManager() {
                    @Override
                    public void sendBucketLeaderAndIsrRequest(
                            int receiveServerId,
                            NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest,
                            BiConsumer<NotifyLeaderAndIsrResponse, ? super Throwable>
                                    responseConsumer) {
                        sentRequests.add(notifyLeaderAndIsrRequest);
                        if (sendCount.getAndIncrement() == 0) {
                            responseConsumer.accept(
                                    null, new NetworkException("simulated send failure for test"));
                        } else {
                            responseConsumer.accept(new NotifyLeaderAndIsrResponse(), null);
                        }
                    }
                };
        ManualScheduler scheduler = new ManualScheduler();
        CoordinatorRequestBatch batch =
                new CoordinatorRequestBatch(
                        flakyChannel,
                        newSynchronousAccessContextEventManager(),
                        coordinatorContext,
                        scheduler,
                        RETRY_DELAY_MS);
        batch.addNotifyLeaderRequestForTabletServers(
                Collections.singleton(0),
                PhysicalTablePath.of(tablePath),
                tb,
                Collections.singletonList(0),
                leaderAndIsr);

        batch.sendRequestToTabletServers(0);

        assertThat(sendCount.get()).isEqualTo(1);
        assertThat(scheduler.pendingTaskCount()).isEqualTo(1);

        scheduler.runPendingTasks();

        // The retry resends the same buckets and succeeds, so no further retry is scheduled.
        assertThat(sendCount.get()).isEqualTo(2);
        assertThat(scheduler.pendingTaskCount()).isEqualTo(0);
        assertThat(sentRequests).hasSize(2);
        for (NotifyLeaderAndIsrRequest sentRequest : sentRequests) {
            assertThat(sentRequest.getNotifyBucketsLeaderReqsList()).hasSize(1);
            assertThat(
                            toTableBucket(
                                    sentRequest
                                            .getNotifyBucketsLeaderReqsList()
                                            .get(0)
                                            .getTableBucket()))
                    .isEqualTo(tb);
        }
    }

    /**
     * A failed notify-leader-and-isr send to a server that is no longer live must not schedule a
     * retry; recovery for dead servers is owned by the dead-server path.
     */
    @Test
    void testNotifyLeaderAndIsrFailureToDeadServerSchedulesNoRetry() {
        long tableId = 500L;
        TableBucket tb = new TableBucket(tableId, 0);
        TablePath tablePath = TablePath.of("db1", "t5");

        coordinatorContext.putTablePath(tableId, tablePath);
        putTableInfo(tableId, tablePath);
        // Server 0 is deliberately NOT registered as live.
        coordinatorContext.updateBucketReplicaAssignment(tb, Collections.singletonList(0));
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(0, 0, Collections.singletonList(0), Collections.emptyList(), 0, 0);
        coordinatorContext.putBucketLeaderAndIsr(tb, leaderAndIsr);

        AtomicInteger sendCount = new AtomicInteger();
        TestCoordinatorChannelManager failingChannel =
                new TestCoordinatorChannelManager() {
                    @Override
                    public void sendBucketLeaderAndIsrRequest(
                            int receiveServerId,
                            NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest,
                            BiConsumer<NotifyLeaderAndIsrResponse, ? super Throwable>
                                    responseConsumer) {
                        sendCount.incrementAndGet();
                        responseConsumer.accept(
                                null, new NetworkException("simulated send failure for test"));
                    }
                };
        ManualScheduler scheduler = new ManualScheduler();
        CoordinatorRequestBatch batch =
                new CoordinatorRequestBatch(
                        failingChannel,
                        newSynchronousAccessContextEventManager(),
                        coordinatorContext,
                        scheduler,
                        RETRY_DELAY_MS);
        batch.addNotifyLeaderRequestForTabletServers(
                Collections.singleton(0),
                PhysicalTablePath.of(tablePath),
                tb,
                Collections.singletonList(0),
                leaderAndIsr);

        batch.sendRequestToTabletServers(0);

        assertThat(sendCount.get()).isEqualTo(1);
        assertThat(scheduler.pendingTaskCount()).isEqualTo(0);

        scheduler.runPendingTasks();
        assertThat(sendCount.get()).isEqualTo(1);
    }

    /**
     * A transport-level failure of a stop-replica send must feed back as failed delete results for
     * the delete=true buckets only. Migration-style (deleteLocal=true, deleteRemote=false) sends
     * stay best-effort and must not be routed into the deletion-success machinery.
     */
    @Test
    void testStopReplicaTransportFailureEmitsDeleteFailureOnlyForDeletes() {
        long tableId = 600L;
        TableBucket deleteBucket = new TableBucket(tableId, 0);
        TableBucket migrateBucket = new TableBucket(tableId, 1);
        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Collections.singletonList(0)));

        TestCoordinatorChannelManager failingChannel =
                new TestCoordinatorChannelManager() {
                    @Override
                    public void sendStopBucketReplicaRequest(
                            int receiveServerId,
                            StopReplicaRequest stopReplicaRequest,
                            BiConsumer<StopReplicaResponse, ? super Throwable> responseConsumer) {
                        responseConsumer.accept(
                                null, new NetworkException("simulated send failure for test"));
                    }
                };
        TestingEventManager eventManager = new TestingEventManager();
        CoordinatorRequestBatch batch =
                new CoordinatorRequestBatch(
                        failingChannel,
                        eventManager,
                        coordinatorContext,
                        new ManualScheduler(),
                        RETRY_DELAY_MS);
        // delete=true: replica deletion, must feed back into the deletion retry chain.
        batch.addStopReplicaRequestForTabletServers(
                Collections.singleton(0), deleteBucket, true, true, 0);
        // deleteLocal=true, deleteRemote=false: replica migration, must stay best-effort.
        batch.addStopReplicaRequestForTabletServers(
                Collections.singleton(0), migrateBucket, true, false, 0);

        batch.sendRequestToTabletServers(0);

        List<DeleteReplicaResponseReceivedEvent> deleteEvents = new ArrayList<>();
        for (CoordinatorEvent event : eventManager.getEvents()) {
            if (event instanceof DeleteReplicaResponseReceivedEvent) {
                deleteEvents.add((DeleteReplicaResponseReceivedEvent) event);
            }
        }
        assertThat(deleteEvents).hasSize(1);
        List<DeleteReplicaResultForBucket> results = deleteEvents.get(0).getDeleteReplicaResults();
        assertThat(results).hasSize(1);
        DeleteReplicaResultForBucket result = results.get(0);
        assertThat(result.getTableBucket()).isEqualTo(deleteBucket);
        assertThat(result.getReplica()).isEqualTo(0);
        assertThat(result.failed()).isTrue();
    }

    /** Registers table metadata so normal notifications carry the bucket layout epoch. */
    private void putTableInfo(long tableId, TablePath tablePath) {
        coordinatorContext.putTableInfo(
                TableInfo.of(
                        tablePath,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
    }

    private static TestCoordinatorChannelManager newAlwaysFailingChannelManager() {
        return new TestCoordinatorChannelManager() {
            @Override
            public void sendBucketLeaderAndIsrRequest(
                    int receiveServerId,
                    NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest,
                    BiConsumer<NotifyLeaderAndIsrResponse, ? super Throwable> responseConsumer) {
                responseConsumer.accept(
                        null, new NetworkException("simulated send failure for test"));
            }
        };
    }

    /**
     * Returns an EventManager that synchronously executes {@link AccessContextEvent}s against the
     * test's CoordinatorContext, mimicking the serial event-thread semantics.
     */
    private EventManager newSynchronousAccessContextEventManager() {
        return event -> {
            if (event instanceof AccessContextEvent) {
                AccessContextEvent<?> accessContextEvent = (AccessContextEvent<?>) event;
                accessContextEvent.getAccessFunction().apply(coordinatorContext);
            }
        };
    }

    /**
     * A scheduler that queues tasks instead of running them, so tests pump retries by hand. Never
     * combine with an always-failing channel and pump it: the self-rearming retry would resend
     * forever.
     */
    private static final class ManualScheduler implements Scheduler {
        private final List<Runnable> pendingTasks = new ArrayList<>();

        @Override
        public void startup() {
            // do nothing
        }

        @Override
        public void shutdown() {
            // do nothing
        }

        @Override
        public ScheduledFuture<?> schedule(
                String name, Runnable task, long delayMs, long periodMs) {
            pendingTasks.add(task);
            return new ScheduledTask<>(() -> null, delayMs, periodMs);
        }

        private int pendingTaskCount() {
            return pendingTasks.size();
        }

        private void runPendingTasks() {
            List<Runnable> tasksToRun = new ArrayList<>(pendingTasks);
            pendingTasks.clear();
            for (Runnable task : tasksToRun) {
                task.run();
            }
        }
    }
}
