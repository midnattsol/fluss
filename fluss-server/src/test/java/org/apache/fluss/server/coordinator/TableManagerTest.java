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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketReplica;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbStopReplicaReqForBucket;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.StopReplicaResponse;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.DeleteReplicaResponseReceivedEvent;
import org.apache.fluss.server.coordinator.event.ResumeDropEvent;
import org.apache.fluss.server.coordinator.event.TestingEventManager;
import org.apache.fluss.server.coordinator.statemachine.ReplicaStateMachine;
import org.apache.fluss.server.coordinator.statemachine.TableBucketStateMachine;
import org.apache.fluss.server.entity.DeleteReplicaResultForBucket;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.coordinator.statemachine.BucketState.OnlineBucket;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.OnlineReplica;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.ReplicaDeletionStarted;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.ReplicaDeletionSuccessful;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TableManager}. */
class TableManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;
    private static ZkEpoch zkEpoch;
    private static ExecutorService ioExecutor;

    private CoordinatorContext coordinatorContext;
    private ReplicaCapacityController replicaCapacityController;
    private ReplicaStateMachine replicaStateMachine;
    private TableManager tableManager;
    private TableLifecycleThrottler lifecycleThrottler;
    private TestingEventManager testingEventManager;
    private TestCoordinatorChannelManager testCoordinatorChannelManager;

    @BeforeAll
    static void baseBeforeAll() throws Exception {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        zkEpoch = zookeeperClient.fenceBecomeCoordinatorLeader("1");
        ioExecutor = Executors.newFixedThreadPool(1);
    }

    @BeforeEach
    void beforeEach() throws IOException {
        initTableManager();
    }

    @AfterEach
    void afterEach() {
        if (tableManager != null) {
            tableManager.shutdown();
        }
        if (lifecycleThrottler != null) {
            lifecycleThrottler.close();
        }
    }

    @AfterAll
    static void afterAll() {
        ioExecutor.shutdownNow();
    }

    private void initTableManager() {
        testingEventManager = new TestingEventManager();
        coordinatorContext = new CoordinatorContext(zkEpoch);
        replicaCapacityController =
                new ReplicaCapacityController(new Configuration(), new CoordinatorMetadataCache());
        testCoordinatorChannelManager = new TestCoordinatorChannelManager();
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, "/tmp/fluss/remote-data");
        CoordinatorRequestBatch coordinatorRequestBatch =
                new CoordinatorRequestBatch(
                        testCoordinatorChannelManager, testingEventManager, coordinatorContext);
        replicaStateMachine =
                new ReplicaStateMachine(
                        coordinatorContext, coordinatorRequestBatch, zookeeperClient);
        TableBucketStateMachine tableBucketStateMachine =
                new TableBucketStateMachine(
                        coordinatorContext, coordinatorRequestBatch, zookeeperClient);
        MetadataManager metadataManager =
                new MetadataManager(
                        zookeeperClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
        lifecycleThrottler =
                new TableLifecycleThrottler(
                        testingEventManager, SystemClock.getInstance(), new Configuration());
        tableManager =
                new TableManager(
                        metadataManager,
                        coordinatorContext,
                        replicaCapacityController,
                        replicaStateMachine,
                        tableBucketStateMachine,
                        new RemoteStorageCleaner(conf, ioExecutor),
                        ioExecutor,
                        lifecycleThrottler);
        tableManager.startup();

        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Arrays.asList(0, 1, 2)));
        CoordinatorTestUtils.makeSendLeaderAndStopRequestAlwaysSuccess(
                coordinatorContext, testCoordinatorChannelManager);
    }

    @Test
    void testCreateTable() throws Exception {
        TableAssignment assignment =
                TableAssignment.builder()
                        .add(0, BucketAssignment.of(0, 1, 2))
                        .add(1, BucketAssignment.of(1, 2, 0))
                        .add(2, BucketAssignment.of(2, 1, 0))
                        .build();

        long tableId = DATA1_TABLE_ID;
        coordinatorContext.putTableInfo(
                TableInfo.of(
                        DATA1_TABLE_PATH,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        tableManager.onCreateNewTable(DATA1_TABLE_PATH, tableId, assignment);

        assertThat(coordinatorContext.getKvBucketCount()).isZero();
        assertThat(replicaCapacityController.getKvLeaderReplicaCount()).isZero();
        // all replica should be online
        checkReplicaOnline(tableId, null, assignment);
        // clear the assignment for the table
        zookeeperClient.deleteTableAssignment(tableId);
    }

    @Test
    void testDeleteTable() throws Exception {
        // first, create a table
        long tableId = zookeeperClient.getTableIdAndIncrement();
        TableAssignment assignment = createAssignment();
        zookeeperClient.registerTableAssignment(tableId, assignment);

        coordinatorContext.putTableInfo(
                TableInfo.of(
                        DATA1_TABLE_PATH_PK,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR_PK,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        tableManager.onCreateNewTable(DATA1_TABLE_PATH_PK, tableId, assignment);
        assertThat(coordinatorContext.getKvBucketCount()).isEqualTo(assignment.getBuckets().size());
        assertThat(replicaCapacityController.getKvLeaderReplicaCount())
                .isEqualTo(assignment.getBuckets().size());

        // now, delete the created table
        coordinatorContext.queueTableDeletion(Collections.singleton(tableId));
        tableManager.onDeleteTable(tableId);
        assertThat(coordinatorContext.getKvBucketCount()).isZero();
        assertThat(replicaCapacityController.getKvLeaderReplicaCount()).isZero();

        // make sure the delete replica success events in event manager is equal to the expected
        checkReplicaDelete(tableId, null, assignment);

        // mark all replica as delete
        for (TableBucketReplica replica : getReplicas(tableId, assignment)) {
            coordinatorContext.putReplicaState(replica, ReplicaDeletionSuccessful);
        }

        // call method resumeDeletions, should delete the assignments from zk
        tableManager.resumeDeletions();
        // retry for async deletion of TableAssignment
        retry(
                Duration.ofSeconds(30),
                () -> assertThat(zookeeperClient.getTableAssignment(tableId)).isEmpty());
        // the table will also be removed from coordinator context
        assertThat(coordinatorContext.getAllReplicasForTable(tableId)).isEmpty();
    }

    @Test
    void testResumeDeletionAfterRestart() throws Exception {
        // first, create a table
        long tableId = zookeeperClient.getTableIdAndIncrement();
        TableAssignment assignment = createAssignment();
        zookeeperClient.registerTableAssignment(tableId, assignment);

        coordinatorContext.putTableInfo(
                TableInfo.of(
                        DATA1_TABLE_PATH,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        tableManager.onCreateNewTable(DATA1_TABLE_PATH, tableId, assignment);

        // now, delete the created table/partition
        coordinatorContext.queueTableDeletion(Collections.singleton(tableId));
        tableManager.onDeleteTable(tableId);

        // shutdown table manager
        tableManager.shutdown();

        // restart table manager, it should resume table delete
        // set coordinator context manually to make sure the followup delete can success
        List<ServerInfo> serverInfos = CoordinatorTestUtils.createServers(Arrays.asList(0, 1, 2));
        // set live tablet servers
        coordinatorContext.setLiveTabletServers(serverInfos);
        CoordinatorTestUtils.makeSendLeaderAndStopRequestAlwaysSuccess(
                coordinatorContext, testCoordinatorChannelManager);

        // update assignment to coordinator context
        for (int bucketId : assignment.getBuckets()) {
            TableBucket tableBucket = new TableBucket(tableId, bucketId);
            List<Integer> replicas = assignment.getBucketAssignment(bucketId).getReplicas();
            coordinatorContext.updateBucketReplicaAssignment(tableBucket, replicas);
        }
        // queue table deletion
        coordinatorContext.queueTableDeletion(Collections.singleton(tableId));

        // start table manager, should resume table deletion
        tableManager.startup();
        // TableLifecycleThrottler defers resume actions to the coordinator event thread by
        // enqueuing ResumeDropEvent; the unit test has no real event loop so dispatch them inline.
        dispatchResumeDropEvents();

        checkReplicaDelete(tableId, null, assignment);
    }

    @Test
    void testDeleteTableWithTransportFailureThenSuccess() throws Exception {
        // A transport-level loss of stopReplica(delete=true) must feed back into the deletion
        // retry chain instead of wedging the table in ReplicaDeletionStarted forever.
        AtomicBoolean failDeleteTransport = new AtomicBoolean(true);
        Map<Integer, TabletServerGateway> gateways = new HashMap<>();
        for (int serverId : Arrays.asList(0, 1, 2)) {
            gateways.put(serverId, new TransportFailingGateway(failDeleteTransport));
        }
        testCoordinatorChannelManager.setGateways(gateways);

        // first, create a table
        long tableId = zookeeperClient.getTableIdAndIncrement();
        TableAssignment assignment = createAssignment();
        zookeeperClient.registerTableAssignment(tableId, assignment);

        coordinatorContext.putTableInfo(
                TableInfo.of(
                        DATA1_TABLE_PATH_PK,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR_PK,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        tableManager.onCreateNewTable(DATA1_TABLE_PATH_PK, tableId, assignment);

        // now, delete the created table; every stopReplica(delete=true) send fails at transport
        // level
        coordinatorContext.queueTableDeletion(Collections.singleton(tableId));
        tableManager.onDeleteTable(tableId);

        // all replicas advanced to ReplicaDeletionStarted, but nothing was acked
        Set<TableBucketReplica> replicas = getReplicas(tableId, assignment);
        for (TableBucketReplica replica : replicas) {
            assertThat(coordinatorContext.getReplicaState(replica))
                    .isEqualTo(ReplicaDeletionStarted);
        }

        // the transport failures must have been fed back as failed delete results
        List<DeleteReplicaResultForBucket> failedResults = collectDeleteResults();
        assertThat(failedResults).hasSize(replicas.size());
        for (DeleteReplicaResultForBucket result : failedResults) {
            assertThat(result.failed()).isTrue();
        }

        // the tablet servers are reachable again; drive the retry chain the coordinator event
        // thread would run (see CoordinatorEventProcessor#processDeleteReplicaResponseReceived)
        failDeleteTransport.set(false);
        dispatchDeleteReplicaEvents();
        // the retry re-sent stopReplica(delete=true), which now succeeds
        dispatchDeleteReplicaEvents();

        // all replicas must now be deletion-successful ...
        for (TableBucketReplica replica : replicas) {
            assertThat(coordinatorContext.getReplicaState(replica))
                    .isEqualTo(ReplicaDeletionSuccessful);
        }
        // ... and the table deletion completes: ZK assignment gone, context cleaned up
        retry(
                Duration.ofSeconds(30),
                () -> assertThat(zookeeperClient.getTableAssignment(tableId)).isEmpty());
        assertThat(coordinatorContext.getAllReplicasForTable(tableId)).isEmpty();
    }

    @Test
    void testResumeDeletionsDefersTableWithReplicaInDeletionStarted() throws Exception {
        // A table with any replica left in ReplicaDeletionStarted must stay deferred: resume must
        // neither complete the deletion nor re-submit it (Kafka-ported gate against duplicate
        // delete storms). Recovery of the stuck replica is owned by the delete-replica retry
        // chain, not by resume.
        long tableId = zookeeperClient.getTableIdAndIncrement();
        TableAssignment assignment = createAssignment();
        zookeeperClient.registerTableAssignment(tableId, assignment);

        coordinatorContext.putTableInfo(
                TableInfo.of(
                        DATA1_TABLE_PATH_PK,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR_PK,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        tableManager.onCreateNewTable(DATA1_TABLE_PATH_PK, tableId, assignment);

        coordinatorContext.queueTableDeletion(Collections.singleton(tableId));
        tableManager.onDeleteTable(tableId);

        // leave the replicas in ReplicaDeletionStarted: drop the responses without driving them,
        // as if the acks were lost
        testingEventManager.clearEvents();

        tableManager.resumeDeletions();

        // the gate defers: no resume re-submitted ...
        for (CoordinatorEvent event : testingEventManager.getEvents()) {
            assertThat(event).isNotInstanceOf(ResumeDropEvent.class);
        }
        // ... and the deletion is not completed either
        assertThat(coordinatorContext.getTablesToBeDeleted()).contains(tableId);
        assertThat(coordinatorContext.getAllReplicasForTable(tableId)).isNotEmpty();
        assertThat(zookeeperClient.getTableAssignment(tableId)).isPresent();

        zookeeperClient.deleteTableAssignment(tableId);
    }

    @Test
    void testCreateAndDropPartition() throws Exception {
        // create a table
        long tableId = zookeeperClient.getTableIdAndIncrement();
        TableAssignment assignment = TableAssignment.builder().build();
        zookeeperClient.registerTableAssignment(tableId, assignment);

        coordinatorContext.putTableInfo(
                TableInfo.of(
                        DATA1_TABLE_PATH,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        tableManager.onCreateNewTable(DATA1_TABLE_PATH, tableId, assignment);

        PartitionAssignment partitionAssignment =
                new PartitionAssignment(tableId, createAssignment().getBucketAssignments());
        String partitionName = "2024";
        long partitionId = zookeeperClient.getPartitionIdAndIncrement();
        zookeeperClient.registerPartitionAssignmentAndMetadata(
                partitionId,
                partitionName,
                partitionAssignment,
                DEFAULT_REMOTE_DATA_DIR,
                DATA1_TABLE_PATH,
                tableId,
                partitionAssignment.getBucketAssignments().size());

        // create partition
        tableManager.onCreateNewPartition(
                DATA1_TABLE_PATH, tableId, partitionId, partitionName, partitionAssignment);

        // all replicas should be online
        checkReplicaOnline(tableId, partitionId, partitionAssignment);

        // drop partition
        // all replicas should be deleted
        coordinatorContext.queuePartitionDeletion(
                Collections.singleton(new TablePartition(tableId, partitionId)));
        tableManager.onDeletePartition(tableId, partitionId);
        checkReplicaDelete(tableId, partitionId, partitionAssignment);

        // mark all replica as delete
        for (TableBucketReplica replica : getReplicas(tableId, partitionId, partitionAssignment)) {
            coordinatorContext.putReplicaState(replica, ReplicaDeletionSuccessful);
        }

        // call method resumeDeletions, should delete the assignments from zk
        tableManager.resumeDeletions();
        // retry for async deletion of PartitionAssignment
        retry(
                Duration.ofSeconds(30),
                () -> assertThat(zookeeperClient.getPartitionAssignment(partitionId)).isEmpty());
        // the partition will also be removed from coordinator context
        assertThat(coordinatorContext.getAllReplicasForPartition(tableId, partitionId)).isEmpty();
    }

    private TableAssignment createAssignment() {
        return TableAssignment.builder()
                .add(0, BucketAssignment.of(0, 1, 2))
                .add(1, BucketAssignment.of(1, 2, 0))
                .add(2, BucketAssignment.of(2, 1, 0))
                .build();
    }

    private void checkReplicaOnline(
            long tableId, @Nullable Long partitionId, TableAssignment tableAssignment)
            throws Exception {
        for (Map.Entry<Integer, BucketAssignment> entry :
                tableAssignment.getBucketAssignments().entrySet()) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, entry.getKey());
            List<Integer> replicas = entry.getValue().getReplicas();
            assertThat(coordinatorContext.getBucketState(tableBucket)).isEqualTo(OnlineBucket);
            // check the leader/epoch of each bucket
            CoordinatorTestUtils.checkLeaderAndIsr(
                    zookeeperClient, tableBucket, 0, replicas.get(0));
            for (int replica : replicas) {
                TableBucketReplica tableBucketReplica =
                        new TableBucketReplica(tableBucket, replica);
                assertThat(coordinatorContext.getReplicaState(tableBucketReplica))
                        .isEqualTo(OnlineReplica);
            }
        }
    }

    private void checkReplicaDelete(
            long tableId, @Nullable Long partitionId, TableAssignment assignment) {
        // collect all the delete success event
        Set<DeleteReplicaResponseReceivedEvent> deleteReplicaSuccessEvents =
                collectDeleteReplicaSuccessEvents();
        Set<TableBucketReplica> deleteTableBucketReplicas = new HashSet<>();
        // get all the delete success replicas from the delete success event
        for (DeleteReplicaResponseReceivedEvent deleteReplicaResponseReceivedEvent :
                deleteReplicaSuccessEvents) {
            List<DeleteReplicaResultForBucket> deleteReplicaResultForBuckets =
                    deleteReplicaResponseReceivedEvent.getDeleteReplicaResults();
            for (DeleteReplicaResultForBucket deleteReplicaResultForBucket :
                    deleteReplicaResultForBuckets) {
                if (deleteReplicaResultForBucket.succeeded()) {
                    deleteTableBucketReplicas.add(
                            deleteReplicaResultForBucket.getTableBucketReplica());
                }
            }
        }

        // get all the expected delete success replicas
        Set<TableBucketReplica> expectedDeleteTableBucketReplicas = new HashSet<>();
        for (int bucketId : assignment.getBuckets()) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
            List<Integer> replicas = assignment.getBucketAssignment(bucketId).getReplicas();
            for (int replica : replicas) {
                expectedDeleteTableBucketReplicas.add(new TableBucketReplica(tableBucket, replica));
            }
        }
        assertThat(deleteTableBucketReplicas).isEqualTo(expectedDeleteTableBucketReplicas);
    }

    private Set<TableBucketReplica> getReplicas(long tableId, TableAssignment assignment) {
        return getReplicas(tableId, null, assignment);
    }

    private Set<TableBucketReplica> getReplicas(
            long tableId, Long partitionId, TableAssignment assignment) {
        Set<TableBucketReplica> tableBucketReplicas = new HashSet<>();
        for (int bucketId : assignment.getBuckets()) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
            List<Integer> replicas = assignment.getBucketAssignment(bucketId).getReplicas();
            for (int replica : replicas) {
                tableBucketReplicas.add(new TableBucketReplica(tableBucket, replica));
            }
        }
        return tableBucketReplicas;
    }

    private Set<DeleteReplicaResponseReceivedEvent> collectDeleteReplicaSuccessEvents() {
        Set<DeleteReplicaResponseReceivedEvent> deleteReplicaResponseReceivedEvent =
                new HashSet<>();
        for (CoordinatorEvent coordinatorEvent : testingEventManager.getEvents()) {
            if (coordinatorEvent instanceof DeleteReplicaResponseReceivedEvent) {
                deleteReplicaResponseReceivedEvent.add(
                        (DeleteReplicaResponseReceivedEvent) coordinatorEvent);
            }
        }
        return deleteReplicaResponseReceivedEvent;
    }

    private List<DeleteReplicaResultForBucket> collectDeleteResults() {
        List<DeleteReplicaResultForBucket> results = new ArrayList<>();
        for (CoordinatorEvent event : testingEventManager.getEvents()) {
            if (event instanceof DeleteReplicaResponseReceivedEvent) {
                results.addAll(
                        ((DeleteReplicaResponseReceivedEvent) event).getDeleteReplicaResults());
            }
        }
        return results;
    }

    /**
     * Drives every pending {@link DeleteReplicaResponseReceivedEvent} through the same steps as
     * {@code CoordinatorEventProcessor#processDeleteReplicaResponseReceived} (clear success counts,
     * retry failures via ReplicaDeletionStarted, mark successes as ReplicaDeletionSuccessful,
     * resume on success) so unit tests can run the deletion retry chain without the real event
     * loop.
     */
    private void dispatchDeleteReplicaEvents() {
        List<CoordinatorEvent> pending = new ArrayList<>(testingEventManager.getEvents());
        testingEventManager.clearEvents();
        Set<TableBucketReplica> failDeletedReplicas = new HashSet<>();
        Set<TableBucketReplica> successDeletedReplicas = new HashSet<>();
        for (CoordinatorEvent event : pending) {
            if (event instanceof DeleteReplicaResponseReceivedEvent) {
                for (DeleteReplicaResultForBucket result :
                        ((DeleteReplicaResponseReceivedEvent) event).getDeleteReplicaResults()) {
                    if (result.succeeded()) {
                        successDeletedReplicas.add(result.getTableBucketReplica());
                    } else {
                        failDeletedReplicas.add(result.getTableBucketReplica());
                    }
                }
            }
        }
        if (failDeletedReplicas.isEmpty() && successDeletedReplicas.isEmpty()) {
            return;
        }
        coordinatorContext.clearFailDeleteNumbers(successDeletedReplicas);
        Tuple2<Set<TableBucketReplica>, Set<TableBucketReplica>> retryAndSuccess =
                coordinatorContext.retryDeleteAndSuccessDeleteReplicas(failDeletedReplicas);
        replicaStateMachine.handleStateChanges(retryAndSuccess.f0, ReplicaDeletionStarted);
        successDeletedReplicas.addAll(retryAndSuccess.f1);
        replicaStateMachine.handleStateChanges(successDeletedReplicas, ReplicaDeletionSuccessful);
        if (!successDeletedReplicas.isEmpty()) {
            tableManager.resumeDeletions();
        }
    }

    /**
     * A tablet gateway that fails stopReplica(delete=true) sends at transport level while the flag
     * is set, then behaves like the always-success gateway. Per-bucket tablet-side errors are
     * unaffected: only transport-level loss is simulated here.
     */
    private static final class TransportFailingGateway extends TestTabletServerGateway {
        private final AtomicBoolean failDeleteTransport;

        private TransportFailingGateway(AtomicBoolean failDeleteTransport) {
            super(false, Collections.emptySet());
            this.failDeleteTransport = failDeleteTransport;
        }

        @Override
        public CompletableFuture<StopReplicaResponse> stopReplica(StopReplicaRequest request) {
            boolean isDelete = false;
            for (PbStopReplicaReqForBucket bucket : request.getStopReplicasReqsList()) {
                if (bucket.isDelete() && bucket.isDeleteRemote()) {
                    isDelete = true;
                    break;
                }
            }
            if (isDelete && failDeleteTransport.get()) {
                CompletableFuture<StopReplicaResponse> future = new CompletableFuture<>();
                future.completeExceptionally(
                        new NetworkException("Simulated stop replica transport failure."));
                return future;
            }
            return super.stopReplica(request);
        }
    }

    /**
     * Drives every {@link ResumeDropEvent} currently in the testing event manager. Mirrors the
     * dispatch logic of {@code CoordinatorEventProcessor#process(CoordinatorEvent)} so unit tests
     * can trigger the deferred resume reconciliation without spinning up the real event loop.
     */
    private void dispatchResumeDropEvents() {
        for (CoordinatorEvent event : testingEventManager.getEvents()) {
            if (event instanceof ResumeDropEvent) {
                ResumeDropEvent resumeEvent = (ResumeDropEvent) event;
                if (resumeEvent.getPartitionId() == null) {
                    tableManager.onDeleteTable(resumeEvent.getTableId());
                } else {
                    tableManager.onDeletePartition(
                            resumeEvent.getTableId(), resumeEvent.getPartitionId());
                }
            }
        }
    }
}
