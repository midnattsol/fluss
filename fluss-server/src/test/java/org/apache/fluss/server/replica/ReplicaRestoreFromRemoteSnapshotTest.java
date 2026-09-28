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

package org.apache.fluss.server.replica;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketSnapshot;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.rpc.messages.AcquireKvSnapshotLeaseRequest;
import org.apache.fluss.rpc.messages.AcquireKvSnapshotLeaseResponse;
import org.apache.fluss.rpc.messages.ReleaseKvSnapshotLeaseRequest;
import org.apache.fluss.rpc.messages.ReleaseKvSnapshotLeaseResponse;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseManager;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.kv.KvSnapshotResource;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.DefaultSnapshotContext;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataDownloader;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDownloadSpec;
import org.apache.fluss.server.kv.snapshot.RemoteSnapshotLease;
import org.apache.fluss.server.kv.snapshot.SnapshotContext;
import org.apache.fluss.server.kv.snapshot.TestingCompletedKvSnapshotCommitter;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.utils.ServerRpcMessageUtils;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for restoring an empty kv replica from a remote snapshot.
 *
 * <p>The tests simulate total local loss: a leader writes data and takes a snapshot, its local kv
 * state is then destroyed, and a fresh replica (empty local snapshot view) becomes leader. The
 * remote snapshot is provided through a dedicated remote store to model snapshots that only exist
 * in remote storage.
 */
class ReplicaRestoreFromRemoteSnapshotTest extends ReplicaTestBase {

    private static final TableBucket TABLE_BUCKET = new TableBucket(DATA1_TABLE_ID_PK, 1);

    @Test
    void testRestoreFromRemoteSnapshot(@TempDir File remoteDir) throws Exception {
        RemoteRestoreSnapshotContext context = new RemoteRestoreSnapshotContext(remoteDir);
        CompletedSnapshot remoteSnapshot = writeDataAndTakeSnapshot(context);
        // Writes after the snapshot must be recovered by replaying the log from its offset.
        putRecordsToLeader(
                context.replica, genKvRecordBatch(Tuple2.of("k3", new Object[] {3, "c"})));
        wipeLocalKvState(context.replica);

        RemoteRestoreSnapshotContext freshContext = new RemoteRestoreSnapshotContext(remoteDir);
        freshContext.remoteSnapshotStore.commitKvSnapshot(remoteSnapshot, 0, 0);
        Replica restored = makeFreshLeader(freshContext);

        assertThat(restored.getKvTablet()).isNotNull();
        // Flush the replayed log so that all restored data is visible to lookups.
        flushAndWait(restored.getKvTablet(), restored.getLocalLogEndOffset());
        List<Tuple2<byte[], byte[]>> expectedKeyValues =
                getKeyValuePairs(
                        genKvRecords(
                                Tuple2.of("k1", new Object[] {1, "a"}),
                                Tuple2.of("k2", new Object[] {2, "b"}),
                                Tuple2.of("k3", new Object[] {3, "c"})));
        assertHasKeyValues(restored.getKvTablet(), expectedKeyValues);
    }

    @Test
    void testNoRemoteSnapshotKeepsRestoreFromLog(@TempDir File remoteDir) throws Exception {
        RemoteRestoreSnapshotContext freshContext = new RemoteRestoreSnapshotContext(remoteDir);
        Replica restored = makeFreshLeader(freshContext);

        // Current behavior is untouched: a healthy but empty tablet, no download, no lease.
        assertThat(restored.getKvTablet()).isNotNull();
        assertKeyMissing(restored.getKvTablet(), Tuple2.of("k1", new Object[] {1, "a"}));
        assertThat(freshContext.downloadEvents).isEmpty();
        assertThat(freshContext.leaseEvents).isEmpty();
    }

    @Test
    void testLeaseAcquiredBeforeDownloadAndReleasedAfter(@TempDir File remoteDir) throws Exception {
        RemoteRestoreSnapshotContext context = new RemoteRestoreSnapshotContext(remoteDir);
        CompletedSnapshot remoteSnapshot = writeDataAndTakeSnapshot(context);
        wipeLocalKvState(context.replica);

        RemoteRestoreSnapshotContext freshContext = new RemoteRestoreSnapshotContext(remoteDir);
        freshContext.remoteSnapshotStore.commitKvSnapshot(remoteSnapshot, 0, 0);
        Replica restored = makeFreshLeader(freshContext);

        assertThat(restored.getKvTablet()).isNotNull();
        long snapshotId = remoteSnapshot.getSnapshotID();
        assertThat(freshContext.orderedEvents)
                .containsExactly("acquire:" + snapshotId, "download", "release:" + snapshotId);
        assertThat(freshContext.activeLeases.get()).isEqualTo(0);
    }

    @Test
    void testRemoteRestoreDisabled(@TempDir File remoteDir) throws Exception {
        RemoteRestoreSnapshotContext context = new RemoteRestoreSnapshotContext(remoteDir);
        CompletedSnapshot remoteSnapshot = writeDataAndTakeSnapshot(context);
        wipeLocalKvState(context.replica);

        RemoteRestoreSnapshotContext freshContext = new RemoteRestoreSnapshotContext(remoteDir);
        freshContext.remoteSnapshotStore.commitKvSnapshot(remoteSnapshot, 0, 0);
        freshContext.remoteRestoreEnabled = false;
        Replica restored = makeFreshLeader(freshContext);

        // Kill-switch off: current behavior even with a remote snapshot present.
        assertThat(restored.getKvTablet()).isNotNull();
        assertKeyMissing(restored.getKvTablet(), Tuple2.of("k1", new Object[] {1, "a"}));
        assertThat(freshContext.downloadEvents).isEmpty();
        assertThat(freshContext.leaseEvents).isEmpty();
    }

    @Test
    void testLeaseViaCoordinatorVisibleToCoordinatorManager() throws Exception {
        KvSnapshotLeaseManager leaseManager = newCoordinatorLeaseManager();
        LeaseForwardingGateway gateway = new LeaseForwardingGateway(leaseManager);
        DefaultSnapshotContext context = newDefaultSnapshotContext(gateway);
        long leaseDurationMs = TimeUnit.MINUTES.toMillis(30);

        RemoteSnapshotLease lease =
                context.acquireRemoteSnapshotLease(TABLE_BUCKET, 7L, leaseDurationMs);

        // The lease went through the coordinator RPC path, so the coordinator-side in-memory
        // manager protects the snapshot. A direct-ZK write would stay invisible here.
        assertThat(leaseManager.snapshotLeaseExist(new TableBucketSnapshot(TABLE_BUCKET, 7L)))
                .isTrue();
        assertThat(gateway.lastLeaseId).startsWith("restore-");
        assertThat(gateway.lastLeaseDurationMs).isEqualTo(leaseDurationMs);

        lease.close();

        // The matching release went through the coordinator as well: the in-memory ref-count is
        // gone and the ZK lease record is dropped.
        assertThat(leaseManager.snapshotLeaseExist(new TableBucketSnapshot(TABLE_BUCKET, 7L)))
                .isFalse();
        assertThat(leaseManager.getLease(gateway.lastLeaseId).isPresent()).isFalse();
    }

    @Test
    void testS3ListingFallbackRecoversWhenZkEmpty() throws Exception {
        // Real snapshot flow writes files into the production remote layout. The ZK snapshot
        // handle store stays empty: snapshots are only committed to the fake local store.
        TestSnapshotContext setupContext = new TestSnapshotContext(productionRemoteKvDir());
        Replica setupReplica = startLeaderWithSnapshotData(setupContext);
        CompletedSnapshot remoteSnapshot =
                setupContext.testKvSnapshotStore.waitUntilSnapshotComplete(TABLE_BUCKET, 0);
        // Writes after the snapshot must be recovered by replaying the log from its offset.
        putRecordsToLeader(setupReplica, genKvRecordBatch(Tuple2.of("k3", new Object[] {3, "c"})));
        wipeLocalKvState(setupReplica);

        // A partially uploaded snapshot (directory without _METADATA yet) must be skipped.
        String tabletDir =
                FlussPaths.remoteKvTabletDir(
                                FlussPaths.remoteKvDir(conf),
                                DATA1_PHYSICAL_TABLE_PATH_PK,
                                TABLE_BUCKET)
                        .toString();
        assertThat(new File(tabletDir, "snap-999").mkdirs()).isTrue();

        KvSnapshotLeaseManager leaseManager = newCoordinatorLeaseManager();
        LeaseForwardingGateway gateway = new LeaseForwardingGateway(leaseManager);
        DefaultSnapshotContext freshContext = newDefaultSnapshotContext(gateway);

        // ZK yields nothing, so discovery falls back to listing remote storage and parses the
        // snapshot metadata with the existing serde: assert parsed fields, not strings.
        Optional<CompletedSnapshot> discovered =
                freshContext.getLatestRemoteSnapshot(DATA1_PHYSICAL_TABLE_PATH_PK, TABLE_BUCKET);
        assertThat(discovered.isPresent()).isTrue();
        assertThat(discovered.get().getSnapshotID()).isEqualTo(remoteSnapshot.getSnapshotID());
        assertThat(discovered.get().getLogOffset()).isEqualTo(remoteSnapshot.getLogOffset());
        assertThat(discovered.get().getTableBucket()).isEqualTo(TABLE_BUCKET);
        assertThat(discovered.get().getRowCount()).isEqualTo(remoteSnapshot.getRowCount());

        Replica restored = makeFreshLeader(freshContext);

        assertThat(restored.getKvTablet()).isNotNull();
        flushAndWait(restored.getKvTablet(), restored.getLocalLogEndOffset());
        List<Tuple2<byte[], byte[]>> expectedKeyValues =
                getKeyValuePairs(
                        genKvRecords(
                                Tuple2.of("k1", new Object[] {1, "a"}),
                                Tuple2.of("k2", new Object[] {2, "b"}),
                                Tuple2.of("k3", new Object[] {3, "c"})));
        assertHasKeyValues(restored.getKvTablet(), expectedKeyValues);
        // The lease pinned the download through the coordinator and is released afterwards.
        assertThat(
                        leaseManager.snapshotLeaseExist(
                                new TableBucketSnapshot(
                                        TABLE_BUCKET, remoteSnapshot.getSnapshotID())))
                .isFalse();
    }

    @Test
    void testS3EmptyStaysHealthyEmpty() throws Exception {
        DefaultSnapshotContext freshContext = newDefaultSnapshotContext(null);

        // No snapshot anywhere: ZK is empty and remote storage holds nothing.
        assertThat(
                        freshContext
                                .getLatestRemoteSnapshot(DATA1_PHYSICAL_TABLE_PATH_PK, TABLE_BUCKET)
                                .isPresent())
                .isFalse();

        Replica restored = makeFreshLeader(freshContext);

        // Current healthy-empty behavior is untouched: an empty tablet, restored from the log.
        assertThat(restored.getKvTablet()).isNotNull();
        assertKeyMissing(restored.getKvTablet(), Tuple2.of("k1", new Object[] {1, "a"}));
    }

    private CompletedSnapshot writeDataAndTakeSnapshot(RemoteRestoreSnapshotContext context)
            throws Exception {
        Replica replica = startLeaderWithSnapshotData(context);
        context.replica = replica;
        return context.testKvSnapshotStore.waitUntilSnapshotComplete(TABLE_BUCKET, 0);
    }

    private Replica startLeaderWithSnapshotData(TestSnapshotContext context) throws Exception {
        Replica replica = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, TABLE_BUCKET, context);
        makeKvReplicaAsLeader(replica, INITIAL_LEADER_EPOCH);
        putRecordsToLeader(
                replica,
                genKvRecordBatch(
                        Tuple2.of("k1", new Object[] {1, "a"}),
                        Tuple2.of("k2", new Object[] {2, "b"})));
        context.scheduledExecutorService.triggerAllNonPeriodicTasks();
        return replica;
    }

    private Replica makeFreshLeader(SnapshotContext freshContext) throws Exception {
        Replica restored = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, TABLE_BUCKET, freshContext);
        makeKvReplicaAsLeader(restored, INITIAL_LEADER_EPOCH + 1);
        return restored;
    }

    private void wipeLocalKvState(Replica replica) {
        makeKvReplicaAsFollower(replica, INITIAL_LEADER_EPOCH);
        assertThat(replica.getKvTablet()).isNull();
    }

    private void makeKvReplicaAsLeader(Replica replica, int leaderEpoch) throws Exception {
        makeLeaderReplica(replica, DATA1_TABLE_PATH_PK, TABLE_BUCKET, leaderEpoch);
    }

    private void makeKvReplicaAsFollower(Replica replica, int leaderEpoch) {
        replica.makeFollower(
                new NotifyLeaderAndIsrData(
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                        TABLE_BUCKET,
                        Collections.singletonList(TABLET_SERVER_ID),
                        new LeaderAndIsr(
                                TABLET_SERVER_ID,
                                leaderEpoch,
                                Collections.singletonList(TABLET_SERVER_ID),
                                Collections.emptyList(),
                                INITIAL_COORDINATOR_EPOCH,
                                // we also use the leader epoch as bucket epoch
                                leaderEpoch),
                        3,
                        0L));
    }

    private void makeLeaderReplica(
            Replica replica, TablePath tablePath, TableBucket tableBucket, int leaderEpoch)
            throws Exception {
        replica.makeLeader(
                new NotifyLeaderAndIsrData(
                        PhysicalTablePath.of(tablePath),
                        tableBucket,
                        Collections.singletonList(TABLET_SERVER_ID),
                        new LeaderAndIsr(
                                TABLET_SERVER_ID,
                                leaderEpoch,
                                Collections.singletonList(TABLET_SERVER_ID),
                                Collections.emptyList(),
                                INITIAL_COORDINATOR_EPOCH,
                                // we also use the leader epoch as bucket epoch
                                leaderEpoch),
                        3,
                        0L));
    }

    private LogAppendInfo putRecordsToLeader(Replica replica, KvRecordBatch kvRecords)
            throws Exception {
        LogAppendInfo logAppendInfo =
                replica.putRecordsToLeader(kvRecords, null, MergeMode.DEFAULT, 0);
        KvTablet kvTablet = replica.getKvTablet();
        assertThat(kvTablet).isNotNull();
        // flush to make data visible
        flushAndWait(kvTablet, replica.getLocalLogEndOffset());
        return logAppendInfo;
    }

    private void assertHasKeyValues(
            KvTablet kvTablet, List<Tuple2<byte[], byte[]>> expectedKeyValues) throws IOException {
        List<byte[]> keys = new ArrayList<>();
        List<byte[]> expectedValues = new ArrayList<>();
        for (Tuple2<byte[], byte[]> expectedKeyValue : expectedKeyValues) {
            keys.add(expectedKeyValue.f0);
            expectedValues.add(expectedKeyValue.f1);
        }
        assertThat(
                        kvTablet.multiGet(keys).stream()
                                .map(ByteArraySlice::toByteArray)
                                .collect(Collectors.toList()))
                .containsExactlyElementsOf(expectedValues);
    }

    private void assertKeyMissing(KvTablet kvTablet, Tuple2<String, Object[]> record)
            throws IOException {
        byte[] key = getKeyValuePairs(genKvRecords(record)).get(0).f0;
        List<ByteArraySlice> values = kvTablet.multiGet(Collections.singletonList(key));
        assertThat(values).hasSize(1);
        assertThat(values.get(0)).isNull();
    }

    private final List<ExecutorService> testExecutors = new ArrayList<>();
    private final List<KvSnapshotResource> testResources = new ArrayList<>();

    @AfterEach
    void tearDownRestoreFixtures() {
        for (KvSnapshotResource resource : testResources) {
            resource.close();
        }
        testResources.clear();
        for (ExecutorService executor : testExecutors) {
            executor.shutdownNow();
        }
        testExecutors.clear();
    }

    /**
     * The production remote kv dir derived from the test configuration, so that snapshots written
     * by a {@link TestSnapshotContext} land exactly where {@link DefaultSnapshotContext} lists
     * them.
     */
    private String productionRemoteKvDir() {
        String dir = FlussPaths.remoteKvDir(conf).toString();
        new File(dir).mkdirs();
        return dir;
    }

    private DefaultSnapshotContext newDefaultSnapshotContext(
            @Nullable TestCoordinatorGateway gateway) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        testExecutors.add(executor);
        KvSnapshotResource resource = KvSnapshotResource.create(TABLET_SERVER_ID, conf, executor);
        testResources.add(resource);
        return DefaultSnapshotContext.create(
                zkClient, new TestingCompletedKvSnapshotCommitter(), resource, conf, gateway);
    }

    private KvSnapshotLeaseManager newCoordinatorLeaseManager() {
        return new KvSnapshotLeaseManager(
                Duration.ofDays(7).toMillis(),
                zkClient,
                new File(tempDir, "lease-metadata").getAbsolutePath(),
                new ManuallyTriggeredScheduledExecutorService(),
                new ManualClock(System.currentTimeMillis()),
                TestingMetricGroups.COORDINATOR_METRICS);
    }

    /**
     * A {@link TestCoordinatorGateway} serving lease RPCs with a real coordinator-side {@link
     * KvSnapshotLeaseManager}, emulating {@code CoordinatorService} through the same {@link
     * ServerRpcMessageUtils} helpers. Seam-level tests use it to prove that leases acquired through
     * the coordinator path are visible to the coordinator's in-memory manager.
     */
    private static class LeaseForwardingGateway extends TestCoordinatorGateway {
        private final KvSnapshotLeaseManager leaseManager;
        private String lastLeaseId;
        private long lastLeaseDurationMs;

        LeaseForwardingGateway(KvSnapshotLeaseManager leaseManager) {
            this.leaseManager = leaseManager;
        }

        @Override
        public CompletableFuture<AcquireKvSnapshotLeaseResponse> acquireKvSnapshotLease(
                AcquireKvSnapshotLeaseRequest request) {
            try {
                lastLeaseId = request.getLeaseId();
                lastLeaseDurationMs = request.getLeaseDurationMs();
                Map<TableBucket, Long> unavailable =
                        leaseManager.acquireLease(
                                request.getLeaseId(),
                                request.getLeaseDurationMs(),
                                ServerRpcMessageUtils.getAcquireKvSnapshotLeaseData(request));
                return CompletableFuture.completedFuture(
                        ServerRpcMessageUtils.makeAcquireKvSnapshotLeaseResponse(unavailable));
            } catch (Exception e) {
                CompletableFuture<AcquireKvSnapshotLeaseResponse> future =
                        new CompletableFuture<>();
                future.completeExceptionally(e);
                return future;
            }
        }

        @Override
        public CompletableFuture<ReleaseKvSnapshotLeaseResponse> releaseKvSnapshotLease(
                ReleaseKvSnapshotLeaseRequest request) {
            try {
                leaseManager.release(
                        request.getLeaseId(),
                        ServerRpcMessageUtils.getReleaseKvSnapshotLeaseData(request));
                return CompletableFuture.completedFuture(new ReleaseKvSnapshotLeaseResponse());
            } catch (Exception e) {
                CompletableFuture<ReleaseKvSnapshotLeaseResponse> future =
                        new CompletableFuture<>();
                future.completeExceptionally(e);
                return future;
            }
        }
    }

    /**
     * A {@link TestSnapshotContext} with a dedicated remote snapshot store plus lease/download
     * tracking. The inherited {@code testKvSnapshotStore} models the local snapshot view, while
     * {@code remoteSnapshotStore} models snapshots that only exist in remote storage.
     */
    private class RemoteRestoreSnapshotContext extends TestSnapshotContext {
        private final TestingCompletedKvSnapshotCommitter remoteSnapshotStore =
                new TestingCompletedKvSnapshotCommitter();
        private final List<String> orderedEvents = new ArrayList<>();
        private final List<String> leaseEvents = new ArrayList<>();
        private final List<String> downloadEvents = new ArrayList<>();
        private final AtomicInteger activeLeases = new AtomicInteger(0);
        private boolean remoteRestoreEnabled = true;
        private Replica replica;

        RemoteRestoreSnapshotContext(File remoteDir) throws Exception {
            super(remoteDir.getPath());
        }

        @Override
        public boolean isRemoteSnapshotRestoreEnabled() {
            return remoteRestoreEnabled;
        }

        @Override
        public Optional<CompletedSnapshot> getLatestRemoteSnapshot(
                PhysicalTablePath physicalPath, TableBucket tableBucket) {
            return Optional.ofNullable(remoteSnapshotStore.getLatestCompletedSnapshot(tableBucket));
        }

        @Override
        public RemoteSnapshotLease acquireRemoteSnapshotLease(
                TableBucket tableBucket, long snapshotId, long leaseDurationMs) {
            leaseEvents.add("acquire:" + snapshotId);
            orderedEvents.add("acquire:" + snapshotId);
            activeLeases.incrementAndGet();
            return () -> {
                leaseEvents.add("release:" + snapshotId);
                orderedEvents.add("release:" + snapshotId);
                activeLeases.decrementAndGet();
            };
        }

        @Override
        public KvSnapshotDataDownloader getSnapshotDataDownloader() {
            KvSnapshotDataDownloader delegate = super.getSnapshotDataDownloader();
            return new KvSnapshotDataDownloader(executorService) {
                @Override
                public void transferAllDataToDirectory(
                        KvSnapshotDownloadSpec downloadSpec, CloseableRegistry closeableRegistry)
                        throws Exception {
                    downloadEvents.add("download");
                    orderedEvents.add("download");
                    delegate.transferAllDataToDirectory(downloadSpec, closeableRegistry);
                }
            };
        }
    }
}
