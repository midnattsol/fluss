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
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataDownloader;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDownloadSpec;
import org.apache.fluss.server.kv.snapshot.RemoteSnapshotLease;
import org.apache.fluss.server.kv.snapshot.TestingCompletedKvSnapshotCommitter;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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

    private CompletedSnapshot writeDataAndTakeSnapshot(RemoteRestoreSnapshotContext context)
            throws Exception {
        Replica replica = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, TABLE_BUCKET, context);
        context.replica = replica;
        makeKvReplicaAsLeader(replica, INITIAL_LEADER_EPOCH);
        putRecordsToLeader(
                replica,
                genKvRecordBatch(
                        Tuple2.of("k1", new Object[] {1, "a"}),
                        Tuple2.of("k2", new Object[] {2, "b"})));
        context.scheduledExecutorService.triggerAllNonPeriodicTasks();
        return context.testKvSnapshotStore.waitUntilSnapshotComplete(TABLE_BUCKET, 0);
    }

    private Replica makeFreshLeader(RemoteRestoreSnapshotContext freshContext) throws Exception {
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
        public Optional<CompletedSnapshot> getLatestRemoteSnapshot(TableBucket tableBucket) {
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
