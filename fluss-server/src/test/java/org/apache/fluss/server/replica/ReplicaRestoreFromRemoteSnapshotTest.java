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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketSnapshot;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils.KvRecordBatchFactory;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.remote.RemoteLogSegment;
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
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import org.apache.fluss.server.kv.snapshot.DefaultSnapshotContext;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataDownloader;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDownloadSpec;
import org.apache.fluss.server.kv.snapshot.RemoteSnapshotLease;
import org.apache.fluss.server.kv.snapshot.SnapshotContext;
import org.apache.fluss.server.kv.snapshot.TestingCompletedKvSnapshotCommitter;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogSegment;
import org.apache.fluss.server.log.remote.LogSegmentFiles;
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
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Override
    protected Configuration getServerConf() {
        Configuration config = super.getServerConf();
        config.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofHours(1));
        return config;
    }

    @Test
    void testRestoreFromRemoteSnapshot(@TempDir File remoteDir) throws Exception {
        RemoteRestoreSnapshotContext context = new RemoteRestoreSnapshotContext(remoteDir);
        CompletedSnapshot remoteSnapshot = writeDataAndTakeSnapshot(context);
        // Writes after the snapshot must be recovered by replaying the log from its offset.
        putRecordsToLeader(context.replica, genKvRecordBatch(new Object[] {3, "c"}));
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
                                new Object[] {1, "a"},
                                new Object[] {2, "b"},
                                new Object[] {3, "c"}));
        assertHasKeyValues(restored.getKvTablet(), expectedKeyValues);
    }

    @Test
    void testRestoreFromRemoteSnapshotWithEmptyLocalLog(@TempDir File remoteDir) throws Exception {
        RemoteRestoreSnapshotContext context = new RemoteRestoreSnapshotContext(remoteDir);
        CompletedSnapshot remoteSnapshot = writeDataAndTakeSnapshot(context);
        wipeLocalKvState(context.replica);
        // Model a lost disk: the restored log starts at zero, while the remote snapshot already
        // contains the acknowledged records up to its log offset.
        logManager.truncateFullyAndStartAt(TABLE_BUCKET, 0);
        assertThat(context.replica.getLocalLogEndOffset()).isZero();
        assertThat(remoteSnapshot.getLogOffset()).isPositive();

        RemoteRestoreSnapshotContext freshContext = new RemoteRestoreSnapshotContext(remoteDir);
        freshContext.remoteSnapshotStore.commitKvSnapshot(remoteSnapshot, 0, 0);
        Replica restored = makeFreshLeader(freshContext);

        assertThat(freshContext.downloadEvents).containsExactly("download");
        assertThat(restored.getLocalLogEndOffset()).isEqualTo(remoteSnapshot.getLogOffset());
        assertThat(zkClient.hasLimitedRecovery()).isTrue();
        assertHasKeyValues(
                restored.getKvTablet(),
                getKeyValuePairs(genKvRecords(new Object[] {1, "a"}, new Object[] {2, "b"})));
        putRecordsToLeader(restored, genKvRecordBatch(new Object[] {3, "c"}));
        assertHasKeyValues(
                restored.getKvTablet(),
                getKeyValuePairs(
                        genKvRecords(
                                new Object[] {1, "a"},
                                new Object[] {2, "b"},
                                new Object[] {3, "c"})));
    }

    @Test
    void testFailedRestoreClosesKvBeforeRetry(@TempDir File remoteDir) throws Exception {
        RemoteRestoreSnapshotContext context = new RemoteRestoreSnapshotContext(remoteDir);
        CompletedSnapshot remoteSnapshot = writeDataAndTakeSnapshot(context);
        wipeLocalKvState(context.replica);
        // A nonempty log ending before the snapshot offset is inconsistent: fail closed, but
        // retry without retaining RocksDB's LOCK from the previous attempt.
        logManager.truncateTo(TABLE_BUCKET, remoteSnapshot.getLogOffset() - 1);

        RemoteRestoreSnapshotContext freshContext = new RemoteRestoreSnapshotContext(remoteDir);
        freshContext.remoteSnapshotStore.commitKvSnapshot(remoteSnapshot, 0, 0);
        assertThatThrownBy(() -> makeFreshLeader(freshContext))
                .hasRootCauseMessage(
                        String.format(
                                "Cannot restore snapshot at offset %s for %s: local log ends at %s but still contains records.",
                                remoteSnapshot.getLogOffset(),
                                TABLE_BUCKET,
                                remoteSnapshot.getLogOffset() - 1));
        assertThat(freshContext.downloadEvents).hasSize(5);
        assertThat(kvManager.getKv(TABLE_BUCKET)).isEmpty();
    }

    @Test
    void testRestoredWriterCanContinueAfterLogLoss(@TempDir File remoteDir) throws Exception {
        RemoteRestoreSnapshotContext context = new RemoteRestoreSnapshotContext(remoteDir);
        Replica original = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, TABLE_BUCKET, context);
        makeKvReplicaAsLeader(original, INITIAL_LEADER_EPOCH);
        KvRecordBatchFactory batches = KvRecordBatchFactory.of(DEFAULT_SCHEMA_ID);
        putRecordsToLeader(
                original, batches.ofRecords(genKvRecords(new Object[] {1, "a"}), 77L, 0));
        context.scheduledExecutorService.triggerAllNonPeriodicTasks();
        CompletedSnapshot snapshot =
                context.testKvSnapshotStore.waitUntilSnapshotComplete(TABLE_BUCKET, 0);
        assertThat(snapshot.getWriterSnapshotPath()).isNotNull();
        wipeLocalKvState(original);
        logManager.truncateFullyAndStartAt(TABLE_BUCKET, 0);

        RemoteRestoreSnapshotContext fresh = new RemoteRestoreSnapshotContext(remoteDir);
        fresh.remoteSnapshotStore.commitKvSnapshot(snapshot, 0, 0);
        Replica restored = makeFreshLeader(fresh);
        assertThat(restored.getLogTablet().activeWriters()).containsKey(77L);
        LogAppendInfo duplicate =
                putRecordsToLeader(
                        restored, batches.ofRecords(genKvRecords(new Object[] {1, "a"}), 77L, 0));
        assertThat(duplicate.duplicated()).isTrue();
        putRecordsToLeader(
                restored, batches.ofRecords(genKvRecords(new Object[] {2, "b"}), 77L, 1));
        assertHasKeyValues(
                restored.getKvTablet(),
                getKeyValuePairs(genKvRecords(new Object[] {1, "a"}, new Object[] {2, "b"})));
    }

    @Test
    void testSnapshotCapturesWritersAtFlushedOffsetWhileLogIsAhead(@TempDir File remoteDir)
            throws Exception {
        RemoteRestoreSnapshotContext context = new RemoteRestoreSnapshotContext(remoteDir);
        Replica original = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, TABLE_BUCKET, context);
        makeKvReplicaAsLeader(original, INITIAL_LEADER_EPOCH);
        KvRecordBatchFactory batches = KvRecordBatchFactory.of(DEFAULT_SCHEMA_ID);
        putRecordsToLeader(
                original, batches.ofRecords(genKvRecords(new Object[] {1, "a"}), 77L, 0));
        long flushedOffset = original.getKvTablet().getFlushedLogOffset();
        // The next WAL batch is accepted locally, but not flushed into the KV snapshot.
        original.putRecordsToLeader(
                batches.ofRecords(genKvRecords(new Object[] {2, "b"}), 77L, 1),
                null,
                MergeMode.DEFAULT,
                0);
        assertThat(original.getLocalLogEndOffset()).isGreaterThan(flushedOffset);
        context.scheduledExecutorService.triggerAllNonPeriodicTasks();
        CompletedSnapshot snapshot =
                context.testKvSnapshotStore.waitUntilSnapshotComplete(TABLE_BUCKET, 0);
        assertThat(snapshot.getLogOffset()).isEqualTo(flushedOffset);
        assertThat(snapshot.getWriterSnapshotPath()).isNotNull();

        wipeLocalKvState(original);
        logManager.truncateFullyAndStartAt(TABLE_BUCKET, 0);
        RemoteRestoreSnapshotContext fresh = new RemoteRestoreSnapshotContext(remoteDir);
        fresh.remoteSnapshotStore.commitKvSnapshot(snapshot, 0, 0);
        Replica restored = makeFreshLeader(fresh);
        putRecordsToLeader(
                restored, batches.ofRecords(genKvRecords(new Object[] {2, "b"}), 77L, 1));
        assertHasKeyValues(
                restored.getKvTablet(),
                getKeyValuePairs(genKvRecords(new Object[] {1, "a"}, new Object[] {2, "b"})));
    }

    @Test
    void testRemoteLogTailReplayedAfterTotalLocalLoss(@TempDir File remoteDir) throws Exception {
        RemoteRestoreSnapshotContext originalContext = new RemoteRestoreSnapshotContext(remoteDir);
        Replica original =
                makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, TABLE_BUCKET, originalContext);
        makeKvReplicaAsLeader(original, INITIAL_LEADER_EPOCH);
        KvRecordBatchFactory batches = KvRecordBatchFactory.of(DEFAULT_SCHEMA_ID);
        putRecordsToLeader(
                original, batches.ofRecords(genKvRecords(new Object[] {1, "a"}), 77L, 0));
        originalContext.scheduledExecutorService.triggerAllNonPeriodicTasks();
        CompletedSnapshot snapshot =
                originalContext.testKvSnapshotStore.waitUntilSnapshotComplete(TABLE_BUCKET, 0);
        putRecordsToLeader(
                original, batches.ofRecords(genKvRecords(new Object[] {2, "b"}), 77L, 1));
        long remoteEnd = original.getLocalLogEndOffset();
        original.getLogTablet().roll(Optional.empty());
        List<LogSegment> localSegments = original.getLogTablet().getSegments();
        LogSegment segment = localSegments.get(0);
        File writerSnapshot =
                original.getLogTablet().writerStateManager().fetchSnapshot(remoteEnd).get();
        RemoteLogSegment remoteSegment =
                RemoteLogSegment.Builder.builder()
                        .remoteLogSegmentId(UUID.randomUUID())
                        .remoteLogStartOffset(0L)
                        .remoteLogEndOffset(remoteEnd)
                        .maxTimestamp(segment.maxTimestampSoFar())
                        .segmentSizeInBytes(segment.getFileLogRecords().sizeInBytes())
                        .tableBucket(TABLE_BUCKET)
                        .physicalTablePath(DATA1_PHYSICAL_TABLE_PATH_PK)
                        .build();
        remoteLogStorage.copyLogSegmentFiles(
                remoteSegment,
                new LogSegmentFiles(
                        segment.getFileLogRecords().file().toPath(),
                        segment.offsetIndex().file().toPath(),
                        segment.timeIndex().file().toPath(),
                        writerSnapshot.toPath()));
        remoteLogManager.registerReplica(original);
        remoteLogManager
                .remoteLogTablet(TABLE_BUCKET)
                .loadRemoteLogManifest(
                        new RemoteLogManifest(
                                DATA1_PHYSICAL_TABLE_PATH_PK,
                                TABLE_BUCKET,
                                Collections.singletonList(remoteSegment)));
        original.getLogTablet().updateRemoteLogOffsets(0L, remoteEnd, remoteEnd);

        wipeLocalKvState(original);
        logManager.truncateFullyAndStartAt(TABLE_BUCKET, 0);
        RemoteRestoreSnapshotContext freshContext = new RemoteRestoreSnapshotContext(remoteDir);
        freshContext.remoteSnapshotStore.commitKvSnapshot(snapshot, 0, 0);
        Replica restored = makeFreshLeader(freshContext);

        assertThat(restored.getLocalLogEndOffset()).isEqualTo(remoteEnd);
        assertHasKeyValues(
                restored.getKvTablet(),
                getKeyValuePairs(genKvRecords(new Object[] {1, "a"}, new Object[] {2, "b"})));
        putRecordsToLeader(
                restored, batches.ofRecords(genKvRecords(new Object[] {3, "c"}), 77L, 2));
        assertHasKeyValues(
                restored.getKvTablet(),
                getKeyValuePairs(
                        genKvRecords(
                                new Object[] {1, "a"},
                                new Object[] {2, "b"},
                                new Object[] {3, "c"})));
    }

    @Test
    void testSnapshotWithoutWriterCheckpointFailsClosedOnEmptyLog(@TempDir File remoteDir)
            throws Exception {
        RemoteRestoreSnapshotContext original = new RemoteRestoreSnapshotContext(remoteDir);
        CompletedSnapshot snapshot = writeDataAndTakeSnapshot(original);
        wipeLocalKvState(original.replica);
        logManager.truncateFullyAndStartAt(TABLE_BUCKET, 0);
        CompletedSnapshot legacy =
                new CompletedSnapshot(
                        snapshot.getTableBucket(),
                        snapshot.getSnapshotID(),
                        snapshot.getSnapshotLocation(),
                        snapshot.getKvSnapshotHandle(),
                        snapshot.getLogOffset(),
                        snapshot.getRowCount(),
                        snapshot.getAutoIncIDRanges());
        RemoteRestoreSnapshotContext fresh = new RemoteRestoreSnapshotContext(remoteDir);
        fresh.remoteSnapshotStore.commitKvSnapshot(legacy, 0, 0);
        assertThatThrownBy(() -> makeFreshLeader(fresh))
                .hasRootCauseMessage(
                        String.format(
                                "Cannot restore snapshot at offset %s for %s with an empty log: the snapshot has no writer checkpoint.",
                                snapshot.getLogOffset(), TABLE_BUCKET));
        assertThat(kvManager.getKv(TABLE_BUCKET)).isEmpty();
    }

    @Test
    void testRemoteDiscoveryFailureDoesNotStartEmptyLeader(@TempDir File remoteDir)
            throws Exception {
        RemoteRestoreSnapshotContext original = new RemoteRestoreSnapshotContext(remoteDir);
        writeDataAndTakeSnapshot(original);
        wipeLocalKvState(original.replica);
        logManager.truncateFullyAndStartAt(TABLE_BUCKET, 0);

        RemoteRestoreSnapshotContext fresh = new RemoteRestoreSnapshotContext(remoteDir);
        fresh.failRemoteDiscovery = true;
        assertThatThrownBy(() -> makeFreshLeader(fresh))
                .hasRootCauseMessage("remote storage unavailable");
        assertThat(kvManager.getKv(TABLE_BUCKET)).isEmpty();
    }

    @Test
    void testWrittenBucketWithoutSnapshotCannotReturnEmpty(@TempDir File remoteDir)
            throws Exception {
        RemoteRestoreSnapshotContext original = new RemoteRestoreSnapshotContext(remoteDir);
        writeDataAndTakeSnapshot(original);
        wipeLocalKvState(original.replica);
        logManager.truncateFullyAndStartAt(TABLE_BUCKET, 0);

        RemoteRestoreSnapshotContext fresh = new RemoteRestoreSnapshotContext(remoteDir);
        assertThatThrownBy(() -> makeFreshLeader(fresh))
                .hasRootCauseMessage(
                        String.format(
                                "Cannot initialize %s with an empty log: this bucket has written data but has no verifiable snapshot.",
                                TABLE_BUCKET));
        assertThat(original.replica.getLocalLogEndOffset()).isZero();
    }

    @Test
    void testKnownRemoteLogTailMustNotBeDiscarded(@TempDir File remoteDir) throws Exception {
        RemoteRestoreSnapshotContext original = new RemoteRestoreSnapshotContext(remoteDir);
        CompletedSnapshot snapshot = writeDataAndTakeSnapshot(original);
        wipeLocalKvState(original.replica);
        logManager.truncateFullyAndStartAt(TABLE_BUCKET, 0);
        original.replica
                .getLogTablet()
                .updateRemoteLogOffsets(
                        0, snapshot.getLogOffset() + 1, snapshot.getLogOffset() + 1);

        RemoteRestoreSnapshotContext fresh = new RemoteRestoreSnapshotContext(remoteDir);
        fresh.remoteSnapshotStore.commitKvSnapshot(snapshot, 0, 0);
        assertThatThrownBy(() -> makeFreshLeader(fresh))
                .hasMessageContaining("Failed to create KV tablet");
        assertThat(kvManager.getKv(TABLE_BUCKET)).isEmpty();
        assertThat(original.replica.getLocalLogEndOffset()).isZero();
    }

    @Test
    void testOrphanedKvFilesOnEmptyLogAreNotTrusted(@TempDir File remoteDir) throws Exception {
        RemoteRestoreSnapshotContext original = new RemoteRestoreSnapshotContext(remoteDir);
        CompletedSnapshot snapshot = writeDataAndTakeSnapshot(original);
        wipeLocalKvState(original.replica);
        logManager.truncateFullyAndStartAt(TABLE_BUCKET, 0);
        File kvDir =
                FlussPaths.kvTabletDir(
                        original.replica.getLogTablet().getDataDir(),
                        DATA1_PHYSICAL_TABLE_PATH_PK,
                        TABLE_BUCKET);
        Files.createDirectories(kvDir.toPath());
        Files.write(new File(kvDir, "partial-download").toPath(), new byte[] {1});

        RemoteRestoreSnapshotContext fresh = new RemoteRestoreSnapshotContext(remoteDir);
        assertThatThrownBy(() -> makeFreshLeader(fresh))
                .hasRootCauseMessage(
                        String.format(
                                "Cannot initialize %s with local KV files and no log or verifiable snapshot.",
                                TABLE_BUCKET));

        fresh.remoteSnapshotStore.commitKvSnapshot(snapshot, 0, 0);
        Replica restored = makeFreshLeader(fresh);
        assertHasKeyValues(
                restored.getKvTablet(),
                getKeyValuePairs(genKvRecords(new Object[] {1, "a"}, new Object[] {2, "b"})));
    }

    @Test
    void testCorruptWriterCheckpointDoesNotAdvanceLog(@TempDir File remoteDir) throws Exception {
        RemoteRestoreSnapshotContext original = new RemoteRestoreSnapshotContext(remoteDir);
        CompletedSnapshot snapshot = writeDataAndTakeSnapshot(original);
        wipeLocalKvState(original.replica);
        logManager.truncateFullyAndStartAt(TABLE_BUCKET, 0);
        Files.write(
                new File(snapshot.getWriterSnapshotPath().toString()).toPath(),
                new byte[] {1, 2, 3});

        RemoteRestoreSnapshotContext fresh = new RemoteRestoreSnapshotContext(remoteDir);
        fresh.remoteSnapshotStore.commitKvSnapshot(snapshot, 0, 0);
        assertThatThrownBy(() -> makeFreshLeader(fresh))
                .hasStackTraceContaining("Invalid writer checkpoint");
        assertThat(original.replica.getLocalLogEndOffset()).isZero();
    }

    @Test
    void testNoRemoteSnapshotKeepsRestoreFromLog(@TempDir File remoteDir) throws Exception {
        RemoteRestoreSnapshotContext freshContext = new RemoteRestoreSnapshotContext(remoteDir);
        Replica restored = makeFreshLeader(freshContext);

        // Current behavior is untouched: a healthy but empty tablet, no download, no lease.
        assertThat(restored.getKvTablet()).isNotNull();
        assertKeyMissing(restored.getKvTablet(), new Object[] {1, "a"});
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
    void testSnapshotDiscoveredThroughHandleIsAlsoLeased(@TempDir File remoteDir) throws Exception {
        RemoteRestoreSnapshotContext original = new RemoteRestoreSnapshotContext(remoteDir);
        CompletedSnapshot snapshot = writeDataAndTakeSnapshot(original);
        wipeLocalKvState(original.replica);
        logManager.truncateFullyAndStartAt(TABLE_BUCKET, 0);

        RemoteRestoreSnapshotContext fresh = new RemoteRestoreSnapshotContext(remoteDir);
        fresh.testKvSnapshotStore.commitKvSnapshot(snapshot, 0, 0);
        Replica restored = makeFreshLeader(fresh);
        assertThat(fresh.orderedEvents)
                .containsExactly(
                        "acquire:" + snapshot.getSnapshotID(),
                        "download",
                        "release:" + snapshot.getSnapshotID());
        assertThat(restored.getKvTablet()).isNotNull();
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

        // Kill-switch off: the remote snapshot is ignored and the data is recovered from the
        // log instead, without any download or lease.
        assertThat(restored.getKvTablet()).isNotNull();
        flushAndWait(restored.getKvTablet(), restored.getLocalLogEndOffset());
        assertHasKeyValues(
                restored.getKvTablet(),
                getKeyValuePairs(genKvRecords(new Object[] {1, "a"}, new Object[] {2, "b"})));
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
        putRecordsToLeader(setupReplica, genKvRecordBatch(new Object[] {3, "c"}));
        wipeLocalKvState(setupReplica);

        // A partially uploaded snapshot (directory without _METADATA yet) must be skipped.
        String tabletDir =
                FlussPaths.remoteKvTabletDir(
                                FlussPaths.remoteKvDir(conf),
                                DATA1_PHYSICAL_TABLE_PATH_PK,
                                TABLE_BUCKET)
                        .toString();
        assertThat(new File(tabletDir, "snap-999").mkdirs()).isTrue();
        // In production the coordinator persists the snapshot metadata file into the snapshot
        // directory when committing (see CompletedSnapshotStore); the fake committer used above
        // skips that step, so materialize it here to model the production remote layout that the
        // listing fallback parses.
        Files.write(
                new File(
                                tabletDir,
                                FlussPaths.REMOTE_KV_SNAPSHOT_DIR_PREFIX
                                        + remoteSnapshot.getSnapshotID()
                                        + "/_METADATA")
                        .toPath(),
                CompletedSnapshotJsonSerde.toJson(remoteSnapshot));

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
                                new Object[] {1, "a"},
                                new Object[] {2, "b"},
                                new Object[] {3, "c"}));
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
        assertKeyMissing(restored.getKvTablet(), new Object[] {1, "a"});
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
        // Keys are derived from the row's primary-key columns (see PKBasedKvRecordFactory): log
        // replay re-encodes keys from the logged rows, so the test data must use
        // schema-consistent keys for replayed writes to land on the expected keys.
        putRecordsToLeader(replica, genKvRecordBatch(new Object[] {1, "a"}, new Object[] {2, "b"}));
        context.scheduledExecutorService.triggerAllNonPeriodicTasks();
        return replica;
    }

    private Replica makeFreshLeader(SnapshotContext freshContext) throws Exception {
        Replica restored = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, TABLE_BUCKET, freshContext);
        makeKvReplicaAsLeader(restored, INITIAL_LEADER_EPOCH + 1);
        return restored;
    }

    private void wipeLocalKvState(Replica replica) {
        makeKvReplicaAsFollower(replica, INITIAL_LEADER_EPOCH + 1);
        assertThat(replica.getKvTablet()).isNull();
    }

    private void makeKvReplicaAsLeader(Replica replica, int leaderEpoch) throws Exception {
        makeLeaderReplica(replica, DATA1_TABLE_PATH_PK, TABLE_BUCKET, leaderEpoch);
    }

    private void makeKvReplicaAsFollower(Replica replica, int leaderEpoch) {
        int newLeaderId = TABLET_SERVER_ID + 1;
        List<Integer> replicas = Arrays.asList(TABLET_SERVER_ID, newLeaderId);
        replica.makeFollower(
                new NotifyLeaderAndIsrData(
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                        TABLE_BUCKET,
                        replicas,
                        new LeaderAndIsr(
                                newLeaderId,
                                leaderEpoch,
                                replicas,
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
        List<ByteArraySlice> values = kvTablet.multiGet(keys);
        assertThat(
                        values.stream()
                                .map(slice -> slice == null ? null : slice.toByteArray())
                                .collect(Collectors.toList()))
                .containsExactlyElementsOf(expectedValues);
    }

    private void assertKeyMissing(KvTablet kvTablet, Object[] record) throws IOException {
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
        private boolean failRemoteDiscovery;
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
                PhysicalTablePath physicalPath, TableBucket tableBucket) throws Exception {
            if (failRemoteDiscovery) {
                throw new IOException("remote storage unavailable");
            }
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
