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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.AcquireKvSnapshotLeaseRequest;
import org.apache.fluss.rpc.messages.AcquireKvSnapshotLeaseResponse;
import org.apache.fluss.rpc.messages.ReleaseKvSnapshotLeaseRequest;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseHandler;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseMetadataManager;
import org.apache.fluss.server.kv.KvSnapshotResource;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.function.FunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getUnavailableSnapshots;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeAcquireKvSnapshotLeaseRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeReleaseKvSnapshotLeaseRequest;

/** A default implementation for {@link SnapshotContext}. */
public class DefaultSnapshotContext implements SnapshotContext, ServerReconfigurable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSnapshotContext.class);

    private final ZooKeeperClient zooKeeperClient;
    private final CompletedKvSnapshotCommitter completedKvSnapshotCommitter;

    private final ScheduledExecutorService snapshotScheduler;
    private final ExecutorService asyncOperationsThreadPool;
    private final KvSnapshotDataUploader kvSnapshotDataUploader;
    private final KvSnapshotDataDownloader kvSnapshotDataDownloader;

    private volatile long kvSnapshotIntervalMs;

    /** The write buffer size for writing the kv snapshot file to remote filesystem. */
    private final int writeBufferSizeInBytes;

    private final CompletedSnapshotHandleStore completedSnapshotHandleStore;

    private final KvSnapshotLeaseMetadataManager leaseMetadataManager;

    /**
     * The tablet-to-coordinator channel used to acquire snapshot leases through the coordinator so
     * that the coordinator's in-memory lease manager protects the snapshot. Null when no
     * coordinator channel is reachable from this context; lease acquisition then falls back to
     * direct ZK writes (see {@link #acquireRemoteSnapshotLease}).
     */
    @Nullable private final CoordinatorGateway coordinatorGateway;

    private final boolean restoreFromRemoteSnapshotEnabled;

    private final int maxFetchLogSizeInRecoverKv;

    private final int remoteLogPrefetchNumInRecoverKv;

    private final int remoteLogDownloadThreadsInRecoverKv;

    private final FsPath remoteKvDir;

    private DefaultSnapshotContext(
            ZooKeeperClient zooKeeperClient,
            CompletedKvSnapshotCommitter completedKvSnapshotCommitter,
            ScheduledExecutorService snapshotScheduler,
            ExecutorService asyncOperationsThreadPool,
            KvSnapshotDataUploader kvSnapshotDataUploader,
            KvSnapshotDataDownloader kvSnapshotDataDownloader,
            long kvSnapshotIntervalMs,
            int writeBufferSizeInBytes,
            FsPath remoteKvDir,
            CompletedSnapshotHandleStore completedSnapshotHandleStore,
            KvSnapshotLeaseMetadataManager leaseMetadataManager,
            @Nullable CoordinatorGateway coordinatorGateway,
            boolean restoreFromRemoteSnapshotEnabled,
            int maxFetchLogSizeInRecoverKv,
            int remoteLogPrefetchNumInRecoverKv,
            int remoteLogDownloadThreadsInRecoverKv) {
        this.zooKeeperClient = zooKeeperClient;
        this.completedKvSnapshotCommitter = completedKvSnapshotCommitter;
        this.snapshotScheduler = snapshotScheduler;
        this.asyncOperationsThreadPool = asyncOperationsThreadPool;
        this.kvSnapshotDataUploader = kvSnapshotDataUploader;
        this.kvSnapshotDataDownloader = kvSnapshotDataDownloader;
        this.kvSnapshotIntervalMs = kvSnapshotIntervalMs;
        this.writeBufferSizeInBytes = writeBufferSizeInBytes;
        this.remoteKvDir = remoteKvDir;

        this.completedSnapshotHandleStore = completedSnapshotHandleStore;
        this.leaseMetadataManager = leaseMetadataManager;
        this.coordinatorGateway = coordinatorGateway;
        this.restoreFromRemoteSnapshotEnabled = restoreFromRemoteSnapshotEnabled;
        this.maxFetchLogSizeInRecoverKv = maxFetchLogSizeInRecoverKv;
        this.remoteLogPrefetchNumInRecoverKv = remoteLogPrefetchNumInRecoverKv;
        this.remoteLogDownloadThreadsInRecoverKv = remoteLogDownloadThreadsInRecoverKv;
    }

    public static DefaultSnapshotContext create(
            ZooKeeperClient zkClient,
            CompletedKvSnapshotCommitter completedKvSnapshotCommitter,
            KvSnapshotResource kvSnapshotResource,
            Configuration conf,
            @Nullable CoordinatorGateway coordinatorGateway) {
        return new DefaultSnapshotContext(
                zkClient,
                completedKvSnapshotCommitter,
                kvSnapshotResource.getKvSnapshotScheduler(),
                kvSnapshotResource.getAsyncOperationsThreadPool(),
                kvSnapshotResource.getKvSnapshotDataUploader(),
                kvSnapshotResource.getKvSnapshotDataDownloader(),
                conf.get(ConfigOptions.KV_SNAPSHOT_INTERVAL).toMillis(),
                (int) conf.get(ConfigOptions.REMOTE_FS_WRITE_BUFFER_SIZE).getBytes(),
                FlussPaths.remoteKvDir(conf),
                new ZooKeeperCompletedSnapshotHandleStore(zkClient),
                new KvSnapshotLeaseMetadataManager(
                        zkClient, conf.getString(ConfigOptions.REMOTE_DATA_DIR)),
                coordinatorGateway,
                conf.get(ConfigOptions.KV_RESTORE_FROM_REMOTE_SNAPSHOT_ENABLED),
                (int) conf.get(ConfigOptions.KV_RECOVER_LOG_RECORD_BATCH_MAX_SIZE).getBytes(),
                conf.get(ConfigOptions.KV_RECOVERY_REMOTE_LOG_PREFETCH_NUM),
                conf.get(ConfigOptions.KV_RECOVERY_REMOTE_LOG_DOWNLOAD_THREADS));
    }

    public ZooKeeperClient getZooKeeperClient() {
        return zooKeeperClient;
    }

    public ExecutorService getAsyncOperationsThreadPool() {
        return asyncOperationsThreadPool;
    }

    public KvSnapshotDataUploader getSnapshotDataUploader() {
        return kvSnapshotDataUploader;
    }

    @Override
    public KvSnapshotDataDownloader getSnapshotDataDownloader() {
        return kvSnapshotDataDownloader;
    }

    public ScheduledExecutorService getSnapshotScheduler() {
        return snapshotScheduler;
    }

    @Override
    public CompletedKvSnapshotCommitter getCompletedSnapshotReporter() {
        return completedKvSnapshotCommitter;
    }

    @Override
    public long getSnapshotIntervalMs() {
        return kvSnapshotIntervalMs;
    }

    @Override
    public int getSnapshotFsWriteBufferSize() {
        return writeBufferSizeInBytes;
    }

    public FsPath getRemoteKvDir() {
        return remoteKvDir;
    }

    @Override
    public FunctionWithException<TableBucket, CompletedSnapshot, Exception>
            getLatestCompletedSnapshotProvider() {
        return (tableBucket) -> {
            Optional<CompletedSnapshotHandle> optSnapshotHandle =
                    completedSnapshotHandleStore.getLatestCompletedSnapshotHandle(tableBucket);
            if (optSnapshotHandle.isPresent()) {
                return optSnapshotHandle.get().retrieveCompleteSnapshot();
            } else {
                return null;
            }
        };
    }

    @Override
    public boolean isRemoteSnapshotRestoreEnabled() {
        return restoreFromRemoteSnapshotEnabled;
    }

    @Override
    public Optional<CompletedSnapshot> getLatestRemoteSnapshot(
            PhysicalTablePath physicalPath, TableBucket tableBucket) throws Exception {
        List<CompletedSnapshotHandle> handles =
                completedSnapshotHandleStore.getAllCompletedSnapshotHandles(tableBucket);
        CompletedSnapshot latest = null;
        for (CompletedSnapshotHandle handle : handles) {
            CompletedSnapshot snapshot;
            try {
                snapshot = handle.retrieveCompleteSnapshot();
            } catch (Exception e) {
                LOG.warn(
                        "Failed to retrieve remote snapshot metadata {} for bucket {}, skipping it.",
                        handle,
                        tableBucket,
                        e);
                continue;
            }
            if (latest == null || snapshot.getSnapshotID() > latest.getSnapshotID()) {
                latest = snapshot;
            }
        }
        if (latest != null) {
            return Optional.of(latest);
        }
        // The ZK handle store can be empty while snapshot files still exist in remote storage
        // (e.g. ZK data loss, or handles cleaned while files remain). Fall back to listing the
        // bucket's remote snapshot directory before giving up.
        return findLatestSnapshotInRemoteStorage(physicalPath, tableBucket);
    }

    /**
     * List the bucket's remote snapshot directory ({@code snap-<snapshotId>} subdirectories) and
     * return the snapshot with the highest id. Each candidate's {@code _METADATA} file is parsed
     * with the same serde the download path uses ({@link CompletedSnapshotHandle}, i.e. {@link
     * CompletedSnapshotJsonSerde}); entries that cannot be listed or parsed — including partially
     * uploaded snapshots without metadata yet — are skipped with a warning. An empty result keeps
     * the caller's healthy-empty behavior untouched.
     *
     * <p>Note: object stores apply their own pagination to listings internally; the filesystem
     * abstraction surfaces the full listing, so no explicit pagination handling is needed here.
     * Listings may be eventually consistent, so a very recently uploaded snapshot can be missed —
     * the next replica init retry will pick it up.
     *
     * @param physicalPath the physical path of the table, locating the bucket in remote storage
     * @param tableBucket the table bucket to discover the remote snapshot for
     * @return the latest snapshot found in remote storage, or {@link Optional#empty()} when the
     *     directory holds no readable snapshot
     * @throws Exception if the remote directory itself cannot be accessed
     */
    private Optional<CompletedSnapshot> findLatestSnapshotInRemoteStorage(
            PhysicalTablePath physicalPath, TableBucket tableBucket) throws Exception {
        FsPath remoteTabletDir =
                FlussPaths.remoteKvTabletDir(remoteKvDir, physicalPath, tableBucket);
        if (!remoteTabletDir.getFileSystem().exists(remoteTabletDir)) {
            return Optional.empty();
        }
        FileStatus[] statuses = remoteTabletDir.getFileSystem().listStatus(remoteTabletDir);
        CompletedSnapshot latest = null;
        if (statuses != null) {
            for (FileStatus status : statuses) {
                if (!status.isDir()) {
                    continue;
                }
                String dirName = status.getPath().getName();
                if (!dirName.startsWith(FlussPaths.REMOTE_KV_SNAPSHOT_DIR_PREFIX)) {
                    continue;
                }
                CompletedSnapshot snapshot;
                try {
                    FsPath metadataPath = CompletedSnapshot.getMetadataFilePath(status.getPath());
                    // The handle only carries the metadata path here; snapshot id and log offset
                    // are re-read from the file by the existing serde.
                    snapshot =
                            new CompletedSnapshotHandle(0L, metadataPath, 0L)
                                    .retrieveCompleteSnapshot();
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to read remote snapshot metadata in {} for bucket {}, skipping it.",
                            status.getPath(),
                            tableBucket,
                            e);
                    continue;
                }
                if (!tableBucket.equals(snapshot.getTableBucket())) {
                    LOG.warn(
                            "Remote snapshot {} in {} belongs to unexpected bucket {}, skipping it.",
                            snapshot.getSnapshotID(),
                            status.getPath(),
                            snapshot.getTableBucket());
                    continue;
                }
                if (latest == null || snapshot.getSnapshotID() > latest.getSnapshotID()) {
                    latest = snapshot;
                }
            }
        }
        return Optional.ofNullable(latest);
    }

    @Override
    public RemoteSnapshotLease acquireRemoteSnapshotLease(
            TableBucket tableBucket, long snapshotId, long leaseDurationMs) throws Exception {
        String leaseId = "restore-" + UUID.randomUUID();
        if (coordinatorGateway != null) {
            return acquireRemoteSnapshotLeaseViaCoordinator(
                    leaseId, tableBucket, snapshotId, leaseDurationMs);
        }
        // No tablet-to-coordinator channel is reachable from this context (e.g. standalone or
        // test usage without a coordinator gateway). Fall back to writing the lease record
        // straight to ZK. Note this bypasses the live coordinator's in-memory lease manager, so
        // retention protection against a RUNNING coordinator is not guaranteed on this path;
        // prefer the coordinator RPC path whenever a gateway is available.
        return acquireRemoteSnapshotLeaseViaZk(leaseId, tableBucket, snapshotId, leaseDurationMs);
    }

    /**
     * Acquire the lease through the coordinator RPC path so that the coordinator's in-memory {@code
     * KvSnapshotLeaseManager} sees it and protects the snapshot from retention cleanup. This is the
     * tablet-to-coordinator channel reachable from replica init: the tablet server already talks to
     * the coordinator through {@link CoordinatorGateway} (as for ISR adjusts and remote-log
     * manifest commits, including blocking calls), and the request is served by {@code
     * CoordinatorService#acquireKvSnapshotLease}.
     *
     * @param leaseId the generated lease id, unique per restore
     * @param tableBucket the table bucket the snapshot belongs to
     * @param snapshotId the id of the snapshot to pin
     * @param leaseDurationMs the lease duration in milliseconds
     * @return the acquired lease; closing it releases the lease through the coordinator
     * @throws Exception if acquiring the lease failed
     */
    private RemoteSnapshotLease acquireRemoteSnapshotLeaseViaCoordinator(
            final String leaseId,
            final TableBucket tableBucket,
            final long snapshotId,
            final long leaseDurationMs)
            throws Exception {
        AcquireKvSnapshotLeaseRequest request =
                makeAcquireKvSnapshotLeaseRequest(
                        leaseId, tableBucket, snapshotId, leaseDurationMs);
        AcquireKvSnapshotLeaseResponse response =
                coordinatorGateway.acquireKvSnapshotLease(request).get();
        Map<TableBucket, Long> unavailableSnapshots = getUnavailableSnapshots(response);
        if (!unavailableSnapshots.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to acquire remote snapshot lease %s for snapshot %d of bucket %s, "
                                    + "unavailable snapshots: %s.",
                            leaseId, snapshotId, tableBucket, unavailableSnapshots));
        }
        LOG.info(
                "Acquired remote snapshot lease {} for snapshot {} of bucket {} via coordinator.",
                leaseId,
                snapshotId,
                tableBucket);
        return () -> {
            ReleaseKvSnapshotLeaseRequest releaseRequest =
                    makeReleaseKvSnapshotLeaseRequest(
                            leaseId, Collections.singletonList(tableBucket));
            coordinatorGateway.releaseKvSnapshotLease(releaseRequest).get();
            LOG.info(
                    "Released remote snapshot lease {} for snapshot {} of bucket {} via coordinator.",
                    leaseId,
                    snapshotId,
                    tableBucket);
        };
    }

    /**
     * Write the lease record straight to ZK. See {@link #acquireRemoteSnapshotLease} for why this
     * is only a fallback when no coordinator gateway is available.
     *
     * @param leaseId the generated lease id, unique per restore
     * @param tableBucket the table bucket the snapshot belongs to
     * @param snapshotId the id of the snapshot to pin
     * @param leaseDurationMs the lease duration in milliseconds
     * @return the acquired lease; closing it deletes the ZK lease record
     * @throws Exception if acquiring the lease failed
     */
    private RemoteSnapshotLease acquireRemoteSnapshotLeaseViaZk(
            final String leaseId,
            final TableBucket tableBucket,
            final long snapshotId,
            final long leaseDurationMs)
            throws Exception {
        long expirationTime = System.currentTimeMillis() + leaseDurationMs;
        KvSnapshotLeaseHandler leaseHandler = new KvSnapshotLeaseHandler(expirationTime);
        leaseHandler.acquireBucket(tableBucket, snapshotId, tableBucket.getBucket() + 1);
        leaseMetadataManager.registerLease(leaseId, leaseHandler);
        LOG.info(
                "Acquired remote snapshot lease {} for snapshot {} of bucket {} via ZK.",
                leaseId,
                snapshotId,
                tableBucket);
        return () -> {
            leaseMetadataManager.deleteLease(leaseId);
            LOG.info(
                    "Released remote snapshot lease {} for snapshot {} of bucket {} via ZK.",
                    leaseId,
                    snapshotId,
                    tableBucket);
        };
    }

    @Override
    public int maxFetchLogSizeInRecoverKv() {
        return maxFetchLogSizeInRecoverKv;
    }

    @Override
    public int remoteLogPrefetchNumInRecoverKv() {
        return remoteLogPrefetchNumInRecoverKv;
    }

    @Override
    public int remoteLogDownloadThreadsInRecoverKv() {
        return remoteLogDownloadThreadsInRecoverKv;
    }

    @Override
    public void handleSnapshotBroken(CompletedSnapshot snapshot) throws Exception {
        completedSnapshotHandleStore.remove(snapshot.getTableBucket(), snapshot.getSnapshotID());
        snapshot.discardAsync(asyncOperationsThreadPool);
    }

    // ============ ServerReconfigurable Implementation ============

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        // Type validation is already handled by DynamicServerConfig.
        // Here we only do basic sanity checks.
        long newIntervalMs = newConfig.get(ConfigOptions.KV_SNAPSHOT_INTERVAL).toMillis();
        if (newIntervalMs <= 0) {
            throw new ConfigException(
                    String.format(
                            "Invalid kv.snapshot.interval can not be negative or zero: %d ms",
                            newIntervalMs));
        }
    }

    @Override
    public void reconfigure(Configuration newConfig) {
        long newIntervalMs = newConfig.get(ConfigOptions.KV_SNAPSHOT_INTERVAL).toMillis();
        if (newIntervalMs == kvSnapshotIntervalMs) {
            LOG.debug("kv.snapshot.interval unchanged: {} ms", newIntervalMs);
            return;
        }
        long oldIntervalMs = kvSnapshotIntervalMs;
        kvSnapshotIntervalMs = newIntervalMs;
        LOG.info("kv.snapshot.interval reconfigured: {} ms -> {} ms", oldIntervalMs, newIntervalMs);
    }
}
