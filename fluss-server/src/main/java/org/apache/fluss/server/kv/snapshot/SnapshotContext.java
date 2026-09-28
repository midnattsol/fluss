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
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.utils.function.FunctionWithException;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A context to provide necessary objects/parameters used by kv snapshot and recover from snapshot.
 */
public interface SnapshotContext {

    /** Get the zookeeper client. */
    ZooKeeperClient getZooKeeperClient();

    /** Get the thread pool to execute tasks running in the async phase of kv snapshot . */
    ExecutorService getAsyncOperationsThreadPool();

    /** Get the uploader for uploading the kv snapshot data. */
    KvSnapshotDataUploader getSnapshotDataUploader();

    /** Get the downloader for downloading the kv snapshot data. */
    KvSnapshotDataDownloader getSnapshotDataDownloader();

    /** Get the scheduler to schedule kv snapshot. */
    ScheduledExecutorService getSnapshotScheduler();

    /** Get a reporter to report completed snapshot. */
    CompletedKvSnapshotCommitter getCompletedSnapshotReporter();

    /** Get the interval of kv snapshot. */
    long getSnapshotIntervalMs();

    /** Get the size of the write buffer for writing the kv snapshot file to remote filesystem. */
    int getSnapshotFsWriteBufferSize();

    /** Get the remote root path to store kv snapshot files. */
    FsPath getRemoteKvDir();

    /**
     * Get the provider of latest CompletedSnapshot for a table bucket. When no completed snapshot
     * exists, the CompletedSnapshot provided will be null.
     */
    FunctionWithException<TableBucket, CompletedSnapshot, Exception>
            getLatestCompletedSnapshotProvider();

    /**
     * Whether a replica with empty local state and no local snapshot may restore its kv tablet from
     * a snapshot in remote storage. See {@code kv.restore-from-remote-snapshot.enabled}.
     *
     * @return true if restoring from a remote snapshot is enabled
     */
    default boolean isRemoteSnapshotRestoreEnabled() {
        return true;
    }

    /**
     * Discover the latest snapshot available in remote storage for the given table bucket. Unlike
     * {@link #getLatestCompletedSnapshotProvider()}, which serves the local snapshot view, this
     * looks up snapshots whose files live in remote storage so that an empty replica (e.g. after
     * total local loss) can bootstrap from them. Snapshots whose metadata can no longer be read are
     * skipped.
     *
     * @param tableBucket the table bucket to discover the remote snapshot for
     * @return the latest readable remote snapshot, or {@link Optional#empty()} when no remote
     *     snapshot exists
     * @throws Exception if discovering the remote snapshots failed
     */
    default Optional<CompletedSnapshot> getLatestRemoteSnapshot(TableBucket tableBucket)
            throws Exception {
        return Optional.empty();
    }

    /**
     * Acquire a lease pinning the given remote snapshot for the duration of a restore, so that the
     * snapshot is not deleted while its files are being downloaded. The caller must {@link
     * RemoteSnapshotLease#close() close} the returned lease once the restore no longer needs the
     * snapshot.
     *
     * @param tableBucket the table bucket the snapshot belongs to
     * @param snapshotId the id of the snapshot to pin
     * @param leaseDurationMs the lease duration in milliseconds
     * @return the acquired lease, never null
     * @throws Exception if acquiring the lease failed
     */
    default RemoteSnapshotLease acquireRemoteSnapshotLease(
            TableBucket tableBucket, long snapshotId, long leaseDurationMs) throws Exception {
        return () -> {};
    }

    /**
     * Handles broken snapshots.
     *
     * <p>In the current implementation, broken snapshots may already have occurred in production
     * environments due to issues such as https://github.com/apache/fluss/issues/1304. While we must
     * prevent inconsistent or broken snapshots from being committed, we also need to provide
     * mechanisms to help the server recover from such snapshots rather than failing permanently.
     *
     * @param snapshot The broken snapshot to handle
     * @throws Exception if recovery handling fails
     */
    void handleSnapshotBroken(CompletedSnapshot snapshot) throws Exception;

    /**
     * Get the max fetch size for fetching log to apply kv during recovering kv. The kv may apply
     * log during recovering.
     */
    int maxFetchLogSizeInRecoverKv();

    /**
     * Get the maximum number of remote log segments that can be downloaded but not yet consumed
     * during KV recovery. See {@code kv.recover.remote-log.prefetch-num}.
     */
    default int remoteLogPrefetchNumInRecoverKv() {
        return ConfigOptions.KV_RECOVERY_REMOTE_LOG_PREFETCH_NUM.defaultValue();
    }

    /**
     * Get the number of threads used to download remote log segments during KV recovery. See {@code
     * kv.recover.remote-log.download-threads}.
     */
    default int remoteLogDownloadThreadsInRecoverKv() {
        return ConfigOptions.KV_RECOVERY_REMOTE_LOG_DOWNLOAD_THREADS.defaultValue();
    }
}
