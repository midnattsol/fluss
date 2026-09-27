/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.DescribeTabletServersResponse;
import org.apache.fluss.rpc.messages.TabletServerHealth;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CoordinatorService#computeTabletServers}. */
class DescribeTabletServersTest {

    private CoordinatorContext ctx;

    @BeforeEach
    void setUp() {
        ctx = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);
        ctx.setLiveTabletServers(
                Arrays.asList(makeServerInfo(0), makeServerInfo(1), makeServerInfo(2)));
    }

    @Test
    void testEmptyClusterReportsAllLiveServersWithZeroCounts() {
        DescribeTabletServersResponse resp =
                CoordinatorService.computeTabletServers(ctx, new int[0]);

        assertThat(resp.getServersCount()).isEqualTo(3);
        for (TabletServerHealth server : resp.getServersList()) {
            assertThat(server.getNumReplicas()).isEqualTo(0);
            assertThat(server.getInSyncReplicas()).isEqualTo(0);
            assertThat(server.getNumLeaderReplicas()).isEqualTo(0);
            assertThat(server.getActiveLeaderReplicas()).isEqualTo(0);
        }
    }

    @Test
    void testSingleBucketTalliesPerServerSlices() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1, 2));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1, 2), Collections.emptyList(), 0, 1));

        Map<Integer, TabletServerHealth> byServer =
                byServerId(CoordinatorService.computeTabletServers(ctx, new int[0]));

        assertThat(byServer.keySet()).containsExactly(0, 1, 2);
        assertCounts(byServer.get(0), 0, 1, 1, 1, 1);
        assertCounts(byServer.get(1), 1, 1, 1, 0, 0);
        assertCounts(byServer.get(2), 2, 1, 1, 0, 0);
    }

    @Test
    void testRequestFiltersAndSortsServers() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1, 2));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(1, 1, Arrays.asList(1), Collections.emptyList(), 0, 1));

        DescribeTabletServersResponse resp =
                CoordinatorService.computeTabletServers(ctx, new int[] {2, 0});

        assertThat(resp.getServersCount()).isEqualTo(2);
        assertThat(resp.getServerAt(0).getServerId()).isEqualTo(0);
        assertThat(resp.getServerAt(1).getServerId()).isEqualTo(2);
        // Server 0 hosts a replica but is out of ISR and holds no leadership.
        assertCounts(byServerId(resp).get(0), 0, 1, 0, 0, 0);
        // Server 1 is untouched by this request.
        assertThat(byServerId(resp)).doesNotContainKey(1);
    }

    @Test
    void testUnknownServerReportsZeroCounts() {
        DescribeTabletServersResponse resp =
                CoordinatorService.computeTabletServers(ctx, new int[] {9});

        assertThat(resp.getServersCount()).isEqualTo(1);
        assertCounts(byServerId(resp).get(9), 9, 0, 0, 0, 0);
    }

    @Test
    void testInactiveLeaderCountsPerServer() {
        TableBucket tb = new TableBucket(1L, 0);
        ctx.updateBucketReplicaAssignment(tb, Arrays.asList(0, 1));
        ctx.putBucketLeaderAndIsr(
                tb, new LeaderAndIsr(0, 1, Arrays.asList(0, 1), Collections.emptyList(), 0, 1));
        ctx.addPendingLeaderActivation(tb);

        Map<Integer, TabletServerHealth> byServer =
                byServerId(CoordinatorService.computeTabletServers(ctx, new int[0]));

        // The leader is hosted but not active yet: leadership counts, activeness does not.
        assertCounts(byServer.get(0), 0, 1, 1, 1, 0);
        assertCounts(byServer.get(1), 1, 1, 1, 0, 0);
    }

    private static Map<Integer, TabletServerHealth> byServerId(DescribeTabletServersResponse resp) {
        Map<Integer, TabletServerHealth> byServer = new HashMap<>();
        for (TabletServerHealth server : resp.getServersList()) {
            byServer.put(server.getServerId(), server);
        }
        return byServer;
    }

    private static void assertCounts(
            TabletServerHealth server,
            int serverId,
            int numReplicas,
            int inSyncReplicas,
            int numLeaderReplicas,
            int activeLeaderReplicas) {
        assertThat(server.getServerId()).isEqualTo(serverId);
        assertThat(server.getNumReplicas()).isEqualTo(numReplicas);
        assertThat(server.getInSyncReplicas()).isEqualTo(inSyncReplicas);
        assertThat(server.getNumLeaderReplicas()).isEqualTo(numLeaderReplicas);
        assertThat(server.getActiveLeaderReplicas()).isEqualTo(activeLeaderReplicas);
    }

    private static ServerInfo makeServerInfo(int id) {
        return new ServerInfo(
                id,
                "RACK" + id,
                Endpoint.fromListenersString("CLIENT://host" + id + ":9124"),
                ServerType.TABLET_SERVER);
    }
}
