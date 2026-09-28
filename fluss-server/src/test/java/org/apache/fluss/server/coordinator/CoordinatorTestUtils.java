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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.testutils.common.ScheduledTask;
import org.apache.fluss.utils.concurrent.Scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Utils related to coordinator for test purpose. */
public class CoordinatorTestUtils {

    public static void makeSendLeaderAndStopRequestAlwaysSuccess(
            CoordinatorContext coordinatorContext,
            TestCoordinatorChannelManager testCoordinatorChannelManager) {
        Map<Integer, TabletServerGateway> gateways =
                makeTabletServerGateways(
                        coordinatorContext.liveTabletServerSet(),
                        Collections.emptySet(),
                        Collections.emptySet());
        testCoordinatorChannelManager.setGateways(gateways);
    }

    public static void makeSendLeaderAndStopRequestFailContext(
            CoordinatorContext coordinatorContext,
            TestCoordinatorChannelManager testCoordinatorChannelManager,
            Set<Integer> failServers) {
        Map<Integer, TabletServerGateway> gateways =
                makeTabletServerGateways(
                        coordinatorContext.liveTabletServerSet(),
                        failServers,
                        Collections.emptySet());
        testCoordinatorChannelManager.setGateways(gateways);
    }

    public static void makeSendLeaderAndStopRequestAlwaysSuccess(
            TestCoordinatorChannelManager testCoordinatorChannelManager,
            Set<Integer> servers,
            Set<ApiKeys> ignoreApis) {
        Map<Integer, TabletServerGateway> gateways =
                makeTabletServerGateways(servers, Collections.emptySet(), ignoreApis);
        testCoordinatorChannelManager.setGateways(gateways);
    }

    public static void makeSendLeaderAndStopRequestFailContext(
            TestCoordinatorChannelManager testCoordinatorChannelManager,
            Set<Integer> servers,
            Set<Integer> failServers) {
        Map<Integer, TabletServerGateway> gateways =
                makeTabletServerGateways(servers, failServers, Collections.emptySet());
        testCoordinatorChannelManager.setGateways(gateways);
    }

    private static Map<Integer, TabletServerGateway> makeTabletServerGateways(
            Set<Integer> servers, Set<Integer> failedServers, Set<ApiKeys> ignoreApis) {
        Map<Integer, TabletServerGateway> gateways = new HashMap<>();
        for (Integer server : servers) {
            TabletServerGateway tabletServerGateway =
                    new TestTabletServerGateway(failedServers.contains(server), ignoreApis);
            gateways.put(server, tabletServerGateway);
        }
        return gateways;
    }

    public static List<ServerInfo> createServers(List<Integer> servers) {
        List<ServerInfo> tabletServes = new ArrayList<>();
        for (int server : servers) {
            tabletServes.add(
                    new ServerInfo(
                            server,
                            "RACK" + server,
                            Endpoint.fromListenersString("CLIENT://host:100"),
                            ServerType.TABLET_SERVER));
        }
        return tabletServes;
    }

    public static void checkLeaderAndIsr(
            ZooKeeperClient zooKeeperClient,
            TableBucket tableBucket,
            int expectLeaderEpoch,
            int expectLeader)
            throws Exception {
        LeaderAndIsr leaderAndIsr = zooKeeperClient.getLeaderAndIsr(tableBucket).get();
        assertThat(leaderAndIsr.leaderEpoch()).isEqualTo(expectLeaderEpoch);
        assertThat(leaderAndIsr.leader()).isEqualTo(expectLeader);
    }

    /**
     * Creates a request batch wired with a scheduler that records retries but never runs them, so
     * tests keep the previous fire-and-forget behavior of failed notify-leader-and-isr sends.
     */
    public static CoordinatorRequestBatch newCoordinatorRequestBatch(
            CoordinatorChannelManager channelManager,
            EventManager eventManager,
            CoordinatorContext coordinatorContext) {
        return new CoordinatorRequestBatch(
                channelManager,
                eventManager,
                coordinatorContext,
                newNeverRunScheduler(),
                ConfigOptions.COORDINATOR_NOTIFY_LEADER_AND_ISR_RETRY_DELAY
                        .defaultValue()
                        .toMillis());
    }

    /** Returns a scheduler that accepts tasks but never executes them. */
    public static Scheduler newNeverRunScheduler() {
        return new Scheduler() {
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
                return new ScheduledTask<>(() -> null, delayMs, periodMs);
            }
        };
    }
}
