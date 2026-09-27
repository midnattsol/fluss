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

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Objects;

/**
 * Per-TabletServer health slice returned by {@link Admin#describeTabletServers(java.util.List)}:
 * the four counters {@link ClusterHealth} reports cluster-wide, scoped to the replicas and leaders
 * hosted by one server.
 *
 * @since 1.1
 */
@PublicEvolving
public final class TabletServerHealth {

    private final int serverId;
    private final int numReplicas;
    private final int inSyncReplicas;
    private final int numLeaderReplicas;
    private final int activeLeaderReplicas;

    public TabletServerHealth(
            int serverId,
            int numReplicas,
            int inSyncReplicas,
            int numLeaderReplicas,
            int activeLeaderReplicas) {
        this.serverId = serverId;
        this.numReplicas = numReplicas;
        this.inSyncReplicas = inSyncReplicas;
        this.numLeaderReplicas = numLeaderReplicas;
        this.activeLeaderReplicas = activeLeaderReplicas;
    }

    public int getServerId() {
        return serverId;
    }

    public int getNumReplicas() {
        return numReplicas;
    }

    public int getInSyncReplicas() {
        return inSyncReplicas;
    }

    public int getNumLeaderReplicas() {
        return numLeaderReplicas;
    }

    public int getActiveLeaderReplicas() {
        return activeLeaderReplicas;
    }

    /** True when every hosted replica is in-sync and every hosted leader is active. */
    public boolean isGreen() {
        return inSyncReplicas == numReplicas && activeLeaderReplicas == numLeaderReplicas;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TabletServerHealth)) {
            return false;
        }
        TabletServerHealth that = (TabletServerHealth) o;
        return serverId == that.serverId
                && numReplicas == that.numReplicas
                && inSyncReplicas == that.inSyncReplicas
                && numLeaderReplicas == that.numLeaderReplicas
                && activeLeaderReplicas == that.activeLeaderReplicas;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                serverId, numReplicas, inSyncReplicas, numLeaderReplicas, activeLeaderReplicas);
    }

    @Override
    public String toString() {
        return "TabletServerHealth{"
                + "serverId="
                + serverId
                + ", numReplicas="
                + numReplicas
                + ", inSyncReplicas="
                + inSyncReplicas
                + ", numLeaderReplicas="
                + numLeaderReplicas
                + ", activeLeaderReplicas="
                + activeLeaderReplicas
                + '}';
    }
}
