// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::error::Result;
use crate::proto::{DescribeTabletServersResponse, TabletServerHealth as PbTabletServerHealth};

/// Mirrors Java `org.apache.fluss.client.admin.TabletServerHealth`.
///
/// Per-TabletServer health slice: the four counters [`ClusterHealth`]
/// reports cluster-wide, scoped to the replicas and leaders hosted by one
/// server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletServerHealth {
    pub server_id: i32,
    pub num_replicas: i32,
    pub in_sync_replicas: i32,
    pub num_leader_replicas: i32,
    pub active_leader_replicas: i32,
}

impl TabletServerHealth {
    pub fn from_pb(pb: &PbTabletServerHealth) -> Result<Self> {
        Ok(Self {
            server_id: pb.server_id,
            num_replicas: pb.num_replicas,
            in_sync_replicas: pb.in_sync_replicas,
            num_leader_replicas: pb.num_leader_replicas,
            active_leader_replicas: pb.active_leader_replicas,
        })
    }

    /// True when every hosted replica is in-sync and every hosted leader is active.
    pub fn is_green(&self) -> bool {
        self.in_sync_replicas == self.num_replicas
            && self.active_leader_replicas == self.num_leader_replicas
    }
}

/// Result of `describe_tablet_servers`: one entry per requested server.
pub type TabletServersHealth = Vec<TabletServerHealth>;

/// Convert a full response into model entries, preserving server order.
pub fn tablet_servers_from_pb(resp: &DescribeTabletServersResponse) -> Result<TabletServersHealth> {
    resp.servers
        .iter()
        .map(TabletServerHealth::from_pb)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pb(
        server_id: i32,
        num_replicas: i32,
        in_sync_replicas: i32,
        num_leader_replicas: i32,
        active_leader_replicas: i32,
    ) -> PbTabletServerHealth {
        PbTabletServerHealth {
            server_id,
            num_replicas,
            in_sync_replicas,
            num_leader_replicas,
            active_leader_replicas,
        }
    }

    #[test]
    fn test_from_pb_and_is_green() {
        let green = TabletServerHealth::from_pb(&pb(0, 2, 2, 1, 1)).unwrap();
        assert_eq!(green.server_id, 0);
        assert!(green.is_green());

        let lagging = TabletServerHealth::from_pb(&pb(1, 2, 1, 0, 0)).unwrap();
        assert!(!lagging.is_green());

        let empty = TabletServerHealth::from_pb(&pb(9, 0, 0, 0, 0)).unwrap();
        assert!(empty.is_green());
    }

    #[test]
    fn test_response_preserves_order() {
        let resp = DescribeTabletServersResponse {
            servers: vec![pb(2, 0, 0, 0, 0), pb(0, 1, 1, 1, 1)],
        };
        let servers = tablet_servers_from_pb(&resp).unwrap();
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].server_id, 2);
        assert_eq!(servers[1].server_id, 0);
    }
}
