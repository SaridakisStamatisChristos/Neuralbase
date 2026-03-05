// SPDX-License-Identifier: Apache-2.0
// Cluster node registry and shard routing.
//
// NodeRegistry tracks cluster membership and health.
// ConsistentHashRouter maps keys to shard IDs and shard IDs to nodes.
//
// CONFIDENCE: raw=0.80 effective=0.70
// DEPENDS_ON: consensus::rpc (NodeId)

// Session 5 — not yet wired into query path. Suppress dead_code.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::cmp::Reverse;

use crate::consensus::rpc::NodeId;

// ── NodeInfo ───────────────────────────────────────────────────────────────

/// Metadata about a single cluster node.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: NodeId,
    /// Address for Raft RPC traffic.
    pub raft_addr: String,
    /// Address for SQL query fragment exchange.
    pub query_addr: String,
    /// Last heartbeat received (used for failure detection).
    pub last_heartbeat: Instant,
    pub alive: bool,
}

// ── ClusterConfig ──────────────────────────────────────────────────────────

/// Static cluster configuration loaded at startup.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// All member nodes (including self).
    pub nodes: Vec<NodeInfo>,
    /// Number of shards.
    pub shard_count: u32,
    /// Replication factor (number of nodes that store each shard).
    pub replication_factor: usize,
    /// Heartbeat interval for failure detection.
    pub heartbeat_interval: Duration,
    /// Number of missed heartbeats before marking a node unavailable.
    pub heartbeat_miss_threshold: u32,
}

impl ClusterConfig {
    /// Default 3-node local cluster config.
    pub fn default_3node() -> Self {
        let now = Instant::now();
        Self {
            nodes: vec![
                NodeInfo {
                    id: "node1".to_string(),
                    raft_addr: "127.0.0.1:7001".to_string(),
                    query_addr: "127.0.0.1:8001".to_string(),
                    last_heartbeat: now,
                    alive: true,
                },
                NodeInfo {
                    id: "node2".to_string(),
                    raft_addr: "127.0.0.1:7002".to_string(),
                    query_addr: "127.0.0.1:8002".to_string(),
                    last_heartbeat: now,
                    alive: true,
                },
                NodeInfo {
                    id: "node3".to_string(),
                    raft_addr: "127.0.0.1:7003".to_string(),
                    query_addr: "127.0.0.1:8003".to_string(),
                    last_heartbeat: now,
                    alive: true,
                },
            ],
            shard_count: 8,
            replication_factor: 2,
            heartbeat_interval: Duration::from_millis(100),
            heartbeat_miss_threshold: 3,
        }
    }
}

// ── NodeRegistry ───────────────────────────────────────────────────────────

/// Thread-safe registry of cluster nodes.
/// Updated by the Raft heartbeat monitor as nodes join/leave.
#[derive(Debug, Clone)]
pub struct NodeRegistry {
    nodes: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,
    config: ClusterConfig,
}

impl NodeRegistry {
    pub fn new(config: ClusterConfig) -> Self {
        let mut map = HashMap::new();
        for node in &config.nodes {
            map.insert(node.id.clone(), node.clone());
        }
        Self {
            nodes: Arc::new(RwLock::new(map)),
            config,
        }
    }

    /// Record a heartbeat from a node; mark it alive.
    pub fn record_heartbeat(&self, id: &NodeId) {
        if let Ok(mut guard) = self.nodes.write() {
            if let Some(node) = guard.get_mut(id) {
                node.last_heartbeat = Instant::now();
                node.alive = true;
            }
        }
    }

    /// Run failure detection: mark nodes that have not sent a heartbeat
    /// within `heartbeat_interval * miss_threshold` as unavailable.
    pub fn check_failures(&self) {
        let threshold = self.config.heartbeat_interval
            * self.config.heartbeat_miss_threshold;
        if let Ok(mut guard) = self.nodes.write() {
            for node in guard.values_mut() {
                if node.alive && node.last_heartbeat.elapsed() > threshold {
                    node.alive = false;
                }
            }
        }
    }

    /// Return all currently alive nodes.
    pub fn alive_nodes(&self) -> Vec<NodeInfo> {
        self.nodes
            .read()
            .unwrap()
            .values()
            .filter(|n| n.alive)
            .cloned()
            .collect()
    }

    /// Return a specific node's info.
    pub fn get(&self, id: &NodeId) -> Option<NodeInfo> {
        self.nodes.read().unwrap().get(id).cloned()
    }

    /// Add or update a node.
    pub fn upsert(&self, info: NodeInfo) {
        self.nodes.write().unwrap().insert(info.id.clone(), info);
    }

    pub fn shard_count(&self) -> u32 {
        self.config.shard_count
    }
}

// ── ConsistentHashRouter ───────────────────────────────────────────────────

/// Maps a primary key to a shard ID via rendezvous (highest-random-weight) hashing.
/// Rendezvous hashing is preferred over ring hashing: it distributes load evenly
/// when nodes are added/removed, without the need for virtual nodes.
pub struct ConsistentHashRouter {
    shard_count: u32,
    registry: Arc<NodeRegistry>,
}

impl ConsistentHashRouter {
    pub fn new(registry: Arc<NodeRegistry>) -> Self {
        Self {
            shard_count: registry.shard_count(),
            registry,
        }
    }

    /// Map a key to a shard ID.
    pub fn key_to_shard(&self, key: &[u8]) -> u32 {
        (fnv1a(key) % u64::from(self.shard_count)) as u32
    }

    /// Return the primary node responsible for a given shard.
    /// Uses highest-random-weight selection among alive nodes.
    pub fn shard_to_node(&self, shard_id: u32) -> Option<NodeInfo> {
        let alive = self.registry.alive_nodes();
        if alive.is_empty() {
            return None;
        }
        // Rendezvous: for each node, hash(shard_id || node_id); pick highest.
        alive
            .into_iter()
            .max_by_key(|n| {
                let mut input = shard_id.to_be_bytes().to_vec();
                input.extend_from_slice(n.id.as_bytes());
                fnv1a(&input)
            })
    }

    /// Return all nodes that should hold a replica of `shard_id`
    /// (up to `replication_factor` nodes, sorted by rendezvous weight descending).
    pub fn shard_replicas(&self, shard_id: u32, replication_factor: usize) -> Vec<NodeInfo> {
        let mut alive = self.registry.alive_nodes();
        alive.sort_by_key(|n| {
            let mut input = shard_id.to_be_bytes().to_vec();
            input.extend_from_slice(n.id.as_bytes());
            Reverse(fnv1a(&input))
        });
        alive.truncate(replication_factor);
        alive
    }
}

/// FNV-1a 64-bit hash — deterministic, no deps needed.
fn fnv1a(data: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

// ── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_to_shard_is_deterministic() {
        let config = ClusterConfig::default_3node();
        let registry = Arc::new(NodeRegistry::new(config));
        let router = ConsistentHashRouter::new(Arc::clone(&registry));
        assert_eq!(router.key_to_shard(b"user:42"), router.key_to_shard(b"user:42"));
    }

    #[test]
    fn shard_to_node_returns_some_when_alive() {
        let config = ClusterConfig::default_3node();
        let registry = Arc::new(NodeRegistry::new(config));
        let router = ConsistentHashRouter::new(Arc::clone(&registry));
        assert!(router.shard_to_node(0).is_some());
    }

    #[test]
    fn failure_detection_marks_stale_node_dead() {
        let mut config = ClusterConfig::default_3node();
        config.heartbeat_interval = Duration::from_millis(1);
        config.heartbeat_miss_threshold = 1;
        let registry = NodeRegistry::new(config);
        // Don't record a heartbeat — wait for threshold.
        std::thread::sleep(Duration::from_millis(10));
        registry.check_failures();
        let alive = registry.alive_nodes();
        // All nodes are stale.
        assert_eq!(alive.len(), 0);
    }

    #[test]
    fn record_heartbeat_keeps_node_alive() {
        let mut config = ClusterConfig::default_3node();
        config.heartbeat_interval = Duration::from_millis(1);
        config.heartbeat_miss_threshold = 1;
        let registry = NodeRegistry::new(config);
        std::thread::sleep(Duration::from_millis(5));
        registry.record_heartbeat(&"node1".to_string());
        registry.check_failures();
        let alive = registry.alive_nodes();
        assert!(alive.iter().any(|n| n.id == "node1"));
    }

    #[test]
    fn shard_replicas_count_bounded_by_alive() {
        let config = ClusterConfig::default_3node();
        let registry = Arc::new(NodeRegistry::new(config));
        let router = ConsistentHashRouter::new(Arc::clone(&registry));
        let replicas = router.shard_replicas(0, 5);
        // Only 3 alive nodes, so never more than 3.
        assert!(replicas.len() <= 3);
    }
}
