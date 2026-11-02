//! Boids-inspired swarm routing for chunk placement and read steering.

use nifsi_core::protocol::{CohortId, SliceAnnouncement};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::Ordering,
        Arc, RwLock,
    },
    time::{Duration, Instant},
};
use tracing::debug;

// Wire format gossip payload (shared with transport layer)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealthGossip {
    pub node_id: String,
    pub rtt_ms: f32,
    pub disk_usage: f32,
    pub queue_depth: u32,
    pub epoch: u64,
    pub forwarded: bool,
}

// Score snapshot entry

#[derive(Debug, Clone)]
pub struct ScoredPeer {
    pub node_id:    String,
    pub base_score: f32,
}


// Configuration
#[derive(Debug, Clone)]
pub struct SwarmConfig {
    pub stale_after: Duration,
    pub cohesion_weight: f32,
    pub alignment_weight: f32,
    pub separation_penalty: f32,
    pub cohort_separation_penalty: f32,
    pub rtt_baseline_ms: f32,
    pub epidemic_k: usize,
}

impl Default for SwarmConfig {
    fn default() -> Self {
        Self {
            stale_after:        Duration::from_secs(10),
            cohesion_weight:    1.0,
            alignment_weight:   1.0,
            separation_penalty: 2.5,
            cohort_separation_penalty: 3.0,
            rtt_baseline_ms:    10.0,
            epidemic_k:         2,
        }
    }
}


// Internal tracking record (behind RwLock, only on gossip update path)

#[derive(Debug, Clone)]
struct PeerRecord {
    gossip:       NodeHealthGossip,
    refreshed_at: Instant,
}
// SwarmRouter
#[derive(Clone)]
pub struct SwarmRouter {
    config: SwarmConfig,
    peers: Arc<RwLock<HashMap<String, PeerRecord>>>,
    cohort_placements: Arc<RwLock<HashMap<CohortId, HashSet<String>>>>,
    snapshot: Arc<std::sync::atomic::AtomicPtr<Vec<ScoredPeer>>>,
    _snapshot_keeper: Arc<RwLock<Vec<Arc<Vec<ScoredPeer>>>>>,
}

impl SwarmRouter {
    /// Create a new router with the given configuration.
    pub fn new(config: SwarmConfig) -> Self {
        // Start with an empty snapshot.
        let empty: Box<Vec<ScoredPeer>> = Box::new(Vec::new());
        let ptr = Box::into_raw(empty);

        Self {
            config,
            peers:            Arc::new(RwLock::new(HashMap::new())),
            cohort_placements: Arc::new(RwLock::new(HashMap::new())),
            snapshot:         Arc::new(std::sync::atomic::AtomicPtr::new(ptr)),
            _snapshot_keeper: Arc::new(RwLock::new(Vec::new())),
        }
    }
    // Ingestion
    pub fn update_peer(&self, gossip: NodeHealthGossip) {
        debug!(
            node_id     = %gossip.node_id,
            rtt_ms      = gossip.rtt_ms,
            disk_usage  = gossip.disk_usage,
            queue_depth = gossip.queue_depth,
            epoch       = gossip.epoch,
            "swarm: peer health updated"
        );

        {
            let mut guard = self.peers.write().unwrap();
            let record = guard
                .entry(gossip.node_id.clone())
                .or_insert_with(|| PeerRecord {
                    gossip: gossip.clone(),
                    refreshed_at: Instant::now(),
                });

            // Only accept newer snapshots (avoid stale replays).
            if gossip.epoch >= record.gossip.epoch {
                record.gossip        = gossip;
                record.refreshed_at  = Instant::now();
            }
        }
        self.rebuild_snapshot();
    }
    // Lockfree routing reads
    pub fn rank_peers_for_chunk(&self, already_holding: &[&str]) -> Vec<String> {
        let snap_ptr = self.snapshot.load(Ordering::Acquire);
        let snap: &Vec<ScoredPeer> = unsafe { &*snap_ptr };

        if already_holding.is_empty() {
            return snap.iter().map(|p| p.node_id.clone()).collect();
        }
        let sep = self.config.separation_penalty;
        let mut adjusted: Vec<(f32, &str)> = snap
            .iter()
            .map(|p| {
                let score = if already_holding.contains(&p.node_id.as_str()) {
                    p.base_score - sep
                } else {
                    p.base_score
                };
                (score, p.node_id.as_str())
            })
            .collect();
        adjusted.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        adjusted.into_iter().map(|(_, id)| id.to_owned()).collect()
    }


    // Cohort placement tracking

    /// Record that `node_id` holds a slice from `cohort_id`.
    pub fn record_peer_cohort(&self, node_id: &str, cohort_id: CohortId) {
        let mut guard = self.cohort_placements.write().unwrap();
        guard.entry(cohort_id).or_default().insert(node_id.to_string());
    }
    /// Record a slice announcement for cohort tracking.
    pub fn record_slice_announcement(&self, ann: &SliceAnnouncement) {
        self.record_peer_cohort(&ann.node_id, ann.cohort_id);
    }
    pub fn peers_holding_cohort(&self, cohort_id: &CohortId) -> Vec<String> {
        let guard = self.cohort_placements.read().unwrap();
        guard.get(cohort_id)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn rank_peers_for_slice(
        &self,
        cohort_peers: &[&str],
        already_holding: &[&str],
    ) -> Vec<String> {
        let snap_ptr = self.snapshot.load(Ordering::Acquire);
        let snap: &Vec<ScoredPeer> = unsafe { &*snap_ptr };

        if cohort_peers.is_empty() && already_holding.is_empty() {
            return snap.iter().map(|p| p.node_id.clone()).collect();
        }

        let sep = self.config.separation_penalty;
        let cohort_sep = self.config.cohort_separation_penalty;

        let mut adjusted: Vec<(f32, &str)> = snap
            .iter()
            .map(|p| {
                let mut score = p.base_score;
                // Direct separation: peer already holds the target slice
                if already_holding.contains(&p.node_id.as_str()) {
                    score -= sep;
                }
                // Cohort anti-affinity: peer holds another slice in the same cohort
                if cohort_peers.contains(&p.node_id.as_str()) {
                    score -= cohort_sep;
                }
                (score, p.node_id.as_str())
            })
            .collect();

        adjusted.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        adjusted.into_iter().map(|(_, id)| id.to_owned()).collect()
    }

    /// Convenience wrapper ranks peers for a slice given a registered `CohortId`
    pub fn rank_peers_for_slice_cohort(
        &self,
        cohort_id: &CohortId,
        already_holding: &[&str],
    ) -> Vec<String> {
        let cohort_nodes = self.peers_holding_cohort(cohort_id);
        let cohort_refs: Vec<&str> = cohort_nodes.iter().map(|s| s.as_str()).collect();
        self.rank_peers_for_slice(&cohort_refs, already_holding)
    }

    /// Return the single best peer for slice placement, or `None` if no live peers are known
    #[inline]
    pub fn best_peer_for_slice(
        &self,
        cohort_peers: &[&str],
        already_holding: &[&str],
    ) -> Option<String> {
        self.rank_peers_for_slice(cohort_peers, already_holding).into_iter().next()
    }

    /// Convenience wrapper, basically rank peers for read routing (no Separation force)
    #[inline]
    pub fn rank_peers_for_read(&self) -> Vec<String> {
        self.rank_peers_for_chunk(&[])
    }

    /// Return the single best peer for a chunk write, or `None` if no live peers are known
    #[inline]
    pub fn best_peer_for_chunk(&self, already_holding: &[&str]) -> Option<String> {
        self.rank_peers_for_chunk(already_holding).into_iter().next()
    }
    // Diagnostics

    /// Return raw health data for all currently live (non-stale) peers.
    pub fn live_peers(&self) -> Vec<NodeHealthGossip> {
        let guard = self.peers.read().unwrap();
        let now   = Instant::now();
        guard
            .values()
            .filter(|r| now.duration_since(r.refreshed_at) < self.config.stale_after)
            .map(|r| r.gossip.clone())
            .collect()
    }

    /// Total number of peers tracked (including stale)
    pub fn peer_count(&self) -> usize {
        self.peers.read().unwrap().len()
    }

    /// The configured epidemic fan out factor K
    pub fn epidemic_k(&self) -> usize {
        self.config.epidemic_k
    }
    // Snapshot rebuild — called only on gossip update (infrequent write path)

    fn rebuild_snapshot(&self) {
        let guard = self.peers.read().unwrap();
        let now   = Instant::now();

        let mut scored: Vec<ScoredPeer> = guard
            .values()
            .filter(|r| now.duration_since(r.refreshed_at) < self.config.stale_after)
            .map(|r| ScoredPeer {
                node_id:    r.gossip.node_id.clone(),
                base_score: self.base_score(&r.gossip),
            })
            .collect();

        scored.sort_by(|a, b| {
            b.base_score
                .partial_cmp(&a.base_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let new_box: Box<Vec<ScoredPeer>> = Box::new(scored);
        let new_ptr = Box::into_raw(new_box);
        let old_ptr = self.snapshot.swap(new_ptr, Ordering::AcqRel);
        if !old_ptr.is_null() {
            unsafe { drop(Box::from_raw(old_ptr)); }
        }
    }
    // Boids scoring — private

    /// Base score (Cohesion + Alignment). Separation is applied at query time.
    fn base_score(&self, gossip: &NodeHealthGossip) -> f32 {
        // Cohesion -> attraction toward low-RTT peers.
        //   score = w / (1 + rtt / baseline)
        //   At rtt==0        → w
        //   At rtt==baseline → w/2
        //   As rtt→∞         → 0
        let cohesion = self.config.cohesion_weight
            / (1.0 + gossip.rtt_ms / self.config.rtt_baseline_ms);

        // Alignment preference for peers with free headroom.
        //   queue contribution capped at 1.0 when queue_depth ≥ 10
        let queue_fraction = (gossip.queue_depth as f32 / 10.0).min(1.0);
        let headroom       = (1.0 - gossip.disk_usage) * (1.0 - queue_fraction);
        let alignment      = self.config.alignment_weight * headroom;

        cohesion + alignment
    }
}
// Tests

#[cfg(test)]
mod tests {
    use super::*;

    fn gossip(id: &str, rtt_ms: f32, disk_usage: f32, queue_depth: u32) -> NodeHealthGossip {
        NodeHealthGossip {
            node_id:     id.to_owned(),
            rtt_ms,
            disk_usage,
            queue_depth,
            epoch:       1,
            forwarded:   false,
        }
    }

    #[test]
    fn ranks_low_rtt_peer_first_when_no_separation() {
        let router = SwarmRouter::new(SwarmConfig::default());
        router.update_peer(gossip("slow",   200.0, 0.3, 0));
        router.update_peer(gossip("fast",    10.0, 0.3, 0));
        router.update_peer(gossip("medium",  50.0, 0.3, 0));

        let ranked = router.rank_peers_for_read();
        assert_eq!(ranked[0], "fast");
        assert_eq!(ranked[2], "slow");
    }

    #[test]
    fn separation_demotes_peer_already_holding_chunk() {
        let router = SwarmRouter::new(SwarmConfig::default());
        // "fast" has best RTT but already holds the chunk.
        router.update_peer(gossip("fast",    5.0, 0.1, 0));
        router.update_peer(gossip("medium", 40.0, 0.2, 0));

        let ranked = router.rank_peers_for_chunk(&["fast"]);
        assert_eq!(ranked[0], "medium");
    }

    #[test]
    fn high_disk_usage_reduces_score() {
        let router = SwarmRouter::new(SwarmConfig::default());
        router.update_peer(gossip("full",  10.0, 0.99, 0));
        router.update_peer(gossip("empty", 10.0, 0.01, 0));

        let ranked = router.rank_peers_for_read();
        assert_eq!(ranked[0], "empty");
    }

    #[test]
    fn stale_peers_excluded_from_ranking() {
        let config = SwarmConfig {
            stale_after: Duration::from_millis(1),
            ..Default::default()
        };
        let router = SwarmRouter::new(config);
        router.update_peer(gossip("old", 5.0, 0.1, 0));

        std::thread::sleep(Duration::from_millis(5));

        // Force a rebuild so that the stale TTL is evaluated
        router.update_peer(gossip("trigger", 5.0, 0.1, 0));
        std::thread::sleep(Duration::from_millis(5));
        router.update_peer(gossip("trigger2", 5.0, 0.1, 0));

        assert!(router.live_peers().iter().all(|p| p.node_id != "old"),
            "stale peer should not appear in live_peers()");
    }

    #[test]
    fn epoch_guards_against_stale_replay() {
        let router = SwarmRouter::new(SwarmConfig::default());
        router.update_peer(NodeHealthGossip {
            node_id: "node".into(), rtt_ms: 10.0,
            disk_usage: 0.5, queue_depth: 0, epoch: 5, forwarded: false,
        });
        // Replay with older epoch should be ignored
        router.update_peer(NodeHealthGossip {
            node_id: "node".into(), rtt_ms: 999.0,
            disk_usage: 0.9, queue_depth: 99, epoch: 3, forwarded: false,
        });

        let live = router.live_peers();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].rtt_ms, 10.0, "stale replay must be ignored");
    }

    #[test]
    fn snapshot_read_is_consistent_with_updates() {
        let router = SwarmRouter::new(SwarmConfig::default());
        for i in 0..10u32 {
            router.update_peer(gossip(&format!("node{i}"), i as f32, 0.1, 0));
        }
        let ranked = router.rank_peers_for_read();
        assert_eq!(ranked.len(), 10);
        // node0 has the lowest RTT (0.0) so should be first
        assert_eq!(ranked[0], "node0");
    }

    #[test]
    fn cohort_anti_affinity_demotes_peer_in_same_cohort() {
        let router = SwarmRouter::new(SwarmConfig::default());
        router.update_peer(gossip("fast", 5.0, 0.1, 0));
        router.update_peer(gossip("medium", 40.0, 0.2, 0));

        // "fast" holds a slice in the same cohort
        let ranked = router.rank_peers_for_slice(&["fast"], &[]);
        assert_eq!(ranked[0], "medium", "Cohort anti-affinity must demote 'fast'");
    }

    #[test]
    fn cohort_anti_affinity_with_registered_cohort() {
        let router = SwarmRouter::new(SwarmConfig::default());
        let cohort: [u8; 16] = [7u8; 16];
        router.update_peer(gossip("fast", 5.0, 0.1, 0));
        router.update_peer(gossip("medium", 40.0, 0.2, 0));

        router.record_peer_cohort("fast", cohort);

        let ranked = router.rank_peers_for_slice_cohort(&cohort, &[]);
        assert_eq!(ranked[0], "medium", "Registered cohort anti-affinity must demote 'fast'");
    }

    #[test]
    fn compounding_penalties_for_duplicate_and_cohort() {
        let router = SwarmRouter::new(SwarmConfig::default());
        router.update_peer(gossip("node_a", 5.0, 0.1, 0)); // fast
        router.update_peer(gossip("node_b", 20.0, 0.1, 0)); // mid
        router.update_peer(gossip("node_c", 50.0, 0.1, 0)); // slow

        let ranked = router.rank_peers_for_slice(&["node_a", "node_b"], &["node_a"]);
        assert_eq!(ranked[0], "node_c");
        assert_eq!(ranked[1], "node_b");
        assert_eq!(ranked[2], "node_a");
    }
}

