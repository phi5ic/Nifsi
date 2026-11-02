use crate::log::{LogEntry, LogStore};
use crate::rpc::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::network::RaftNetwork;
use crate::state::{HardStateStore, MetadataStore, NodeRole, RaftState};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Instant};
use tracing::{debug, info, warn};

// Constants

/// Maximum number of log entries to send in a single AppendEntries RPC.
const MAX_ENTRIES_PER_RPC: u64 = 64;

/// Heartbeat interval for the leader (ms).
const HEARTBEAT_INTERVAL_MS: u64 = 50;
// Public error / result types
#[derive(Debug, thiserror::Error)]
pub enum ProposeError {
    #[error("not the leader")]
    NotLeader,
    #[error("channel closed")]
    ChannelClosed,
}
// Actor command enum
pub enum RaftCommand {
    Propose {
        data: Vec<u8>,
        reply: oneshot::Sender<Result<u64, ProposeError>>,
    },

    AppendEntries {
        req: AppendEntriesRequest,
        reply: oneshot::Sender<AppendEntriesResponse>,
    },

    RequestVote {
        req: RequestVoteRequest,
        reply: oneshot::Sender<RequestVoteResponse>,
    },
}

// RaftHandle — the public interface
#[derive(Clone)]
pub struct RaftHandle {
    tx: mpsc::Sender<RaftCommand>,
    commit_index: Arc<AtomicU64>,
}

impl RaftHandle {
    pub async fn propose(&self, data: Vec<u8>) -> Result<u64, ProposeError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(RaftCommand::Propose { data, reply: reply_tx })
            .await
            .map_err(|_| ProposeError::ChannelClosed)?;
        reply_rx.await.map_err(|_| ProposeError::ChannelClosed)?
    }
    pub async fn append_entries(
        &self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, ProposeError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(RaftCommand::AppendEntries { req, reply: reply_tx })
            .await
            .map_err(|_| ProposeError::ChannelClosed)?;
        reply_rx.await.map_err(|_| ProposeError::ChannelClosed)
    }
    pub fn commit_index(&self) -> u64 {
        self.commit_index.load(Ordering::Acquire)
    }
    pub async fn request_vote(
        &self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, ProposeError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(RaftCommand::RequestVote { req, reply: reply_tx })
            .await
            .map_err(|_| ProposeError::ChannelClosed)?;
        reply_rx.await.map_err(|_| ProposeError::ChannelClosed)
    }
}

// RaftNode — the actor implementation (private, runs inside tokio::spawn)

pub struct RaftNode {
    id: String,
    peers: Vec<String>,
    network: Arc<dyn RaftNetwork>,
    role: NodeRole,
    state: RaftState,
    log_store: Arc<dyn LogStore>,
    hard_state: Arc<dyn HardStateStore>,
    metadata_store: Arc<dyn MetadataStore>,
    commit_index: u64,
    shared_commit: Arc<AtomicU64>,
    last_applied: u64,
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,
    rng_state: u64,

    rx: mpsc::Receiver<RaftCommand>,
}

impl RaftNode {
    pub fn new(
        id: String,
        peers: Vec<String>,
        network: Arc<dyn RaftNetwork>,
        log_store: Arc<dyn LogStore>,
        hard_state: Arc<dyn HardStateStore>,
        metadata_store: Arc<dyn MetadataStore>,
    ) -> (RaftHandle, RaftNode) {
        let (tx, rx) = mpsc::channel(256);

        // Load persisted hard state (term + voted_for) from durable storage.
        let (current_term, voted_for) = hard_state.load().unwrap_or((0, None));

        // Shared commit index: starts at 0, written by actor, read by handle.
        let shared_commit = Arc::new(AtomicU64::new(0));

        let handle = RaftHandle {
            tx,
            commit_index: shared_commit.clone(),
        };

        // Clone id before moving it into the struct literal (needed for seed_rng).
        let rng_state = Self::seed_rng(&id, current_term);

        let node = RaftNode {
            id,
            peers,
            network,
            role: NodeRole::Follower,
            state: RaftState { current_term, voted_for },
            log_store,
            hard_state,
            metadata_store,
            commit_index: 0,
            shared_commit,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            rng_state,
            rx,
        };

        (handle, node)
    }

    pub fn spawn(
        id: String,
        peers: Vec<String>,
        network: Arc<dyn RaftNetwork>,
        log_store: Arc<dyn LogStore>,
        hard_state: Arc<dyn HardStateStore>,
        metadata_store: Arc<dyn MetadataStore>,
    ) -> RaftHandle {
        let (handle, node) = Self::new(id, peers, network, log_store, hard_state, metadata_store);
        tokio::spawn(node.run());
        handle
    }

    fn seed_rng(id: &str, current_term: u64) -> u64 {
        use std::time::SystemTime;
        let id_hash = id
            .bytes()
            .fold(0u64, |acc, b| acc.wrapping_mul(6364136223846793005).wrapping_add(b as u64));
        let time_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let seed = id_hash ^ time_ns ^ (current_term.wrapping_mul(2654435761));
        if seed == 0 { 0x517cc1b727220a95 } else { seed }
    }

    // Main loop

    pub async fn run(mut self) {
        info!("Raft node {} starting", self.id);
        loop {
            match self.role {
                NodeRole::Follower  => self.run_follower().await,
                NodeRole::Candidate => self.run_candidate().await,
                NodeRole::Leader    => self.run_leader().await,
            }
        }
    }

    // Follower loop

    async fn run_follower(&mut self) {
        info!("Node {} is Follower (term {})", self.id, self.state.current_term);

        let mut deadline = Instant::now() + self.election_timeout();

        loop {
            tokio::select! {
                _ = sleep(deadline.saturating_duration_since(Instant::now())) => {
                    info!("Node {} election timeout fired", self.id);
                    self.role = NodeRole::Candidate;
                    return;
                }

                cmd = self.rx.recv() => {
                    match cmd {
                        None => return, // channel closed — shut down
                        Some(RaftCommand::AppendEntries { req, reply }) => {
                            let resp = self.handle_append_entries(req).await;
                            let reset = resp.success;
                            let _ = reply.send(resp);
                            if reset {
                                deadline = Instant::now() + self.election_timeout();
                            }
                            if self.role != NodeRole::Follower {
                                return;
                            }
                        }
                        Some(RaftCommand::RequestVote { req, reply }) => {
                            let resp = self.handle_request_vote(req).await;
                            let reset = resp.vote_granted;
                            let _ = reply.send(resp);
                            if reset {
                                deadline = Instant::now() + self.election_timeout();
                            }
                            if self.role != NodeRole::Follower {
                                return;
                            }
                        }
                        Some(RaftCommand::Propose { reply, .. }) => {
                            let _ = reply.send(Err(ProposeError::NotLeader));
                        }
                    }
                }
            }
        }
    }

    // Candidate loop

    async fn run_candidate(&mut self) {
        self.state.current_term += 1;
        self.state.voted_for = Some(self.id.clone());
        self.persist_hard_state();
        info!("Node {} is Candidate (term {})", self.id, self.state.current_term);

        let (my_last_index, my_last_term) =
            self.log_store.last_index_and_term().await.unwrap_or((0, 0));

        let req = RequestVoteRequest {
            term: self.state.current_term,
            candidate_id: self.id.clone(),
            last_log_index: my_last_index,
            last_log_term: my_last_term,
        };

        // Fan out RequestVote RPCs.
        let mut tasks = tokio::task::JoinSet::new();
        for peer in &self.peers {
            let network = self.network.clone();
            let peer_id = peer.clone();
            let req_clone = req.clone();
            tasks.spawn(async move {
                network.send_request_vote(&peer_id, req_clone).await
            });
        }

        // majority = floor(cluster_size / 2) + 1, where cluster_size = peers + self
        let cluster_size = self.peers.len() + 1;
        let majority = cluster_size / 2 + 1;
        let mut votes = 1usize; // vote for self
        let mut won_election = false;

        // Early exit: if we are already a majority (single-node cluster), skip the loop.
        if votes >= majority {
            won_election = true;
        }

        let election_timeout = self.election_timeout();
        let deadline = Instant::now() + election_timeout;

        if !won_election {
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                tokio::select! {
                    _ = sleep(remaining) => {
                        warn!("Node {} election timed out, restarting", self.id);
                        break;
                    }

                    res = tasks.join_next(), if !tasks.is_empty() => {
                        match res {
                            Some(Ok(Ok(resp))) => {
                                if resp.term > self.state.current_term {
                                    info!("Node {} discovered higher term {}, reverting to follower",
                                        self.id, resp.term);
                                    self.step_down(resp.term);
                                    return;
                                }
                                if resp.vote_granted && resp.term == self.state.current_term {
                                    votes += 1;
                                    debug!("Node {} got vote ({}/{})", self.id, votes, majority);
                                    if votes >= majority {
                                        won_election = true;
                                        break;
                                    }
                                }
                            }
                            _ => {} // timeout or error from this peer, ignore
                        }
                    }

                    cmd = self.rx.recv() => {
                        match cmd {
                            None => return,
                            Some(RaftCommand::AppendEntries { req, reply }) => {
                                let resp = self.handle_append_entries(req).await;
                                let stepped_down = self.role == NodeRole::Follower;
                                let _ = reply.send(resp);
                                if stepped_down { return; }
                            }
                            Some(RaftCommand::RequestVote { req, reply }) => {
                                let resp = self.handle_request_vote(req).await;
                                let stepped_down = self.role == NodeRole::Follower;
                                let _ = reply.send(resp);
                                if stepped_down { return; }
                            }
                            Some(RaftCommand::Propose { reply, .. }) => {
                                let _ = reply.send(Err(ProposeError::NotLeader));
                            }
                        }
                    }
                }
            }
        }

        if won_election {
            info!("Node {} won election (term {})", self.id, self.state.current_term);
            self.become_leader().await;
        } else {
        }
    }

    // Leader loop

    async fn run_leader(&mut self) {
        info!("Node {} is Leader (term {})", self.id, self.state.current_term);

        loop {
            self.leader_replicate_all().await;

            self.apply_committed_entries().await;

            let tick = Instant::now() + Duration::from_millis(HEARTBEAT_INTERVAL_MS);
            loop {
                let remaining = tick.saturating_duration_since(Instant::now());
                if remaining.is_zero() { break; }

                tokio::select! {
                    _ = sleep(remaining) => break,

                    cmd = self.rx.recv() => {
                        match cmd {
                            None => return,
                            Some(RaftCommand::Propose { data, reply }) => {
                                let result = self.leader_append(data).await;
                                let _ = reply.send(result);
                            }
                            Some(RaftCommand::AppendEntries { req, reply }) => {
                                let resp = self.handle_append_entries(req).await;
                                let stepped_down = self.role != NodeRole::Leader;
                                let _ = reply.send(resp);
                                if stepped_down { return; }
                            }
                            Some(RaftCommand::RequestVote { req, reply }) => {
                                let resp = self.handle_request_vote(req).await;
                                let stepped_down = self.role != NodeRole::Leader;
                                let _ = reply.send(resp);
                                if stepped_down { return; }
                            }
                        }
                    }
                }
            }

            if self.role != NodeRole::Leader {
                return;
            }
        }
    }

    /// Append a new entry to the leader's own log.
    async fn leader_append(&mut self, data: Vec<u8>) -> Result<u64, ProposeError> {
        let (last_index, _) = self
            .log_store
            .last_index_and_term()
            .await
            .unwrap_or((0, 0));

        let new_index = last_index + 1;
        let entry = LogEntry {
            term: self.state.current_term,
            index: new_index,
            data,
        };

        if let Err(e) = self.log_store.append(&[entry]).await {
            warn!("Leader {} failed to append entry: {}", self.id, e);
            return Err(ProposeError::NotLeader); // treat log error as transient
        }

        debug!("Leader {} appended entry at index {}", self.id, new_index);
        Ok(new_index)
    }

    /// Send AppendEntries to all peers and update tracking indices.
    async fn leader_replicate_all(&mut self) {
        let peers: Vec<String> = self.peers.clone();
        let (last_index, _) = self
            .log_store
            .last_index_and_term()
            .await
            .unwrap_or((0, 0));

        // Collect (peer_id, req) pairs.
        let mut peer_reqs: Vec<(String, AppendEntriesRequest)> = Vec::new();

        for peer in &peers {
            let next_idx = *self.next_index.get(peer).unwrap_or(&1);
            let prev_log_index = next_idx.saturating_sub(1);
            let prev_log_term = if prev_log_index > 0 {
                self.log_store
                    .get_entry(prev_log_index)
                    .await
                    .unwrap_or(None)
                    .map(|e| e.term)
                    .unwrap_or(0)
            } else {
                0
            };

            // Fetch up to MAX_ENTRIES_PER_RPC entries starting at next_idx.
            let mut entries = Vec::new();
            let end = (next_idx + MAX_ENTRIES_PER_RPC).min(last_index + 1);
            for idx in next_idx..end {
                match self.log_store.get_entry(idx).await {
                    Ok(Some(e)) => entries.push(e),
                    _ => break,
                }
            }

            peer_reqs.push((
                peer.clone(),
                AppendEntriesRequest {
                    term: self.state.current_term,
                    leader_id: self.id.clone(),
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.commit_index,
                },
            ));
        }

        // Fan out all RPCs concurrently.
        let mut tasks: tokio::task::JoinSet<(String, AppendEntriesRequest, Result<AppendEntriesResponse, _>)> =
            tokio::task::JoinSet::new();

        for (peer_id, req) in peer_reqs {
            let network = self.network.clone();
            let peer_clone = peer_id.clone();
            tasks.spawn(async move {
                let result = network.send_append_entries(&peer_clone, req.clone()).await;
                (peer_id, req, result)
            });
        }

        // Process responses.
        while let Some(join_result) = tasks.join_next().await {
            let Ok((peer_id, sent_req, rpc_result)) = join_result else { continue };

            match rpc_result {
                Ok(resp) => {
                    if resp.term > self.state.current_term {
                        info!("Node {} discovered higher term {}, stepping down",
                            self.id, resp.term);
                        self.step_down(resp.term);
                        return;
                    }

                    if resp.success {
                        let new_match = sent_req.prev_log_index
                            + sent_req.entries.len() as u64;
                        let entry = self.match_index.entry(peer_id.clone()).or_insert(0);
                        if new_match > *entry {
                            *entry = new_match;
                        }
                        self.next_index.insert(peer_id, new_match + 1);
                    } else {
                        self.handle_replication_failure(&peer_id, &resp).await;
                    }
                }
                Err(e) => {
                    debug!("Node {} failed to reach peer {}: {}", self.id, peer_id, e);
                }
            }
        }

        self.advance_commit_index(last_index).await;
    }

    async fn handle_replication_failure(&mut self, peer_id: &str, resp: &AppendEntriesResponse) {
        let current_next = *self.next_index.get(peer_id).unwrap_or(&1);

        if resp.conflict_index > 0 {
            if resp.conflict_term == 0 {
                self.next_index.insert(peer_id.to_owned(), resp.conflict_index);
            } else {
                let (last_idx, _) = self.log_store.last_index_and_term().await.unwrap_or((0, 0));
                let mut found = false;
                for idx in (1..=last_idx).rev() {
                    if let Ok(Some(entry)) = self.log_store.get_entry(idx).await {
                        if entry.term == resp.conflict_term {
                            self.next_index.insert(peer_id.to_owned(), idx + 1);
                            found = true;
                            break;
                        }
                        if entry.term < resp.conflict_term {
                            break;
                        }
                    }
                }
                if !found {
                    self.next_index.insert(peer_id.to_owned(), resp.conflict_index);
                }
            }
        } else {
            self.next_index
                .insert(peer_id.to_owned(), current_next.saturating_sub(1).max(1));
        }
    }
    async fn advance_commit_index(&mut self, last_log_index: u64) {
        let cluster_size = self.peers.len() + 1; // +1 for self
        let quorum = cluster_size / 2 + 1;

        let mut replicated: Vec<u64> = self.match_index.values().copied().collect();
        replicated.push(last_log_index);
        replicated.sort_unstable_by(|a, b| b.cmp(a)); // descending

        for &candidate in replicated.iter().take(quorum) {
            if candidate <= self.commit_index {
                break;
            }
            let count = replicated.iter().filter(|&&i| i >= candidate).count();
            if count >= quorum {
                if let Ok(Some(entry)) = self.log_store.get_entry(candidate).await {
                    if entry.term == self.state.current_term {
                        if candidate > self.commit_index {
                            info!("Leader {} advancing commit_index {} → {}",
                                self.id, self.commit_index, candidate);
                            self.commit_index = candidate;
                            self.shared_commit.store(candidate, Ordering::Release);
                        }
                        break;
                    }
                }
            }
        }
    }

    async fn apply_committed_entries(&mut self) {
        while self.last_applied < self.commit_index {
            let next = self.last_applied + 1;
            match self.log_store.get_entry(next).await {
                Ok(Some(entry)) => {
                    if let Err(e) = self.metadata_store.apply(entry.index, &entry.data) {
                        warn!("State machine apply error at index {}: {}", next, e);
                    }
                    self.last_applied = next;
                    debug!("Applied entry at index {}", next);
                }
                Ok(None) => break, // shouldn't happen if commit_index is correct
                Err(e) => {
                    warn!("Failed to read log entry {}: {}", next, e);
                    break;
                }
            }
        }
    }

    pub async fn handle_append_entries(&mut self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        if req.term < self.state.current_term {
            return AppendEntriesResponse::reject(self.state.current_term);
        }

        // Higher term — update and become follower.
        if req.term > self.state.current_term {
            self.step_down(req.term);
        } else {
            // Same term from a valid leader — ensure we are follower.
            self.role = NodeRole::Follower;
        }

        if req.prev_log_index > 0 {
            match self.log_store.get_entry(req.prev_log_index).await {
                Ok(Some(e)) if e.term == req.prev_log_term => {} // Matches — continue.
                Ok(Some(e)) => {
                    // Term mismatch: find first index with conflicting term.
                    let conflict_term = e.term;
                    let conflict_index = self.first_index_of_term(conflict_term).await;
                    return AppendEntriesResponse::reject_with_hint(
                        self.state.current_term,
                        conflict_term,
                        conflict_index,
                    );
                }
                Ok(None) => {
                    // Log doesn't reach prev_log_index.
                    let (last_index, _) = self.log_store.last_index_and_term().await.unwrap_or((0, 0));
                    return AppendEntriesResponse::reject_with_hint(
                        self.state.current_term,
                        0, // conflict_term = 0 signals "log too short"
                        last_index + 1,
                    );
                }
                Err(_) => {
                    return AppendEntriesResponse::reject(self.state.current_term);
                }
            }
        }

        if !req.entries.is_empty() {
            let mut append_from = 0usize; // index into req.entries of first truly new entry
            for (i, entry) in req.entries.iter().enumerate() {
                match self.log_store.get_entry(entry.index).await {
                    Ok(Some(existing)) if existing.term == entry.term => {
                        append_from = i + 1;
                    }
                    Ok(Some(_)) => {
                        let _ = self.log_store.delete_from(entry.index).await;
                        break;
                    }
                    _ => break, // Entry not yet in log — need to append from here.
                }
            }

            if append_from < req.entries.len() {
                if let Err(e) = self.log_store.append(&req.entries[append_from..]).await {
                    warn!("Failed to append log entries: {}", e);
                    return AppendEntriesResponse::reject(self.state.current_term);
                }
            }
        }

        if req.leader_commit > self.commit_index {
            let last_new = req
                .entries
                .last()
                .map(|e| e.index)
                .unwrap_or(req.prev_log_index);
            let new_commit = req.leader_commit.min(last_new);
            if new_commit > self.commit_index {
                self.commit_index = new_commit;
                self.shared_commit.store(new_commit, Ordering::Release);
                self.apply_committed_entries().await;
            }
        }

        AppendEntriesResponse::ok(self.state.current_term)
    }

    pub async fn handle_request_vote(&mut self, req: RequestVoteRequest) -> RequestVoteResponse {
        if req.term < self.state.current_term {
            return RequestVoteResponse {
                term: self.state.current_term,
                vote_granted: false,
            };
        }

        if req.term > self.state.current_term {
            self.step_down(req.term);
        }

        let can_vote = match &self.state.voted_for {
            None => true,
            Some(id) => id == &req.candidate_id,
        };

        if !can_vote {
            return RequestVoteResponse {
                term: self.state.current_term,
                vote_granted: false,
            };
        }

        let (my_last_index, my_last_term) =
            self.log_store.last_index_and_term().await.unwrap_or((0, 0));

        let candidate_up_to_date = if req.last_log_term != my_last_term {
            req.last_log_term > my_last_term
        } else {
            req.last_log_index >= my_last_index
        };

        if !candidate_up_to_date {
            return RequestVoteResponse {
                term: self.state.current_term,
                vote_granted: false,
            };
        }

        self.state.voted_for = Some(req.candidate_id.clone());
        self.persist_hard_state();

        RequestVoteResponse {
            term: self.state.current_term,
            vote_granted: true,
        }
    }


    fn step_down(&mut self, new_term: u64) {
        self.state.current_term = new_term;
        self.state.voted_for = None;
        self.role = NodeRole::Follower;
        self.persist_hard_state();
    }

    async fn become_leader(&mut self) {
        self.role = NodeRole::Leader;
        let (last_index, _) = self.log_store.last_index_and_term().await.unwrap_or((0, 0));
        for peer in &self.peers {
            self.next_index.insert(peer.clone(), last_index + 1);
            self.match_index.insert(peer.clone(), 0);
        }
    }

    fn persist_hard_state(&self) {
        if let Err(e) = self
            .hard_state
            .save(self.state.current_term, self.state.voted_for.as_deref())
        {
            warn!("Failed to persist hard state: {}", e);
        }
    }

    fn election_timeout(&mut self) -> Duration {
        if self.rng_state == 0 {
            self.rng_state = 0x517cc1b727220a95;
        }
        let mut s = self.rng_state;
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        self.rng_state = s;
        let ms = 150 + (s % 150);
        Duration::from_millis(ms)
    }

    async fn first_index_of_term(&self, target_term: u64) -> u64 {
        let (last_index, _) = self.log_store.last_index_and_term().await.unwrap_or((0, 0));
        let mut first = last_index;
        for idx in 1..=last_index {
            if let Ok(Some(e)) = self.log_store.get_entry(idx).await {
                if e.term == target_term {
                    first = idx;
                    break;
                }
            }
        }
        first
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::SQLiteLogStore;
    use crate::state::{MemHardStateStore, NoopMetadataStore};
    use crate::rpc::{AppendEntriesRequest, RequestVoteRequest};
    use std::sync::Arc;

    use async_trait::async_trait;
    use crate::network::RaftNetwork;
    use std::collections::HashMap as HMap;
    use std::sync::Mutex as StdMutex;

    #[derive(Clone, Default)]
    struct MockNetwork {
        handles: Arc<StdMutex<HMap<String, RaftHandle>>>,
    }

    impl MockNetwork {
        fn register(&self, id: &str, handle: RaftHandle) {
            self.handles.lock().unwrap().insert(id.to_owned(), handle);
        }
    }

    #[async_trait]
    impl RaftNetwork for MockNetwork {
        async fn send_append_entries(
            &self,
            peer_id: &str,
            req: AppendEntriesRequest,
        ) -> Result<AppendEntriesResponse, Box<dyn std::error::Error + Send + Sync>> {
            let handle = self.handles.lock().unwrap().get(peer_id).cloned();
            match handle {
                Some(h) => h.append_entries(req).await.map_err(|e| {
                    Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                        as Box<dyn std::error::Error + Send + Sync>
                }),
                None => Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("peer {} not found", peer_id),
                ))),
            }
        }

        async fn send_request_vote(
            &self,
            peer_id: &str,
            req: RequestVoteRequest,
        ) -> Result<RequestVoteResponse, Box<dyn std::error::Error + Send + Sync>> {
            let handle = self.handles.lock().unwrap().get(peer_id).cloned();
            match handle {
                Some(h) => h.request_vote(req).await.map_err(|e| {
                    Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                        as Box<dyn std::error::Error + Send + Sync>
                }),
                None => Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("peer {} not found", peer_id),
                ))),
            }
        }
    }

    fn make_cluster(n: usize) -> (MockNetwork, Vec<RaftHandle>) {
        let network = MockNetwork::default();
        let ids: Vec<String> = (0..n).map(|i| format!("node{}", i)).collect();
        let mut handles = Vec::new();

        for (_i, id) in ids.iter().enumerate() {
            let peers: Vec<String> = ids.iter().filter(|p| *p != id).cloned().collect();
            let log_store = Arc::new(SQLiteLogStore::new(":memory:").unwrap());
            let hard_state = Arc::new(MemHardStateStore::new());
            let metadata = Arc::new(NoopMetadataStore);
            let net = Arc::new(network.clone()) as Arc<dyn RaftNetwork>;

            let handle = RaftNode::spawn(
                id.clone(),
                peers,
                net,
                log_store,
                hard_state,
                metadata,
            );

            network.register(id, handle.clone());
            handles.push(handle);
        }

        (network, handles)
    }

    #[tokio::test]
    async fn test_single_node_propose() {
        let log_store = Arc::new(SQLiteLogStore::new(":memory:").unwrap());
        let hard_state = Arc::new(MemHardStateStore::new());
        let metadata = Arc::new(NoopMetadataStore);

        struct NoNet;
        #[async_trait]
        impl RaftNetwork for NoNet {
            async fn send_append_entries(&self, _: &str, _: AppendEntriesRequest)
                -> Result<AppendEntriesResponse, Box<dyn std::error::Error + Send + Sync>> {
                Err("no peers".into())
            }
            async fn send_request_vote(&self, _: &str, _: RequestVoteRequest)
                -> Result<RequestVoteResponse, Box<dyn std::error::Error + Send + Sync>> {
                Err("no peers".into())
            }
        }

        let handle = RaftNode::spawn(
            "solo".to_owned(),
            vec![], // no peers
            Arc::new(NoNet),
            log_store,
            hard_state,
            metadata,
        );

        tokio::time::sleep(Duration::from_millis(500)).await;

        let idx = handle.propose(b"hello".to_vec()).await;
        assert!(idx.is_ok(), "propose failed: {:?}", idx);
        assert_eq!(idx.unwrap(), 1);

        let idx2 = handle.propose(b"world".to_vec()).await;
        assert_eq!(idx2.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_append_entries_rejects_stale_leader() {
        let log_store = Arc::new(SQLiteLogStore::new(":memory:").unwrap());
        let hard_state = Arc::new(MemHardStateStore::new());
        let metadata = Arc::new(NoopMetadataStore);

        struct NoNet;
        #[async_trait]
        impl RaftNetwork for NoNet {
            async fn send_append_entries(&self, _: &str, _: AppendEntriesRequest)
                -> Result<AppendEntriesResponse, Box<dyn std::error::Error + Send + Sync>> { Err("no peers".into()) }
            async fn send_request_vote(&self, _: &str, _: RequestVoteRequest)
                -> Result<RequestVoteResponse, Box<dyn std::error::Error + Send + Sync>> { Err("no peers".into()) }
        }

        let handle = RaftNode::spawn(
            "n1".to_owned(),
            vec![],
            Arc::new(NoNet),
            log_store,
            hard_state,
            metadata,
        );

        tokio::time::sleep(Duration::from_millis(500)).await;

        let resp = handle.append_entries(AppendEntriesRequest {
            term: 0,
            leader_id: "old_leader".to_owned(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }).await.unwrap();

        assert!(!resp.success);
    }

    #[tokio::test]
    async fn test_three_node_cluster_replication() {
        let (_net, handles) = make_cluster(3);

        tokio::time::sleep(Duration::from_millis(2000)).await;

        let mut leader_handle = None;
        for h in &handles {
            if h.propose(b"ping".to_vec()).await.is_ok() {
                leader_handle = Some(h.clone());
                break;
            }
        }
        assert!(leader_handle.is_some(), "no leader elected in 700ms");
        let leader = leader_handle.unwrap();
        for i in 0..5u32 {
            let idx = leader.propose(format!("cmd-{}", i).into_bytes()).await;
            assert!(idx.is_ok(), "propose {} failed: {:?}", i, idx);
        }
        tokio::time::sleep(Duration::from_millis(300)).await;

    }
}

