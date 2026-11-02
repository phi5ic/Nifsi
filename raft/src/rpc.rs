use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<crate::log::LogEntry>,
    pub leader_commit: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub conflict_term: u64,
    pub conflict_index: u64,
}

impl AppendEntriesResponse {
    pub fn ok(term: u64) -> Self {
        Self { term, success: true, conflict_term: 0, conflict_index: 0 }
    }
    pub fn reject(term: u64) -> Self {
        Self { term, success: false, conflict_term: 0, conflict_index: 0 }
    }
    pub fn reject_with_hint(term: u64, conflict_term: u64, conflict_index: u64) -> Self {
        Self { term, success: false, conflict_term, conflict_index }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

