use crate::rpc::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse};
use async_trait::async_trait;

#[async_trait]
pub trait RaftNetwork: Send + Sync {
    /// Send an AppendEntries request to the given peer.
    async fn send_append_entries(
        &self,
        peer_id: &str,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn std::error::Error + Send + Sync>>;

    /// Send a RequestVote request to the given peer.
    async fn send_request_vote(
        &self,
        peer_id: &str,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn std::error::Error + Send + Sync>>;
}

