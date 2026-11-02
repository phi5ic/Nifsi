//! TCP transport layer for inter-node Raft communication and swarm gossip.


use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use nifsi_raft::{
    network::RaftNetwork,
    node::RaftHandle,
    rpc::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse},
};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use crate::protocol::{Message, MessageType, NifsiCodec};
use crate::swarm::{NodeHealthGossip, SwarmRouter};
use nifsi_core::protocol::{
    FsMutation, SliceAnnouncement, SliceTransferRequest, SliceTransferResponse,
};

pub type FsMutationHandler = Arc<dyn Fn(FsMutation) + Send + Sync>;
pub type SliceRequestHandler = Arc<dyn Fn(SliceTransferRequest) -> SliceTransferResponse + Send + Sync>;
pub type SliceAnnouncementHandler = Arc<dyn Fn(SliceAnnouncement) + Send + Sync>;

// Wire envelope
#[derive(Serialize, Deserialize)]
pub(crate) enum RaftRpcPayload {
    AppendEntriesReq(AppendEntriesRequest),
    AppendEntriesResp(AppendEntriesResponse),
    RequestVoteReq(RequestVoteRequest),
    RequestVoteResp(RequestVoteResponse),
}

// TcpMeshNetwork

type PeerConn = Mutex<Option<Framed<TcpStream, NifsiCodec>>>;

/// ## Connection pools
#[derive(Clone)]
pub struct TcpMeshNetwork {
    peers: Arc<HashMap<String, SocketAddr>>,
    conns: Arc<HashMap<String, Arc<PeerConn>>>,
    gossip_conns: Arc<HashMap<String, Arc<PeerConn>>>,
    slice_conns: Arc<HashMap<String, Arc<PeerConn>>>,
    next_req_id: Arc<AtomicU64>,
    slice_handler: Arc<tokio::sync::RwLock<Option<SliceRequestHandler>>>,
    slice_announcement_handler: Arc<tokio::sync::RwLock<Option<SliceAnnouncementHandler>>>,
}

impl TcpMeshNetwork {
    /// Create a new network layer.
    pub fn new(peers: HashMap<String, SocketAddr>) -> Self {
        let make_pool = |p: &HashMap<String, SocketAddr>| {
            p.keys()
             .map(|id| (id.clone(), Arc::new(Mutex::new(None))))
             .collect::<HashMap<_, _>>()
        };
        let conns        = make_pool(&peers);
        let gossip_conns = make_pool(&peers);
        let slice_conns  = make_pool(&peers);

        Self {
            peers:                      Arc::new(peers),
            conns:                      Arc::new(conns),
            gossip_conns:               Arc::new(gossip_conns),
            slice_conns:                Arc::new(slice_conns),
            next_req_id:                Arc::new(AtomicU64::new(1)),
            slice_handler:              Arc::new(tokio::sync::RwLock::new(None)),
            slice_announcement_handler: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    pub async fn set_slice_handler(&self, handler: SliceRequestHandler) {
        let mut guard = self.slice_handler.write().await;
        *guard = Some(handler);
    }

    pub async fn set_slice_announcement_handler(&self, handler: SliceAnnouncementHandler) {
        let mut guard = self.slice_announcement_handler.write().await;
        *guard = Some(handler);
    }

    // Inbound listener (call once, spawn the returned future)
    pub async fn listen(
        addr:         SocketAddr,
        handle:       RaftHandle,
        swarm:        Option<Arc<SwarmRouter>>,
        network_self: Option<Arc<TcpMeshNetwork>>,
        on_mutation:  Option<FsMutationHandler>,
    ) -> std::io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("Mesh transport listening on {}", addr);

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            debug!("Inbound connection from {}", peer_addr);

            let handle       = handle.clone();
            let swarm        = swarm.clone();
            let network_self = network_self.clone();
            let on_mutation  = on_mutation.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::serve_connection(stream, handle, swarm, network_self, on_mutation).await {
                    warn!("Connection from {} closed: {}", peer_addr, e);
                }
            });
        }
    }

    async fn serve_connection(
        stream:       TcpStream,
        handle:       RaftHandle,
        swarm:        Option<Arc<SwarmRouter>>,
        network_self: Option<Arc<TcpMeshNetwork>>,
        on_mutation:  Option<FsMutationHandler>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut framed = Framed::new(stream, NifsiCodec);

        while let Some(msg_result) = framed.next().await {
            let msg    = msg_result?;
            let req_id = msg.header.req_id;

            match msg.header.msg_type {
                // Filesystem mutation replication — fire-and-forget.
                MessageType::FsWrite => {
                    if let Some(ref handler) = on_mutation {
                        match bincode::deserialize::<FsMutation>(&msg.payload) {
                            Ok(mutation) => handler(mutation),
                            Err(e) => warn!("Failed to decode FsMutation payload: {}", e),
                        }
                    }
                }

                // Slice RPC request: retrieve a 4 KB sub-packet slice
                MessageType::SliceRequest => {
                    match bincode::deserialize::<SliceTransferRequest>(&msg.payload) {
                        Ok(req) => {
                            let response = if let Some(ref net) = network_self {
                                let handler_opt = net.slice_handler.read().await.clone();
                                if let Some(ref handler) = handler_opt {
                                    handler(req)
                                } else {
                                    SliceTransferResponse {
                                        file_id: req.file_id,
                                        chunk_id: req.chunk_id,
                                        chunk_sequence_idx: req.chunk_sequence_idx,
                                        slice_idx: req.slice_idx,
                                        kind: req.kind,
                                        slice: None,
                                        error: Some("No slice handler registered on node".to_string()),
                                    }
                                }
                            } else {
                                SliceTransferResponse {
                                    file_id: req.file_id,
                                    chunk_id: req.chunk_id,
                                    chunk_sequence_idx: req.chunk_sequence_idx,
                                    slice_idx: req.slice_idx,
                                    kind: req.kind,
                                    slice: None,
                                    error: Some("Network handle unavailable".to_string()),
                                }
                            };

                            match bincode::serialize(&response) {
                                Ok(resp_bytes) => {
                                    let resp_msg = Message::new(MessageType::SliceTransfer, req_id, resp_bytes);
                                    if let Err(e) = framed.send(resp_msg).await {
                                        warn!("Failed to send SliceTransfer response: {}", e);
                                    }
                                }
                                Err(e) => warn!("Failed to serialize SliceTransferResponse: {}", e),
                            }
                        }
                        Err(e) => warn!("Failed to decode SliceTransferRequest payload: {}", e),
                    }
                }

                // Slice RPC transfer response frame (inbound delivery)
                MessageType::SliceTransfer => {
                    debug!("Received SliceTransfer response frame req_id={}", req_id);
                }

                // Slice gossip announcement — cohort registration
                MessageType::SliceAnnouncement => {
                    match bincode::deserialize::<SliceAnnouncement>(&msg.payload) {
                        Ok(ann) => {
                            debug!("Inbound SliceAnnouncement: node {} holds slice {} of cohort {:?}", ann.node_id, ann.slice_idx, ann.cohort_id);
                            if let Some(ref router) = swarm {
                                router.record_peer_cohort(&ann.node_id, ann.cohort_id);
                            }
                            if let Some(ref net) = network_self {
                                let handler_opt = net.slice_announcement_handler.read().await.clone();
                                if let Some(ref handler) = handler_opt {
                                    handler(ann);
                                }
                            }
                        }
                        Err(e) => warn!("Failed to decode SliceAnnouncement payload: {}", e),
                    }
                }

                // Swarm gossip — fire-and-forget, no response needed.
                MessageType::SwarmGossip => {
                    if let (Some(router), Some(net)) = (&swarm, &network_self) {
                        match bincode::deserialize::<NodeHealthGossip>(&msg.payload) {
                            Ok(mut gossip) => {
                                if !gossip.forwarded {
                                    let mut fwd = gossip.clone();
                                    fwd.forwarded = true;
                                    let net_clone = net.clone();
                                    let k = router.epidemic_k();
                                    if k > 1 {
                                        tokio::spawn(async move {
                                            net_clone.epidemic_gossip(&fwd, k - 1).await;
                                        });
                                    }
                                }
                                gossip.forwarded = false; // normalise before storing
                                router.update_peer(gossip);
                            }
                            Err(e) => warn!("Failed to decode SwarmGossip payload: {}", e),
                        }
                    } else if let Some(ref router) = swarm {
                        match bincode::deserialize::<NodeHealthGossip>(&msg.payload) {
                            Ok(mut gossip) => { gossip.forwarded = false; router.update_peer(gossip); }
                            Err(e) => warn!("Failed to decode SwarmGossip payload: {}", e),
                        }
                    }
                }

                // Raft RPCs — deserialise, dispatch, send response.
                MessageType::RaftAppend
                | MessageType::RaftVote
                | MessageType::RaftResponse => {
                    let payload: RaftRpcPayload = bincode::deserialize(&msg.payload)?;

                    let response = match payload {
                        RaftRpcPayload::AppendEntriesReq(req) => {
                            let resp = handle.append_entries(req).await.map_err(|e| {
                                std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
                            })?;
                            RaftRpcPayload::AppendEntriesResp(resp)
                        }
                        RaftRpcPayload::RequestVoteReq(req) => {
                            let resp = handle.request_vote(req).await.map_err(|e| {
                                std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
                            })?;
                            RaftRpcPayload::RequestVoteResp(resp)
                        }
                        _ => {
                            warn!("Unexpected RaftRpcPayload variant; skipping");
                            continue;
                        }
                    };

                    let resp_bytes = bincode::serialize(&response)?;
                    let resp_msg   = Message::new(MessageType::RaftResponse, req_id, resp_bytes);
                    framed.send(resp_msg).await?;
                }

                other => {
                    debug!("Unhandled message type {:?} on inbound connection; ignoring", other);
                }
            }
        }

        Ok(())
    }

    // Filesystem mutation broadcast

    pub async fn broadcast_fs_mutation(&self, mutation: &FsMutation) {
        if self.peers.is_empty() { return; }

        let payload = match bincode::serialize(mutation) {
            Ok(b)  => b,
            Err(e) => { warn!("Failed to serialise FsMutation: {}", e); return; }
        };

        for (peer_id, conn_slot) in self.gossip_conns.iter() {
            let addr = match self.peers.get(peer_id) {
                Some(a) => *a,
                None    => continue,
            };
            let payload_clone = payload.clone();
            let peer_id_owned = peer_id.clone();
            let req_id        = self.next_req_id.fetch_add(1, Ordering::Relaxed);
            let conn_slot     = conn_slot.clone();

            tokio::spawn(async move {
                let msg = Message::new(MessageType::FsWrite, req_id, payload_clone);
                let mut guard = conn_slot.lock().await;

                if guard.is_none() {
                    match tokio::net::TcpStream::connect(addr).await {
                        Ok(s)  => *guard = Some(Framed::new(s, NifsiCodec)),
                        Err(e) => {
                            debug!("FS broadcast connect to {} failed: {}", peer_id_owned, e);
                            return;
                        }
                    }
                }

                if let Some(ref mut framed) = *guard {
                    if let Err(e) = framed.send(msg).await {
                        debug!("FS broadcast send to {} failed: {}", peer_id_owned, e);
                        *guard = None;
                    }
                }
            });
        }
    }

    // Swarm gossip broadcast — epidemic / K-random-peer fan-out

    pub async fn epidemic_gossip(&self, gossip: &NodeHealthGossip, k: usize) {
        if self.peers.is_empty() { return; }

        let payload = match bincode::serialize(gossip) {
            Ok(b)  => b,
            Err(e) => { warn!("Failed to serialise gossip: {}", e); return; }
        };

        let peer_ids: Vec<&String> = self.peers.keys().collect();
        let n = peer_ids.len();
        let k = k.min(n);

        let mut indices: Vec<usize> = (0..n).collect();
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64;
        let mut rng = seed ^ (seed >> 33);
        for i in 0..k {
            // xorshift64 step
            rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
            let j = i + (rng as usize % (n - i));
            indices.swap(i, j);
        }

        for i in 0..k {
            let peer_id  = peer_ids[indices[i]].as_str();
            let conn_slot = match self.gossip_conns.get(peer_id) {
                Some(c) => c.clone(),
                None    => continue,
            };
            let addr = match self.peers.get(peer_id) {
                Some(a) => *a,
                None    => continue,
            };
            let payload_clone = payload.clone();
            let peer_id_owned = peer_id.to_owned();
            let req_id        = self.next_req_id.fetch_add(1, Ordering::Relaxed);

            tokio::spawn(async move {
                let msg = Message::new(MessageType::SwarmGossip, req_id, payload_clone);
                let mut guard = conn_slot.lock().await;

                if guard.is_none() {
                    match tokio::net::TcpStream::connect(addr).await {
                        Ok(s)  => *guard = Some(Framed::new(s, NifsiCodec)),
                        Err(e) => {
                            debug!("Gossip connect to {} failed: {}", peer_id_owned, e);
                            return;
                        }
                    }
                }

                if let Some(ref mut framed) = *guard {
                    if let Err(e) = framed.send(msg).await {
                        debug!("Gossip send to {} failed: {}", peer_id_owned, e);
                        *guard = None;
                    }
                }
            });
        }
    }

    // Slice transfer RPC client methods

    pub async fn request_slice(
        &self,
        peer_id: &str,
        req: SliceTransferRequest,
    ) -> Result<SliceTransferResponse, Box<dyn std::error::Error + Send + Sync>> {
        let addr = self
            .peers
            .get(peer_id)
            .ok_or_else(|| format!("unknown peer: {peer_id}"))?;

        let conn_slot = self
            .slice_conns
            .get(peer_id)
            .ok_or_else(|| format!("no slice connection slot for peer: {peer_id}"))?;

        let req_id = self.next_req_id.fetch_add(1, Ordering::Relaxed);
        let payload_bytes = bincode::serialize(&req)?;
        let msg = Message::new(MessageType::SliceRequest, req_id, payload_bytes);

        let mut guard = conn_slot.lock().await;

        if guard.is_none() {
            debug!("Opening TCP slice connection to peer {} at {}", peer_id, addr);
            let stream = TcpStream::connect(addr).await?;
            *guard = Some(Framed::new(stream, NifsiCodec));
        }

        let framed = guard.as_mut().unwrap();

        if let Err(e) = framed.send(msg).await {
            *guard = None;
            return Err(Box::new(e));
        }

        match framed.next().await {
            Some(Ok(resp_msg)) => {
                if resp_msg.header.msg_type != MessageType::SliceTransfer {
                    return Err(format!("unexpected message type in slice response: {:?}", resp_msg.header.msg_type).into());
                }
                let resp: SliceTransferResponse = bincode::deserialize(&resp_msg.payload)?;
                Ok(resp)
            }
            Some(Err(e)) => {
                *guard = None;
                Err(Box::new(e))
            }
            None => {
                *guard = None;
                Err("peer closed connection during slice transfer".into())
            }
        }
    }

    pub async fn send_slice_transfer(
        &self,
        peer_id: &str,
        resp: &SliceTransferResponse,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr = self
            .peers
            .get(peer_id)
            .ok_or_else(|| format!("unknown peer: {peer_id}"))?;

        let conn_slot = self
            .slice_conns
            .get(peer_id)
            .ok_or_else(|| format!("no slice connection slot for peer: {peer_id}"))?;

        let req_id = self.next_req_id.fetch_add(1, Ordering::Relaxed);
        let payload_bytes = bincode::serialize(resp)?;
        let msg = Message::new(MessageType::SliceTransfer, req_id, payload_bytes);

        let mut guard = conn_slot.lock().await;
        if guard.is_none() {
            let stream = TcpStream::connect(addr).await?;
            *guard = Some(Framed::new(stream, NifsiCodec));
        }

        let framed = guard.as_mut().unwrap();
        if let Err(e) = framed.send(msg).await {
            *guard = None;
            return Err(Box::new(e));
        }
        Ok(())
    }

    pub async fn broadcast_slice_announcement(&self, announcement: &SliceAnnouncement) {
        if self.peers.is_empty() { return; }

        let payload = match bincode::serialize(announcement) {
            Ok(b)  => b,
            Err(e) => { warn!("Failed to serialise SliceAnnouncement: {}", e); return; }
        };

        for (peer_id, conn_slot) in self.gossip_conns.iter() {
            let addr = match self.peers.get(peer_id) {
                Some(a) => *a,
                None    => continue,
            };
            let payload_clone = payload.clone();
            let peer_id_owned = peer_id.clone();
            let req_id        = self.next_req_id.fetch_add(1, Ordering::Relaxed);
            let conn_slot     = conn_slot.clone();

            tokio::spawn(async move {
                let msg = Message::new(MessageType::SliceAnnouncement, req_id, payload_clone);
                let mut guard = conn_slot.lock().await;

                if guard.is_none() {
                    match tokio::net::TcpStream::connect(addr).await {
                        Ok(s)  => *guard = Some(Framed::new(s, NifsiCodec)),
                        Err(e) => {
                            debug!("Slice announcement connect to {} failed: {}", peer_id_owned, e);
                            return;
                        }
                    }
                }

                if let Some(ref mut framed) = *guard {
                    if let Err(e) = framed.send(msg).await {
                        debug!("Slice announcement send to {} failed: {}", peer_id_owned, e);
                        *guard = None;
                    }
                }
            });
        }
    }

    // Outbound RPC helper

    async fn rpc(
        &self,
        peer_id: &str,
        msg_type: MessageType,
        body: &RaftRpcPayload,
    ) -> Result<RaftRpcPayload, Box<dyn std::error::Error + Send + Sync>> {
        let addr = self
            .peers
            .get(peer_id)
            .ok_or_else(|| format!("unknown peer: {peer_id}"))?;

        let conn_slot = self
            .conns
            .get(peer_id)
            .ok_or_else(|| format!("no connection slot for peer: {peer_id}"))?;

        let req_id = self.next_req_id.fetch_add(1, Ordering::Relaxed);
        let payload_bytes = bincode::serialize(body)?;
        let msg = Message::new(msg_type, req_id, payload_bytes);

        let mut guard = conn_slot.lock().await;

        // Lazy-connect or reconnect after a previous error.
        if guard.is_none() {
            debug!("Opening TCP connection to peer {} at {}", peer_id, addr);
            let stream = TcpStream::connect(addr).await?;
            *guard = Some(Framed::new(stream, NifsiCodec));
        }

        let framed = guard.as_mut().unwrap();

        if let Err(e) = framed.send(msg).await {
            *guard = None; // drop broken connection
            return Err(Box::new(e));
        }

        match framed.next().await {
            Some(Ok(resp_msg)) => {
                let resp_payload: RaftRpcPayload = bincode::deserialize(&resp_msg.payload)?;
                Ok(resp_payload)
            }
            Some(Err(e)) => {
                *guard = None;
                Err(Box::new(e))
            }
            None => {
                *guard = None;
                Err("peer closed connection".into())
            }
        }
    }
}

// RaftNetwork implementation

#[async_trait]
impl RaftNetwork for TcpMeshNetwork {
    async fn send_append_entries(
        &self,
        peer_id: &str,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn std::error::Error + Send + Sync>> {
        let resp = self
            .rpc(peer_id, MessageType::RaftAppend, &RaftRpcPayload::AppendEntriesReq(req))
            .await?;
        match resp {
            RaftRpcPayload::AppendEntriesResp(r) => Ok(r),
            _ => Err("unexpected response type for AppendEntries RPC".into()),
        }
    }

    async fn send_request_vote(
        &self,
        peer_id: &str,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn std::error::Error + Send + Sync>> {
        let resp = self
            .rpc(peer_id, MessageType::RaftVote, &RaftRpcPayload::RequestVoteReq(req))
            .await?;
        match resp {
            RaftRpcPayload::RequestVoteResp(r) => Ok(r),
            _ => Err("unexpected response type for RequestVote RPC".into()),
        }
    }
}

