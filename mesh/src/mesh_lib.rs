pub mod protocol;
pub mod swarm;
pub mod transport;

pub use swarm::{NodeHealthGossip, SwarmConfig, SwarmRouter};
pub use transport::{FsMutationHandler, TcpMeshNetwork};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use nifsi_raft::node::RaftHandle;
// Node descriptor
pub struct Node {
    pub id:      String,
    pub address: SocketAddr,
}
// Mesh initialisation
pub async fn start_mesh(
    bind_addr:    SocketAddr,
    peers:        HashMap<String, SocketAddr>,
    raft_handle:  RaftHandle,
    swarm_config: SwarmConfig,
    on_mutation:  Option<FsMutationHandler>,
) -> std::io::Result<(TcpMeshNetwork, Arc<SwarmRouter>)> {
    let network = TcpMeshNetwork::new(peers);
    let swarm   = Arc::new(SwarmRouter::new(swarm_config));

    //Wraping the network in an Arc so that the inbound listener can use it for epidemic
    //reforwarding without trying to create a circular dependency.
    let network_arc = Arc::new(network.clone());

    // Spawn the inbound listener. Runs for the lifetime of the daemon
    let listener_handle   = raft_handle.clone();
    let listener_swarm    = swarm.clone();
    let listener_network  = network_arc.clone();
    let listener_mutation = on_mutation.clone();
    tokio::spawn(async move {
        if let Err(e) = TcpMeshNetwork::listen(
            bind_addr,
            listener_handle,
            Some(listener_swarm),
            Some(listener_network),
            listener_mutation,
        ).await {
            tracing::error!("Mesh listener failed: {}", e);
        }
    });

    Ok((network, swarm))
}

