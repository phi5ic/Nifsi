<div align="center">
  
# Nifsi: A Distributed Userspace Operating System Layer

  
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://github.com/phi5ic/nifsi/blob/main/LICENSE)
![Platform: Linux](https://img.shields.io/badge/Platform-Linux-lightgrey.svg)
![Status: Development](https://img.shields.io/badge/Status-Active%20Development-orange.svg)

</div>

Nifsi is a distributed userspace operating system layer that runs on top of Linux. This project tries to overthrow the overcomplexity of modern orchestration by tossing out YAML manifests, the containers, and the heavy orchestration layers.

Instead, you just install a lightweight daemon and join a mesh. Instantly your machines unify into one system with a single filesystem, a shared process space and transparent resource scheduling.

*This isnt a container orchestrator (like Kubernetes), a hypervisor, or a replacement for the Linux kernel.*

It is a thin layer that handles the distribution so you don't have to. Nifsi handles the network, Linux handles the hardware and your applications do everything else.

## Project Status & Realistic Expectations
**Full transparency**: I am a single developer building this alongside my full-time university coursework. Nifsi is a massive undertaking being built at a pace of roughly 15 hours a week (that is if I dont have exams or Im busy with other things in life). The estimated timeline to a stable v1 release is 12 to 16 months although it would take more as Im learning as Im doing the project along the way.

Because of this, I will try to keep the development steady but it will be subject to delays. There will be quiet periods when Im tired or there are exams when I have to take priority. I'm building this to solve a real problem and learn deep systems architecture. If anyone is following along, expect a marathon, not a sprint.

**Use Of AI**: certain files will be created with AI such as the web interface, such files will be mentioned as comments in the file itself. I have made some benchmarks with AI as well, since I believe it's an overall a better way to test the project thoroughly.

## What works today (v0.1)

| Feature | Status |
|---|---|
| Distributed FUSE filesystem mounted on every node | Works |
| Raft leader election and replicated metadata (SQLite-backed log) | Works |
| Encrypted node-to-node mesh (ChaCha20-Poly1305 AEAD framing) | Works |
| Peer discovery and health gossip (Boids-style swarm) | Works |
| HADR storage engine: slices, parity cohorts, self-repair | Implemented, repair tested on healthy data only |
| CLI: `status`, `inspect`, `slice-query`, `repair`, `join`, `leave`, `evict` | Present; `status`, `inspect`, `slice-query`, `repair` verified |
| Small web dashboard | Present (in the daemon crate) |
| Test suite | 44 tests across `fs`, `mesh` and `raft` |

I did manage to verify it on a local 3 node cluster: leader election settles, a file written on one node is readable from the other. 

## What is not built yet

- Shared process space and remote process execution
- Resource scheduler and "compute follows data"
- eBPF tracing
- A `cat /nifsi/cluster/health` style virtual health file (use `nifsi status` for now)
- Corruption and mid-write crash testing, and dead-peer detection that reflects in `status` quickly

## Security

Mesh traffic between nodes is encrypted and authenticated with ChaCha20-Poly1305. Nifsi has not had an independent security review and is a pre release software, so please dont expose it to untrusted networks. If you do find a security problem, please open an issue or contact me directly.

## Quickstart (3 nodes on one machine)

Requires Linux, FUSE ('fusermount'), and Rust 1.85 or newer as most crates use edition 2024.

```bash
git clone https://github.com/phi5ic/Nifsi
cd Nifsi
cargo build --workspace --release
cargo test --workspace
```

Start 3 daemons, each with its own id, port, data directory, control socket and mount poitn, and pointed at the other two. Run './target/release/nifsid --help' for the exact flags; the shape is:

```bash
./target/release/nifsid --id node1 --bind 127.0.0.1:7331 \
  --data-dir /tmp/nifsi-cluster/node1/data \
  --socket /tmp/nifsi-cluster/node1.sock \
  --mount /tmp/nifsi-cluster/node1/mount \
  --peer node2=127.0.0.1:7332 --peer node3=127.0.0.1:7333
# repeat for node2 (7332) and node3 (7333)
```
Then:
 
```bash
# cluster state: roles, leader, peers, storage-engine counters
./target/release/nifsi --socket /tmp/nifsi-cluster/node1.sock status
 
# write on one node, read on another
echo "hello" > /tmp/nifsi-cluster/node1/mount/hello.txt
cat /tmp/nifsi-cluster/node3/mount/hello.txt
 
# look at how a file is stored
./target/release/nifsi --socket /tmp/nifsi-cluster/node1.sock inspect hello.txt
./target/release/nifsi --socket /tmp/nifsi-cluster/node1.sock slice-query hello.txt
 
# repair one slice
./target/release/nifsi --socket /tmp/nifsi-cluster/node1.sock repair hello.txt --chunk-idx 0 --slice-idx 0
```

## How It Works

> Everything is a file, everywhere.

The project pulls heavy inspiration from Plan 9's philosophy as quoted above, but trying to fix the slow networks and lack of POSIX compatibility that held it back.

- **The Filesystem is the Interface**: Nifsi mounts a distributed FUSE filesystem at `/nifsi/` on every node. There is no web dashboard or custom GUI. Much more simple that way. Health can be checked by running `cat /nifsi/cluster/health`. Standard Unix tools like `ls`, `grep`, and `watch` work normally just like any other Linux machines.

- **Encrypted Mesh**: All nodes connect via a peer-to-peer ChaCha20-Poly1305 AEAD encrypted mesh. The network is fully encrypted by default with zero trust. No exceptions, not even on a local LAN.

- **Compute Follows Data**: When you spawn a process, the scheduler figures out where the required data lives and runs the compute there.

- **Flat Topology**: There is no permanent "master" node to act as a single point of failure. All nodes are equal, and temporary leaders are only elected via Raft when consensus is strictly necessary (like writing files or updating metadata).

- **Bulletproof Writes**: A file is either fully written or not written at all. Partial states are never exposed to users.

## Technology Stack

- **Language**: written entirely in *Rust* for memory safety and C like performance.

- **Filesystem**:*FUSE* allows me to build crash-safe userspace filesystems without writing custom kernel modules.

- **Tracing**: using *eBPF* for safe, userspace kernel instrumentation.

- **Networking**: zero-trust, authenticated AEAD peer-to-peer framing powered by *ChaCha20-Poly1305* and Boids swarm routing.

# What Stage is The Project at Now ?

Im trying to make the v1 possible, v1 is built for small, low latency LAN clusters of roughly 5 to 50 nodes. It is perfect for home labs, small offices and research environments or drones where you just want to share compute and data across a few machines without managing infrastructure stack.

***UPDATE**; The core system is complete and works, you can test it out in a trusted network.

***UPDATE**: The skeletal system is complete and compiles clean.

**Workspace**: five crates wired together as a Cargo workspace:

| Crate | Purpose |
|---|---|
| `core` | Shared wire protocol and config |
| `mesh` | Encrypted mesh, swarm discovery, heartbeats |
| `raft` | SQLite-backed Raft node and log store |
| `fs` | FUSE filesystem and the HADR storage engine |
| `daemon` | `nifsid`, the node daemon (includes the web dashboard) |
| `cli` | `nifsi`, the command-line client |

## Roadmap
 
1. Corruption and failure testing: bit-flip repair, mid-write crash, leader loss, node rejoin
2. Reliable dead-peer detection and real RTT/disk metrics in `status`
3. Remote process execution (`nifsi run`)
4. Scheduler with data-aware placement
5. eBPF-based tracing

# License
Nifsi is released under the **AGPL v3**. It is strictly open-source. You can use it for anything, but if you modify the code and allow users to interact with it over a network, you are legally required to publish your modified source code back to the community.
