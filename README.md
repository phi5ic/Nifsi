# Nifsi: A Distributed Userspace Operating System Layer

<div align="center">
  
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://github.com/yourusername/nifsi/blob/main/LICENSE)
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

## How It Works

> The Network Is The Computer.

The project pulls heavy inspiration from Plan 9's philsophy as quoted above, but trying to fix the slow networks and lack of POSIX compatibility that held it back.

- **The Filesystem is the Interface**: Nifsi mounts a distributed FUSE filesystem at `/nifsi/` on every node. There is no web dashboard or custom GUI. Much more simple that way. Health can be checked by running `cat /nifsi/cluster/health`. Standard Unix tools like `ls`, `grep`, and `watch` work normally just like any other Linux machines.

- **Encrypted Mesh**: All nodes connect via a peer-to-peer WireGuard mesh. The network is fully encrypted by default with zero trust. No exceptions, not even on a local LAN.

- **Compute Follows Data**: When you spawn a process, the scheduler figures out where the required data lives and runs the compute there.

- **Flat Topology**: There is no permanent "master" node to act as a single point of failure. All nodes are equal, and temporary leaders are only elected via Raft when consensus is strictly necessary (like writing files or updating metadata).

- **Bulletproof Writes**: A file is either fully written or not written at all. Partial states are never exposed to users.

## Technology Stack

- **Language**: written entirely in *Rust* for memory safety and C like performance.

- **Filesystem**:*FUSE* allows me to build crash-safe userspace filesystems without writing custom kernel modules.

- **Tracing**: using *eBPF* for safe, userspace kernel instrumentation.

- **Networking**: fast, minimal-attack-surface tunnels powered by *WireGuard*.

# What Stage The Project is at Now ?

Im trying to make the v1 possible, v1 is built for small, low latency LAN clusters of roughly 5 to 50 nodes. It is perfect for home labs, small offices and research enviroments where you just wnat to share compute and data across a few machines without managing infrastructure stack.

# License
Nifsi is released under the **AGPL v3**. It is strictly open-source. You can use it for anything, but if you modify the code and allow users to interact with it over a network, you are legally required to publish your modified source code back to the community.
