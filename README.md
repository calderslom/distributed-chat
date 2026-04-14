
## DISTRIBUTED IRC-STYLE CHAT SYSTEM

A fault-tolerant and scalable chat system built in Java (JDK 21),
deployed via Docker. Designed around distributed systems principles including
gossip-based replication, dynamic client routing, and automatic failure recovery.

### Client GUI

---------------------------------------------------------

![Alice and Bob demonstrating fault-tolerant 
messaging across several chat servers](docs/images/chat_log_example.jpg)


### Background

---------------------------------------------------------


This project originated as a five-person university group project for CPSC 559
(Distributed Systems). My core responsibilities were:

- The entire Addressing Server module (excluding LeaderElectionManager)
- The inter-process communication protocol — including the full messaging layer,
  service discovery, replica synchronization, and consistency guarantees
- The common module (all classes except ChatLog and ChatLogUpdate)

My teammates implemented the Chat Server and Client modules, as well as the original 
LeaderElectionManager class (which I substantially reworked in this fork).

After the course ended, I branched the repository and personally invested 60+ hours
stabilizing the system — resolving fundamental bugs in leader election and failover recovery, 
while also making significant improvements to the Chat Server and Client modules.

The core architecture and feature set are unchanged from before,
as my goal was to fully realize the original design by
delivering a version that works correctly and reliably.


### Overview

---------------------------------------------------------

This project implements a multi-server IRC-style chat platform where no single
server represents a point of failure. Clients connect through a centralized addressing
layer that routes them to available chat servers. Chat servers replicate messages
between each other, and the system recovers from node failures
automatically.

Both the chat server and addressing server layers scale horizontally — additional nodes 
can be spun up to increase capacity and integrate into the live network 
without requiring any configuration.

The entire system is containerized through Docker — with Gradle handling 
dependency resolution and builds inside the container. As a result, Docker is the 
only prerequisite to run the system on any platform.


### Architecture

---------------------------------------------------------

The system has three distinct components:

Addressing Servers
- Manage client routing and maintain a registry of active chat servers.
- Utilize a Primary/Replica model with automatic leader election to ensure
high availability and state persistence.

Chat Servers
- Accept client connections and persist chat logs locally.
- Propagate messages to peer servers using a gossip-based push-pull
protocol with vector timestamp ordering.

Clients
- Connect via a command-line style GUI.
- Automatically reconnect to the network and resend unacknowledged messages
when a disconnection occurs.

### System Architecture Diagram

---------------------------------------------------------

![Steady-state diagram showing: four Chat Servers, 
three Addressing Servers (one primary) and three clients connected 
via the network layer](docs/images/steady-state_system_architecture_487x695.jpg)

### Key Technical Features

---------------------------------------------------------

#### Non-Blocking I/O

Java NIO server sockets allow for high client concurrency without assigning a dedicated 
thread per client. Each connection maintains its own write queue and partial-read buffer, 
correctly handling cases where messages are written or received across multiple I/O events.

#### Client Routing

The Primary Addressing Server routes new clients to the most populated chat server 
below its maximum capacity. 
Routing decisions are based on the live network state — 
only servers with a formally registered and active NIO connection to the Primary are 
eligible, preventing clients from being directed to stale registry entries.


#### Gossip-Based Message Replication

Vector timestamps maintain message ordering across all nodes without requiring a global lock.
Used in conjunction with a gossip-based push-pull protocol, messages are efficiently 
propagated across chat servers — ensuring a consistent view of the chat log without 
relying on a centralized broadcast or coordinator.


#### Message Delivery Guarantees

Clients cache unacknowledged messages and retry on reconnection.
Servers ACK all received messages to confirm delivery.

#### Heartbeat Failure Detection

Chat servers and addressing servers each monitor their own peers via adaptive 
RTT-based timeouts, minimizing false positives on degraded networks. When an
unresponsive node is detected by its peer, a notification is sent
to the Primary Addressing Server via the peer's persistent NIO connection, 
which also serves as a passive liveness check for the Primary. 
All failure information converges at the Primary, which maintains an authoritative view 
of the network topology — centralizing failure awareness while keeping detection itself 
decentralized and domain specific.

#### Automatic Fault Recovery

Failed chat servers rebuild state by pulling the full chat log from peers on reconnection,
requiring no persistent storage and keeping servers stateless and simple to deploy.
The system recovers without operator intervention;
any clients connected to a failed server are automatically rerouted,
ensuring transparent service.

#### Consistency Guarantees
The Primary Addressing Server enforces strong consistency across replicas by ensuring any
topology change is acknowledged by all replicas before being committed.
This guarantees that any view of the network topology —
whether read from the primary or a replica —
reflects the same state, preventing split-brain scenarios during failover.

#### Leader Election and Failover
Addressing Server failover is managed via a Bully-based election protocol, where the 
replica with the highest network ID assumes the role of Primary upon detecting a failure. 
Rather than rebuilding the network from scratch, chat servers and replicas 
instead send a synchronization message to the new Primary, 
preserving the network state across the transition. 

The new Primary validates any synchronization request against its internal records before 
acceptance, preventing unauthorized or stale processes from rejoining the topology. After the
network settles, the Primary performs an internal registry audit, 
purging any nodes that failed to synchronize within a set window.
This ensures the system transitions to a new leader with minimal disruption while 
maintaining the integrity of the network.

#### Deployable On Any Machine Running Docker
Gradle is used in conjunction with Docker to automatically pull the dependencies 
required to build the image for each service: addressingserver, chatserver, client.
Individual docker containers are then spun up for process using these images, meaning that
any platform capable of running Docker can run each component of the system.

### Tech Stack

---------------------------------------------------------
- Java JDK 21
- Java NIO (non-blocking sockets)
- JSON serialization for inter-process communication
- Docker: platform-agnostic containerized deployment services

### How to Run the System

---------------------------------------------------------
#### Prerequisites
- Docker
- Make (optional, for convenience commands)


#### Setting the granularity of system logging

- Each service has its own 'debug level' with a range of [0,5] — with a setting of five 
providing the most verbose system reporting.
    - These are defined as environment variables in the root directory's .env file.
    - A setting of zero is recommended for the Client service if you intend to use the 
     client as a chatroom.


#### Start the network (primary addressing server, two replicas, and two chat servers)

***With Make:***
```bash
make full-network-build
```

***Without Make:***
```bash
# Build images for each service
docker compose build --no-cache

# Spin up the containers
docker compose --profile all up -d --no-deps --scale addressingserver-backup=2 --scale chatserver=2 --scale client=0
```

#### Connecting to the chatroom

Each client uses an interactive terminal for its GUI. Open a new terminal, navigate to
the root directory, and run:

```bash
docker compose run --rm client
```

### Known Limitations

Service discovery is implemented via a shared Docker volume;
only the Primary Addressing Server has write access,
while all other services read from it to locate the Primary during startup and failover.
Writes are performed atomically using a write-and-rename pattern,
preventing dirty reads after leader elections. 

The current constraint is that the shared volume requires all nodes to run within the same 
Docker network — in a production multi-host environment this would be replaced with a 
DNS A record or an external service discovery layer.
