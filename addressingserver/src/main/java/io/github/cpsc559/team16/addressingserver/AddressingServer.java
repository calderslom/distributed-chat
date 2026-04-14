package io.github.cpsc559.team16.addressingserver;
// External Dependencies

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel; // Used for conditionals that don't rely on non-null checks
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.github.cpsc559.team16.common.dto.ServerRole;
import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

import io.github.cpsc559.team16.common.messaging.MessageIDGenerator;
import io.github.cpsc559.team16.common.messaging.Roles;
import io.github.cpsc559.team16.common.utilities.NIOMessageChannel;


public class AddressingServer {

    /**
     * Used by the PRIMARY Addressing server to write discovery information to
     * disk - e.g. hostname, port no, etc. This data is accessed by other network processes
     * that want to initiate contact with the PRIMARY (e.g. upon instantiation, or after an election).
     * <par>
     * This replaces the original DNS functionality wherein a domain A record is dynamically updated
     * with the address of an addressing server in the distributed chat network that is currently
     * acting as the primary.
     * </par>
     */
    private PrimaryDiscoveryManager discoveryManager = null;

    /**
     * Retrieves the {@link PrimaryDiscoveryManager} for this AddressingServer.
     * <p>
     * This manager is responsible for publishing the server's network details (hostname and ports)
     * to a shared discovery file. This file is used by other replicas, chat servers, and clients
     * to locate the current PRIMARY addressing server.
     * </p>
     * <p>
     * <strong>Constraint:</strong> This method can only be called when the server is
     * operating in the {@link ServerRole#PRIMARY} role. If a REPLICA attempts to access
     * the discovery manager, an exception is thrown to prevent accidental or
     * unauthorized writes to the shared state.
     * </p>
     *
     * @return the active {@code PrimaryDiscoveryManager} instance for this server.
     * @throws IllegalStateException if this server's role is not {@code PRIMARY}.
     */
    public PrimaryDiscoveryManager getDiscoveryManager() {
        if (config.getRole() != ServerRole.PRIMARY) {
            throw new IllegalStateException("REPLICA attempted to access DiscoveryManager!");
        }
        if (discoveryManager == null) {
            this.discoveryManager = new PrimaryDiscoveryManager(this.config);
        }
        return discoveryManager;
    }


    /**
     * The network configuration for this {@code AddressingServer} process.
     */
    private final AddrServerConfig config;

    public AddrServerConfig getConfig() {
        return config;
    }

    /**
     * The process responsible for initiating elections and handling election messages from
     * {@code AddressingServer} peers.
     */
    private final LeaderElectionManager leaderElectionManager;

    public LeaderElectionManager getLeaderElectionManager() {
        return leaderElectionManager;
    }


    /**
     * Handles outbound synchronization requests sent from this {@code REPLICA} AddressingServer to the {@code PRIMARY}.
     * <p>
     * This component is responsible for constructing and sending specific request messages, such as those for
     * {@link io.github.cpsc559.team16.common.dto.ChatServerRecord}s and {@link io.github.cpsc559.team16.common.dto.AddrServerRecord}s.
     * </p>
     * <p>
     * It is only initialized when this process is running as a {@code REPLICA}. On {@code PRIMARY} instances, this field
     * remains {@code null} and should not be accessed.
     * </p>
     * <p>
     * The {@code ReplicaRequestManager} is provided to the {@link ReplicaRequestCoordinator} thread once a connection to the
     * {@code PRIMARY} server has been established. It supports flexible sync logic, such as triggering full synchronization
     * every N cycles and more frequent chat server syncs.
     * </p>
     */
    private ReplicaRequestManager replicaRequestManager;

    /**
     * Sets the {@link ReplicaRequestManager} used by this AddressingServer.
     * <p>
     * This is typically invoked by the {@link ReplicaRequestCoordinator} once it has successfully
     * established a connection to the Primary and initialized the manager instance.
     * </p>
     *
     * @param replicaRequestManager the {@code ReplicaRequestManager} instance to store.
     */
    public void setReplicaRequestManager(ReplicaRequestManager replicaRequestManager) {
        this.replicaRequestManager = replicaRequestManager;
    }

    public ReplicaRequestManager getReplicaRequestManager() {
        return this.replicaRequestManager;
    }

    /**
     * Background thread responsible for periodically requesting synchronization data from the {@code PRIMARY} server.
     * <p>
     * This thread is only active on {@code REPLICA} AddressingServers. It coordinates periodic sync requests to
     * retrieve up-to-date {@link io.github.cpsc559.team16.common.dto.ChatServerRecord} and
     * {@link io.github.cpsc559.team16.common.dto.AddrServerRecord} entries.
     * </p>
     * <p>
     * The coordinator uses a {@link java.util.function.Supplier} to retrieve the latest {@link NIOMessageChannel}
     * connected to the {@code PRIMARY}. If the channel is unavailable at startup (e.g., connection has not yet been
     * established), it will wait and retry until the primary becomes reachable.
     * </p>
     * <p>
     * Once a connection is active, the coordinator will:
     * <ul>
     *   <li>Send {@code ChatServerRecord} requests on a regular interval.</li>
     *   <li>Send {@code AddrServerRecord} requests once every N cycles (default is 6).</li>
     * </ul>
     * This component is lifecycle-managed by the {@code AddrServerNetworkManager}, which starts and stops the thread
     * alongside other internal systems.
     * </p>
     */
    ReplicaRequestCoordinator replicaRequestCoordinator;

    public ReplicaRequestCoordinator getReplicaRequestCoordinator() {
        return this.replicaRequestCoordinator;
    }


    /**
     * The process responsible for handling ping messages and managing the ping timeout.
     */
    private final PingManager pingManager;

    public PingManager getPingManager() {
        return pingManager;
    }


    /**
     * The process responsible for managing {@code ChatServer} connections.
     */
    private final ChatServerRegistry chatServerRegistry;

    public ChatServerRegistry getChatServerRegistry() {
        return chatServerRegistry;
    }

    /**
     * The process responsible for managing {@code AddrServerRecord} records.
     */
    private final AddrServerRegistry addrServerRegistry;

    public AddrServerRegistry getAddrServerRegistry() {
        return addrServerRegistry;
    }

    /**
     * The process responsible for managing interactions between the Primary
     * {@code AddressingServer} and its replicas.
     */
    private final PeerManager peerManager;

    public PeerManager getPeerManager() {
        return peerManager;
    }

    /**
     * Manages synchronization and state consistency between the PRIMARY and all registered REPLICA AddressingServers.
     * <p>
     * This coordinator encapsulates the logic needed to track acknowledgments, manage pending events,
     * handle retry logic, and ensure that all state changes
     * (e.g. new server registrations/removal, server updates, leadership changes)
     * are safely replicated across the distributed network.
     * </p>
     */
    private final ReplicaSyncCoordinator replicaSyncCoordinator;

    /**
     * Returns the {@link ReplicaSyncCoordinator} responsible for managing strong consistency across AddressingServers.
     *
     * @return the {@code ReplicaSyncCoordinator} used by this AddressingServer instance.
     */
    public ReplicaSyncCoordinator getReplicaSyncCoordinator() {
        return replicaSyncCoordinator;
    }

    /**
     * Coordinator responsible for managing the registration process for both
     * {@code ChatServer}s and {@code AddressingServer} replicas.
     *
     * <p>This component centralizes registration logic that was previously scattered
     * across multiple classes.
     * It ensures that all processes registering with the primary {@code AddressingServer}
     * are properly acknowledged, synchronized with existing state, and broadcast to other peers.
     * </p>
     *
     * <p>The {@code RegistrationCoordinator} uses the {@link BroadcastManager},
     * {@link ReplicaSyncCoordinator}, {@link ConnectionCleanupManager} and other internal components
     * to enforce consistency, issue acknowledgments, handle I/O errors,
     * and handle special-case logic like first-replica registration.</p>
     */
    private final RegistrationCoordinator registrationCoordinator;

    /**
     * Returns the internal {@link RegistrationCoordinator}, which handles registration workflows
     * for new {@code ChatServer}s and {@code AddressingServer} replicas.
     *
     * @return the {@code RegistrationCoordinator} used by this {@code AddressingServer}.
     */
    public RegistrationCoordinator getRegistrationCoordinator() {
        return registrationCoordinator;
    }


    /**
     * The process responsible for managing interactions between the
     * {@code AddressingServer} and {@code ChatServer}'s
     */
    private final ChatServerManager chatServerManager;

    public ChatServerManager getChatServerManager() {
        return chatServerManager;
    }


    /**
     * The process responsible for managing interactions between the
     * {@code AddressingServer} and {@code ChatServer}'s
     */
    private final ClientManager clientManager;

    public ClientManager getClientManager() {
        return clientManager;
    }

    /**
     * The BroadcastManager is responsible for sending messages to all active channels.
     * It works directly with the live channel maps retrieved from PeerManager and ChatServerManager,
     * ensuring that any changes in those maps (such as channel additions or removals) are immediately visible.
     */
    private final BroadcastManager broadcastManager;

    /**
     * Retrieves the BroadcastManager instance for this AddressingServer.
     *
     * @return the BroadcastManager used to broadcast messages across channels.
     */
    public BroadcastManager getBroadcastManager() {
        return broadcastManager;
    }

    /**
     * The ConnectionCleanupManager centralizes the logic for cleaning up and closing failed connections.
     * It holds references to both PeerManager and ChatServerManager so that any channel failures can be
     * promptly removed from the live channel maps and properly closed.
     */
    private final ConnectionCleanupManager cleanupManager;

    /**
     * Retrieves the ConnectionCleanupManager instance for this AddressingServer.
     *
     * @return the ConnectionCleanupManager used for cleaning up failed connections.
     */
    public ConnectionCleanupManager getCleanupManager() {
        return cleanupManager;
    }


    /**
     * Responsible for opening listener channels, managing the selector,
     * and dispatching accepted connections for the AddressingServer.
     */
    private final AddrServerNetworkManager networkManager;

    /**
     * Retrieves the AddrServerNetworkManager instance for this AddressingServer.
     *
     * @return the AddrServerNetworkManager used for network operations.
     */
    public AddrServerNetworkManager getNetworkManager() {
        return networkManager;
    }

    /**
     * The {@code MessageIDGenerator} instance used by this server to produce globally unique message IDs.
     * <p>
     * This generator ensures that every message requiring acknowledgment or ordering has a distinct identifier,
     * which is critical for maintaining consistency guarantees (e.g., in replication or event tracking).
     * </p>
     * <p>
     * The generator is initialized once per process and typically updated with the server's assigned PID
     * after registration, ensuring message IDs are globally unique across all AddressingServer processes.
     * </p>
     */
    private final MessageIDGenerator genMID;


    /**
     * Returns the {@link MessageIDGenerator} associated with this server.
     * <p>
     * This allows access to generate unique message IDs when constructing messages that
     * require tracking across replicas or clients.
     * </p>
     *
     * @return the server's {@code MessageIDGenerator} instance.
     */
    public MessageIDGenerator getMessageIDGenerator() {
        return this.genMID;
    }

    /**
     * The current highest assigned PID in the distributed network.
     * Typically, this number only grows, i.e. PIDs are not reassigned.
     * <p>
     * However, in the event of a PRIMARY failure, the REPLICA server that is elected leader
     * will find the highest PID in the network by searching through all of the
     * active {@code ServerRecord}'s set {@code pidCounter} to that value.
     * </p>
     * Count begins at 1, not zero. Any PID = 0 represents an unregistered process that should
     * not be allowed to interact with any network process until it has been registered and assigned
     * a PID.
     */
    private Long pidCounter = 0L;

    /**
     * Generates a unique identifier for a chat server by incrementing the internal counter.
     *
     * @return a unique {@code Long} representing the chat server's ID.
     */
    public Long generatePID() {
        return ++this.pidCounter;
    }


    /**
     * Returns the highest process ID (PID) currently assigned in the network,
     * considering both {@code AddressingServer}s and {@code ChatServer}s.
     * <p>
     * This method is used to ensure that all future PIDs assigned during
     * registration events (e.g. new servers joining the network) are strictly
     * greater than any currently active PID. This prevents PID reuse and
     * maintains global uniqueness of process identifiers across both (addr and chat)
     * system roles. Distinct PIDs are essential for interprocess communication.
     * </p>
     *
     * @return the highest PID found in either the {@code AddrServerRegistry} or {@code ChatServerRegistry}.
     */
    public Long getMaxPidInNetwork() {
        Long maxAddrServerPID = this.addrServerRegistry.getMaxPID();
        Long maxChatServerPID = this.chatServerRegistry.getMaxPID();
        return (maxAddrServerPID > maxChatServerPID) ? maxAddrServerPID : maxChatServerPID;
    }

    /**
     * Sets the internal process ID counter to a specified value.
     * <p>
     * This method is typically called during replica promotion, where the newly
     * elected {@code PRIMARY} {@code AddressingServer} must resume assigning
     * unique process IDs without conflicting with existing network processes.
     * </p>
     */
    public void setPidCounterToNetworkMax() {
        this.pidCounter = getMaxPidInNetwork();
    }

    /**
     * Indicates whether the server has been instructed to restart. Typically used after an orphaned or failed
     * AddressingServer has been instructed to terminate and reinitialize.
     * <p>
     * This flag is marked {@code volatile} to ensure visibility across threads. It can be
     * safely updated by any thread (e.g. the {@code AddrServerReadDispatcher}) and read by the main thread
     * to trigger a controlled in-process restart of the {@code AddressingServer}.
     * </p>
     * <p>
     * When {@code true}, the main event loop exits and the server is re-instantiated as a new process.
     * </p>
     */
    private volatile boolean restartRequested = false;

    /**
     * Sets the {@code restartRequested} flag to {@code true}.
     * <p>
     * This method is typically called when a shutdown or restart message is received from the network.
     * It signals the {@code AddressingServer} to exit its current event loop and reinitialize as a new replica.
     * </p>
     */
    public void requestRestart() {
        this.restartRequested = true;
        shutdown();
    }

    /**
     * Checks whether the server has been flagged for restart.
     *
     * @return {@code true} if a restart has been requested, {@code false} otherwise.
     */
    public boolean isRestartRequested() {
        return this.restartRequested;
    }


    /**
     * Constructs an AddressingServer with an {@code AddrServerConfig} object storing its network details.
     * <p>
     * This constructor initializes a new, empty address log to track registered chat servers.
     * </p>
     */
    public AddressingServer() {
        String debugEnv = System.getenv().getOrDefault("AS_DEBUG_LEVEL", "2");
        int debugLevel = Integer.parseInt(debugEnv);
        setDebugLevel(debugLevel);
        System.out.println("DEBUG SYSTEM INITIALIZED TO LEVEL: " + debugLevel);

        this.config = new AddrServerConfig();
        this.genMID = new MessageIDGenerator(this.config::getPID);

        this.chatServerRegistry = new ChatServerRegistry();
        this.chatServerManager = new ChatServerManager(chatServerRegistry);
        this.clientManager = new ClientManager(chatServerRegistry, chatServerManager);

        this.addrServerRegistry = new AddrServerRegistry(this);
        this.peerManager = new PeerManager(this);

        this.cleanupManager = new ConnectionCleanupManager(peerManager, chatServerManager, genMID);
        this.broadcastManager = new BroadcastManager(genMID, peerManager.getChannels(), chatServerManager.getChannels(), cleanupManager);

        this.replicaSyncCoordinator = new ReplicaSyncCoordinator(peerManager, broadcastManager, cleanupManager);
        this.cleanupManager.setReplicaCoordinator(replicaSyncCoordinator);

        this.registrationCoordinator = new RegistrationCoordinator(
                config,
                peerManager,
                chatServerManager,
                broadcastManager,
                replicaSyncCoordinator,
                cleanupManager,
                chatServerRegistry,
                addrServerRegistry,
                this::generatePID,
                genMID
        );


        try {
            this.networkManager = new AddrServerNetworkManager(cleanupManager, this.config,
                    replicaSyncCoordinator.getPendingEvents(),
                    this::createReplicaRequestCoordinator);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize network manager", e);
        }

        this.leaderElectionManager = new LeaderElectionManager(this);
        this.pingManager = new PingManager(this);
    }


    public void registerPrimaryAddrServer() {

        Long pid = generatePID();
        config.setPID(pid);

        String host = config.getHostAddress();
        if (host == null || host.isEmpty() || host.equals("localhost")) {
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                System.err.println("[PRIMARY ERROR] Failed to resolve local hostname: " + e.getMessage());
                host = "localhost"; // Use fallback hostname
            }
        }
        System.out.println("PRIMARY initializing with Hostname: " + host + " and PID: " + pid);

        // Update Local Registry (Self-Register by creating an AddrServerRecord)
        addrServerRegistry.registerAddrServer(
                pid,
                host,
                config.getClientPort(),
                config.getReplicaPort(),
                config.getChatServerPort(),
                config.getRole()
        );
        // Publish details to shared volume for network discovery
        try {
            PrimaryDiscoveryManager discovery = new PrimaryDiscoveryManager(config);
            discovery.publish();
        } catch (IOException e) {
            System.err.println("CRITICAL: Primary could not publish discovery file: " + e.getMessage());
        }
    }

    /**
     * Creates a new {@link ReplicaRequestCoordinator} instance for this AddressingServer.
     * <p>
     * This method is intended to be used by components (e.g., {@link AddrServerNetworkManager})
     * that need to create or restart the {@code ReplicaRequestCoordinator} thread responsible
     * for issuing periodic synchronization requests to the Primary AddressingServer.
     * </p>
     *
     * <p>
     * The returned thread will:
     * <ul>
     *   <li>Attempt to fetch the {@link NIOMessageChannel} associated with the Primary via {@link PeerManager}.</li>
     *   <li>Instantiate a {@link ReplicaRequestManager} using the channel and the local {@link MessageIDGenerator}.</li>
     *   <li>Use the {@code getPID} method in {@link AddrServerConfig} to retrieve the network PID of the instantiating process.</li>
     *   <li>Publish the newly created {@code ReplicaRequestManager} to the internal field via a callback.</li>
     * </ul>
     * </p>
     *
     * @return a new, unstarted {@code ReplicaRequestCoordinator} thread configured for this AddressingServer instance.
     */
    public ReplicaRequestCoordinator createReplicaRequestCoordinator() {
        // If a coordinator already exists, try to shut it down first to avoid thread leaks (we only ever need one).
        if (this.replicaRequestCoordinator != null) {
            this.replicaRequestCoordinator.shutdown();
        }
        this.replicaRequestCoordinator = new ReplicaRequestCoordinator(
                this.genMID,
                this::setReplicaRequestManager, // Save reference to internal field
                this.peerManager::getPrimaryNIOChannel
        );
        return this.replicaRequestCoordinator;
    }


    /**
     * Attempts to register this replica AddressingServer with the PRIMARY AddressingServer.
     * <p>
     * This method attempts to open a network connection to the PRIMARY and send a registration
     * message. If the first attempt fails (e.g., the PRIMARY is temporarily unreachable), it retries
     * once after a brief pause.
     * </p>
     * <p>
     * If a connection is successfully established, the resulting {@link SocketChannel} is passed
     * to the {@code AddrServerNetworkManager} to be monitored via its {@code Selector}.
     * If both attempts fail, an error is logged, and the replica will not be registered.
     * </p>
     *
     * <p><strong>Retry behavior:</strong></p>
     * <ul>
     *   <li>Initial connection attempt is made immediately.</li>
     *   <li>If it fails, a second attempt is made after a short delay (500 ms).</li>
     *   <li>If both attempts fail, no further retries are attempted in this call.</li>
     * </ul>
     *
     * @see PeerManager#registerWithPrimary(String, int, int, int, int)
     * @see AddrServerNetworkManager#openPersistentChannel(SocketChannel)
     */
//    public void registerReplicaAddrServer() throws IOException {
//        // Retrieve the details for the primary addressing server
//        int discoveryAttempts = 0;
//        int maxDiscoveryAttempts = 10; // Total 20 seconds
//        boolean fileFound = false;
//
//        System.out.println("Replica starting: Searching for Primary discovery file...");
//
//        while (discoveryAttempts < maxDiscoveryAttempts) {
//            if (config.refreshPrimaryDetails()) {
//                fileFound = true;
//                break;
//            }
//
//            discoveryAttempts++;
//            System.out.printf("Discovery file not found. Attempt %d/%d. Retrying in 2s...%n",
//                    discoveryAttempts, maxDiscoveryAttempts);
//
//            try {
//                Thread.sleep(2000);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                throw new IOException("Startup discovery interrupted", e);
//            }
//        }
//
//        if (!fileFound) {
//            System.err.println("CRITICAL: Primary discovery file missing after timeout. Shutting down this process.");
//            System.exit(1);
//        }
//
//        Optional<SocketChannel> maybeChannel = peerManager.registerWithPrimary(
//                config.getPrimaryHostAddress(), config.getPrimaryReplicaPort(),
//                config.getClientPort(), config.getReplicaPort(), config.getChatServerPort());
//
//        // Retry connection using exponential backoff
//        int attempts = 0;
//        while (maybeChannel.isEmpty() && attempts < 5) {
//
//            long sleepTime = (long) Math.pow(2, attempts) * 1000;
//            System.err.println("First attempt to register with primary addressing server failed. " +
//                    "Retrying in " + sleepTime +" seconds.");
//            try {
//                Thread.sleep(sleepTime);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                System.err.println("Retry sleep interrupted.");
//            }
//            maybeChannel = peerManager.registerWithPrimary(
//                    config.getPrimaryHostAddress(), config.getPrimaryReplicaPort(),
//                    config.getClientPort(), config.getReplicaPort(), config.getChatServerPort());
//            attempts++;
//        }
//
//        if (maybeChannel.isPresent()) {
//            SocketChannel channel = maybeChannel.get();
//            try {
//                networkManager.openPersistentChannel(channel);
//            } catch (IOException ioe) {
//                System.err.println("Error occurred while opening persistent channel to PRIMARY: " + ioe.getMessage());
//            }
//        } else {
//            System.err.println("Failed to register with PRIMARY after 2 attempts.");
//            // Optional: we can escalate the issue by starting a leader election.
//        }
//    }

    /**
     * Close all connections in the {@link AddrServerNetworkManager}.
     * Shut down the thread running the heartbeat pings in {@link PingManager}.
     * <p>
     * <b>Throw a party with one candle! Play the cake song:</b>
     * </p>
     * <p><a href="https://youtu.be/6ug6Bbc6diA?si=Empv4R9Wg6kdo1lD">"We do what we must, because we can"</a></p>
     */
    public void shutdown() {
        networkManager.requestShutdown();
        pingManager.shutdown();
    }


    /**
     * Initializes the AddressingServer network access by binding all required NIO channels.
     * These are "listening channels" used solely to monitor incoming connections.
     * <p>
     * Starts the main event loop for this {@code AddressingServer} instance.
     * </p>
     */
    public void start() throws IOException {
        // new Thread(() -> pingManager.run()).start();

        networkManager.openListenerChannels(config.getClientPort(),
                config.getReplicaPort(), config.getChatServerPort());

        networkManager.startEventLoop(new AddrServerReadDispatcher(this));

        pingManager.shutdown();
    }

    public static void main(String[] args) {
        /* This timeout is necessary for proper output in the new terminal window..
         * ... without it, the initial output to console occurs before the terminal is open.
         */
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }


        // Track whether this is the first time the loop has occurred.
        boolean firstIteration = true;

        while (true) {
            AddressingServer server = new AddressingServer();
            String initialServerRole = System.getenv("AS_ROLE");
            String currentServerRole = firstIteration ? initialServerRole : "REPLICA";

            if (currentServerRole.equals(Roles.PRIMARY)) {
                System.out.println("Launching AddressingServer as PRIMARY");
                server.registerPrimaryAddrServer();
            } else {
                System.out.println("Launching AddressingServer as REPLICA");
                boolean connected = server.getPeerManager().registerWithPrimary();
                if (!connected) {
                    System.err.println("Critical: Could not connect to PRIMARY after retries. Restarting initialization...");
                    firstIteration = false;
                    try {
                        TimeUnit.SECONDS.sleep(2);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    continue; // Re-attempt initial connection & registration
                }
                System.out.println("REPLICA registration successful.");
            }

            System.out.println("AddressingServer: Starting PingManager...");
            Thread pingThread = new Thread(() -> server.getPingManager().run());
            pingThread.setDaemon(true);
            pingThread.start();

            try {
                server.start();         // blocks in main event loop of AddrServerNetworkManager
            } catch (IOException e) {
                System.err.println("Server exited due to IOException: " + e.getMessage());
            }

            // If server didn’t request a restart and we are at this point -> exit the JVM
            if (!server.isRestartRequested()) {
                System.out.println("Server exited normally. Shutting down.");
                break;
            }

            System.out.println("Restart requested. Reinitializing as REPLICA...");

            firstIteration = false; // All restarts become replicas
            try {
                TimeUnit.MILLISECONDS.sleep(500); // brief pause before retry
            } catch (InterruptedException ignored) {
            }

        }


    }
}