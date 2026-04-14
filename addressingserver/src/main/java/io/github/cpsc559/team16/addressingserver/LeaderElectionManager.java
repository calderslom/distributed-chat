package io.github.cpsc559.team16.addressingserver;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.cpsc559.team16.common.dto.AddrServerRecord;
import io.github.cpsc559.team16.common.dto.ServerRole;
import io.github.cpsc559.team16.common.messaging.BaseAddrServerMessage;
import io.github.cpsc559.team16.common.messaging.ElectionMessage;
import io.github.cpsc559.team16.common.utilities.NIOMessageChannel;

import static io.github.cpsc559.team16.common.logging.DebugLogger.*;
/**
 * <h1>LeaderElectionManager</h1>
 * <p>
 * Manages the leader election process for an Addressing Server using the Bully Algorithm.
 * This class is responsible for handling leader election messages, determining the leader, 
 * and ensuring fault tolerance by re-electing a leader if necessary.
 * </p>
 * <p>
 * The election process follows these steps:
 * </p>
 * <ol>
 *     <li>Identify the highest available PID among peers and self.</li>
 *     <li>If a process detects a failure in the leader, it initiates an election.</li>
 *     <li>Election messages are sent to all peers with a higher PID.</li>
 *     <li>If no higher PID responds, the process declares itself the leader.</li>
 *     <li>If a higher PID responds, it waits for a leader announcement.</li>
 * </ol>
 */
public class LeaderElectionManager {

    //==========================================================================
    // Fields
    //==========================================================================

    /**
     * <p>
     * The AddressingServer instance that instantiated this LeaderElectionManager object.
     * </p>
     */
    private final AddressingServer server;


    /**
     * <p>
     * Configuration instance for accessing Addressing Server settings.
     * </p>
     */
    private final AddrServerConfig config;

    /**
     * <p>
     * Manages peer connections for communication during elections.
     * </p>
     */
    private final PeerManager peerManager;

    /**
     * <p>
     * Flag indicating whether an election is currently running.
     * </p>
     */
    private boolean running;

    /**
     * <p>
     * Timeout (in milliseconds) to wait for a "Bully" response before assuming leadership.
     * </p>
     */
    private int bullyResponseTimeout = 5000;

    /**
     * <p>
     * Timeout (in milliseconds) to wait for a leader announcement before re-initiating an election.
     * </p>
     */
    private int leaderAnnouncementTimeout = 12000;

    /**
     * <p>
     * Flag used to track if a "Bully" response has been received.
     * </p>
     */
    private volatile boolean bullyResponseReceived = false;

    /**
     * <p>
     * Flag used to track if a leader announcement has been received.
     * </p>
     */
    private volatile boolean leaderAnnouncementReceived = false;

    /**
     * <p>
     * Flag indicating whether an election is in progress.
     * </p>
     */
    private volatile boolean midElection = false;

    private Thread activeElectionThread = null;

    //==========================================================================
    // Constructors and Election State Management
    //==========================================================================

    /**
     * <p>
     * Retrieves the current election status.
     * </p>
     *
     * @return {@code true} if an election is in progress; {@code false} otherwise.
     */
    public boolean isMidElection() {
        return midElection;
    }

    /**
     * <p>
     * Sets the mid-election flag.
     * </p>
     *
     * @param midElection {@code true} to mark election in progress; {@code false} otherwise.
     * @return The updated mid-election flag.
     */
    public boolean setMidElection(boolean midElection) {
        this.midElection = midElection;
        server.getNetworkManager().setMidElection(midElection);
        // Additional actions can be taken when mid-election is set.
        return this.midElection;
    }

    /** Used as a lock to prevent multiple election threads from being created. */
    private final AtomicBoolean electionLock = new AtomicBoolean(false);

    /**
     * <p>
     * Constructs a LeaderElectionManager.
     * This initializes the manager with the given AddressingServer, its configuration, and peer connections.
     * </p>
     *
     * @param server the AddressingServer that instantiated this LeaderElectionManager object.
     */
    public LeaderElectionManager(AddressingServer server) {
        this.server = server;
        this.config = server.getConfig();
        this.peerManager = server.getPeerManager();
        this.running = false;

    }

    //==========================================================================
    // Message Processing
    //==========================================================================

    /**
     * <p>
     * Processes an incoming election-related message.
     * Depending on the message payload, it delegates to the appropriate handler.
     * </p>
     * <ul>
     *   <li>"Election" &rarr; {@link #handleElection(Long, NIOMessageChannel)}</li>
     *   <li>"Leader" &rarr; {@link #handleLeaderAnnouncement(Long)}</li>
     *   <li>"Bully" &rarr; {@link #handleBully()}</li>
     * </ul>
     *
     * @param channel    The {@link SocketChannel} that received the message.
     * @param nioChannel The {@link NIOMessageChannel} used for message decoding and encoding.
     * @param message    The parsed {@link BaseAddrServerMessage} containing the election details.
     */
    public void processElectionMessage(SocketChannel channel, NIOMessageChannel nioChannel, BaseAddrServerMessage<?> message) {
        String payload = (String) message.getPayload();
        // GUARD: If another thread has already promoted this process to PRIMARY, we ignore the stale election message.
        if (this.config.getRole() == ServerRole.PRIMARY && "Election".equals(payload)) {
            return;
        }
        new Thread(() -> {
            long senderPID = message.getSenderPID();
            switch (payload) {
                case "Election" -> handleElection(senderPID, nioChannel);
                case "Leader" -> handleLeaderAnnouncement(senderPID);
                case "Bully" -> handleBully();
                default -> System.err.println("Received unknown election message payload: " + payload);
            }
        }).start();

    }

    //==========================================================================
    // Election Message Handlers
    //==========================================================================

    /**
     * <p>
     * Handles an "Election" message, responding only if the sender has a lower PID.
     * If this process has a higher PID, it sends a "Bully" message back and optionally starts its own election if one is not already in progress.
     * </p>
     *
     * @param senderPID   The PID of the process that sent the election message.
     * @param peerChannel The {@link NIOMessageChannel} of the sender.
     */
    private void handleElection(Long senderPID, NIOMessageChannel peerChannel) {
        System.out.println("LEM: Received election message from PID: " + senderPID);
        setMidElection(true);
        // Only respond if sender's PID is lower than self.
        if (senderPID < getSelfPID()) {
            System.out.println("LEM: Responding to election message. Sending bully to PID: " + senderPID);
            bully(senderPID);  // Send a "Bully" message to the sender.
            if (!running) {
                initiateElection();  // Start an election if not already in progress.
            }
        }
    }

    /**
     * <p>
     * Handles a "Bully" message.
     * This marks that a higher PID exists, preventing self-declaration as leader.
     * </p>
     */
    private void handleBully() {
        System.out.println("LEM: Received bully message. A higher PID exists.");
        bullyResponseReceived = true;
    }

    /**
     * <p>
     * Handles a "Leader" message.
     * Updates the known leader and stops the election process.
     * </p>
     * <p>
     * Only replicas that are not elected to PRIMARY receive a "Leader" message.
     * </p>
     *
     * @param senderPID The PID of the announced leader.
     */
    private void handleLeaderAnnouncement(Long senderPID) {
        debug(DEBUG_NORMAL, "LEM: Received leader announcement from PID: " + senderPID);
        leaderAnnouncementReceived = true;
        running = false;

        // Interrupt the sleeping election thread so it doesn't attempt to promote itself
        if (activeElectionThread != null && activeElectionThread.isAlive()) {
            activeElectionThread.interrupt();
        }

        setMidElection(false);
        followNewLeader(senderPID);
    }

    //==========================================================================
    // Utility Methods
    //==========================================================================

    /**
     * <p>
     * Retrieves a collection of all connected peers' message channels.
     * </p>
     *
     * @return A collection of {@link NIOMessageChannel} objects representing connected peers.
     */
    private Collection<NIOMessageChannel> getPeerChannels() {
        return peerManager.getChannels().values();
    }

    /**
     * <p>
     * Retrieves the PID of the current process.
     * </p>
     *
     * @return The PID of this Addressing Server.
     */
    private long getSelfPID() {
        return config.getPID();
    }

    //==========================================================================
    // Election Control Methods
    //==========================================================================

    /**
     * <p>
     * Initiates the leader election process.
     * If a higher PID exists, election messages are sent to them.
     * If no higher PID responds, this process declares itself the leader.
     * </p>
     */
    public void initiateElection() {

        // GUARD: Check the election lock, if it's false, lock it and continue, return otherwise.
        if (!electionLock.compareAndSet(false, true)) {
            return;
        }
        setMidElection(true);
        activeElectionThread = new Thread(() -> {
            System.out.println("LEM: Initiating election...");
            try {
                if (leaderAnnouncementReceived) return;
                if (!running) {
                    debug(DEBUG_NORMAL, "LEM: started running... [" + new Date().getTime() + "]");
                    this.running = true;
                    this.bullyResponseReceived = false;
                    this.leaderAnnouncementReceived = false;
                    boolean higherPIDExists = false;  // Track if a higher PID exists.

                    // Retrieve all peer PIDs from the registry.
                    Collection<Long> peerPIDS = server.getAddrServerRegistry().getRecords().keySet();

                    // Loop through all peer PIDs (excluding self).
                    for (Long peerPID : peerPIDS) {
                        if (!peerPID.equals(config.getPID())) {
                            System.out.println("LEM: considering sending election message to peer with PID - " + peerPID);
                            if (peerPID > getSelfPID()) {
                                System.out.println("LEM: peer PID is higher than my PID.");
                                higherPIDExists = true;
                                sendTo(generateElectionMessage(), peerPID);
                            }
                        }
                    }

                    // If no higher PID exists, declare self as leader.
                    if (!higherPIDExists) {
                        if (leaderAnnouncementReceived) return;
                        debug(DEBUG_NORMAL, "LEM: No higher PID found. Declaring self as leader.");
                        assumeLeadership();
                    } else {
                        debug(DEBUG_NORMAL,"LEM: Waiting for bullys from higher PIDs... for " + bullyResponseTimeout + "ms");
                        // Wait for a "Bully" response; if none, declare self as leader.
                        Thread.sleep(bullyResponseTimeout);
                        // Check to see if a leader announcement was received while sleeping.
                        if (leaderAnnouncementReceived) {
                            debug(DEBUG_NORMAL,"LEM: Leader was announced during wait. PID " + getSelfPID() + " aborting promotion.");
                            return;
                        }
                        if (!bullyResponseReceived) {
                            System.out.println("LEM: no bully responses received within window.");
                            assumeLeadership();
                        } else {
                            System.out.println("LEM: bully response received. waiting for leader msg now, for " + leaderAnnouncementTimeout + "ms");
                            // Wait for a "Leader" announcement; if none, restart election.
                            Thread.sleep(leaderAnnouncementTimeout);
                            if (!leaderAnnouncementReceived) {
                                running = false;
                                initiateElection();
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                System.err.println("Interrupted while waiting for response during election.");
                this.running = false;
            } finally {
                setMidElection(false);
                electionLock.set(false);
                activeElectionThread = null;
            }
        });
        activeElectionThread.start();
    }

    /**
     * <p>
     * Sends a "Bully" message to a peer to challenge their election.
     * </p>
     *
     * @param peerPID The PID of the peer to send the "Bully" message to.
     */
    public void bully(Long peerPID) {
        sendTo(generateBullyMessage(), peerPID);
    }

    /**
     * <p>
     * Declares this process as the new leader and notifies all peers.
     * </p>
     */
    public void assumeLeadership() {
        if (leaderAnnouncementReceived) {
            debug(DEBUG_BASIC, "assumeLeadership aborted: LeaderAnnouncement was already received.");
            return;
        }
        this.running = false;
        // Shut down the Replica request coordinator (used for replication by replicas, not the primary)
        server.getNetworkManager().shutdownCoordinatorRequest();
        // Use before promoteSelf
        clearFailedLeader(server.getPeerManager().getPrimaryPID());
        ElectionHelper.promoteSelf(this.server);
        this.setMidElection(false);

        ElectionMessage leaderMessage = generateLeaderMessage();

        // Notify all peers (excluding self) about the new leader.
        for (Long peerPID : server.getAddrServerRegistry().getRecords().keySet()) {
            if (!peerPID.equals(config.getPID())) {
                sendTo(leaderMessage, peerPID);
                System.out.println("LEM: Sent leader message to peer with PID " + peerPID);
            }
        }

        new java.util.Timer().schedule(
                new java.util.TimerTask() {
                    @Override
                    public void run() {
                        performRegistryAudit();
                    }
                },
                8000 // 8-second delay to accommodate network process synchronization requests.
        );
    }

    /**
     * Initiates a comprehensive audit of all registries within the Addressing Server.
     * <p>
     * This method acts as the "cleanup" phase of a successful leader election.
     * It triggers a reconciliation of both the internal Addressing Server (Peer)
     * registry and the Chat Server registry. Any stale or "ghost" records found
     * during this process are purged and their failure is broadcasted to the
     * rest of the cluster.
     * </p>
     * <p>
     * After an 8-second 'settle' period following an election, the new leader process
     * used this method to ensure its internal records match the physical network state.
     * It purges any nodes that exist in the database but failed to establish
     * a TCP connection during the transition.
     * </p>
     */
    public void performRegistryAudit() {
        Long myPid = this.config.getPID();
        ConnectionCleanupManager cleanupMgr = this.server.getCleanupManager();

        this.server.getPeerManager().auditRegistryConnections(myPid, cleanupMgr);
        this.server.getChatServerManager().auditRegistryConnections(myPid, cleanupMgr);
    }


    /**
     * <p>
     * Sets a new leader for the Addressing Server.
     * Clears any previous leader and updates configuration accordingly.
     * </p>
     *
     * @param newLeaderPID The PID of the new leader.
     */
    public void followNewLeader(Long newLeaderPID) {
        if (newLeaderPID == null) {
            System.err.println("New leader PID is null. Cannot set new leader.");
            return;
        } else if (newLeaderPID.equals(server.getPeerManager().getPrimaryPID())) {
            System.out.println("New leader PID matches current leader - no changes made.");
            return;
        }
        this.running = false;
        // Clear the record of the current PRIMARY since it has failed
        clearFailedLeader(server.getPeerManager().getPrimaryPID());

        // Give the new Primary 5 seconds to finish its promotion logic and open its server sockets.
        try {
            System.out.println("LEM: Waiting for new Primary to initialize...");
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Retrieve the record of the REPLICA being promoted
        AddrServerRecord record = server.getAddrServerRegistry().getRecords().get(newLeaderPID);
        if (record != null) {
            System.out.println("Promoting another REPLICA process to PRIMARY...");
            record.setRole(ServerRole.PRIMARY);
            server.getPeerManager().synchronizeWithPrimary(record);
        } else {
            System.err.println("WARNING: Critical election failure. " +
                    "No AddrServerRecord found in the registry for PID: " + newLeaderPID + ".");
        }
        this.setMidElection(false);
    }

    /**
     * <p>
     *    Used during failover by REPLICA addressing servers after a new leader has been elected. This method
     *    performs the following cleanup actions related to the failed PRIMARY addressing server:
     * </p>
     * <ul>
     *     <li>Sends a {@code ShutdownMessage} to fence the failed PRIMARY from the network.</li>
     *      <li>Removes the PRIMARY addressing server from the registry and closes the
     *      NIOMessageChannel if it is still open.</li>
     *      <li>Removes the server record associated with the failed PRIMARY connection from this processes
     *       internal set of records using the {@code AddrServerRegistry}.</li>
     *      <li>Cancels the selection key and closes the channel gracefully.</li>
     * </ul>
     */
    public void clearFailedLeader(long failedPrimaryPID) {
        if (failedPrimaryPID != 0L) {
            // TODO: Create a helper class that spawns a thread which attempts to connect to the failed primary several times (sleep between attempts).
            // Hand the class the failed PRIMARY details at this point so it has a frozen snapshot of the network config.
            //server.getCleanupManager().sendShutdownRequestToPrimary(config.getPID(), failedPrimaryPID);
            server.getCleanupManager().disconnectFromPrimaryQuietly();
            server.getAddrServerRegistry().removeRecordByKey(failedPrimaryPID);
        }
    }

    //==========================================================================
    // Message Generation Methods
    //==========================================================================

    /**
     * <p>
     * Generates an election message.
     * </p>
     *
     * @return A {@link BaseAddrServerMessage} representing an election request.
     */
    public BaseAddrServerMessage generateElectionMessage() {
        return ElectionMessage.election(getSelfPID());
    }

    /**
     * <p>
     * Generates a bully message.
     * </p>
     *
     * @return A {@link BaseAddrServerMessage} representing a bully challenge.
     */
    public BaseAddrServerMessage generateBullyMessage() {
        return ElectionMessage.bully(getSelfPID());
    }

    /**
     * <p>
     * Generates a leader message.
     * </p>
     *
     * @return A {@link BaseAddrServerMessage} announcing the new leader.
     */
    public ElectionMessage generateLeaderMessage() {
        return ElectionMessage.leader(getSelfPID());
    }

    //==========================================================================
    // Communication Method
    //==========================================================================

    /**
     * <p>
     * Sends a message to the given peer.
     * This method opens a new connection to the peer and immediately sends the specified message.
     * It uses blocking I/O and then closes the connection once the message is sent.
     * </p>
     *
     * @param message The {@link BaseAddrServerMessage} to send.
     * @param peerPID The PID of the target peer.
     */
    public void sendTo(BaseAddrServerMessage message, Long peerPID) {
        AddrServerRecord peer = server.getAddrServerRegistry().getRecords().get(peerPID);
        if (peer == null) {
            System.err.println("Peer with PID " + peerPID + " not found.");
            return;
        }

        String address = peer.getHostAddress();
        int addressPort = peer.getPeerPort();

        try (Socket addressSocket = new Socket(address, addressPort)) {
            // Create a PrintWriter with auto-flush enabled to send the message.
            PrintWriter out = new PrintWriter(addressSocket.getOutputStream(), true);
            out.println(message.toJson());
        } catch (IOException e) {
            System.err.println("Error sending message to peer: " + e.getMessage());
        }
    }
}