package io.github.cpsc559.team16.addressingserver;

import java.io.IOException;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.github.cpsc559.team16.common.dto.AddrServerRecord;
import io.github.cpsc559.team16.common.dto.ServerRole;
import io.github.cpsc559.team16.common.messaging.*;
import io.github.cpsc559.team16.common.utilities.NIOMessageChannel;
import io.github.cpsc559.team16.common.utilities.NetworkUtils;
import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

/**
 * Manages peer registration, update propagation, and persistent communication
 * between the primary AddressingServer and its replicas.
 * <p>
 * This class maintains a set of persistent {@code SocketChannel} connections
 * to replicas and supports synchronization by pushing updates to them.
 * </p>
 */
public class PeerManager {

    /**
     * The AddressingServer instance that owns this PeerManager.
     * This is used to access the server's configuration and state.
     */
    private final AddressingServer server;

    /**
     * A thread-safe mapping of persistent peer connections.
     * Each peer (replica AddressingServer) is tracked by its associated {@code SocketChannel}
     * and wrapped in an {@code NIOMessageChannel} for structured messaging.
     */
    private final Map<SocketChannel, NIOMessageChannel> peerChannels;

    public Map<SocketChannel, NIOMessageChannel> getPeerChannels() {
        return peerChannels;
    }

    /**
     * Returns a HashMap of SocketChannel and NIOChannel for all the
     * current addressing server connections.
     *
     * @return a map of {@code SocketChannel} to {@code NIOMessageChannel} for peer tracking.
     */
    public Map<SocketChannel, NIOMessageChannel> getChannels() {
        return this.peerChannels;
    }

    /**
     * Checks whether a replica with the specified {@code pid} is currently registered and connected.
     * <p>
     * This method iterates through all active {@link NIOMessageChannel}s in the peer channel map
     * and returns {@code true} if any connected replica reports a non-null, matching PID.
     * </p>
     *
     * @param pid the process ID to check for an existing registered replica connection
     * @return {@code true} if a connected replica with the specified PID is found; {@code false} otherwise
     */
    public Boolean isRegistered(Long pid) {
        for (NIOMessageChannel channel : peerChannels.values()) {
            Long nioPID = channel.getServerPID();
            if (nioPID != 0L && nioPID.equals(pid)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns all connected replica channels that have been assigned a non-zero PID.
     *
     * @return a collection of active {@link NIOMessageChannel}s linked to registered replicas.
     */
    public Collection<NIOMessageChannel> getRegisteredNIOChannels() {
        return peerChannels.values()
                .stream()
                .filter(ch -> ch.getServerPID() != 0L)
                .toList();
    }

    /**
     * Returns a concurrent map of all connected replica channels that have been assigned a non-zero PID.
     * <p>
     * The map is keyed by the replica's PID, with values being their corresponding {@link NIOMessageChannel}.
     * Unregistered channels (PID == 0L) are excluded.
     * </p>
     *
     * @return a {@link ConcurrentHashMap} of replica PIDs to active {@link NIOMessageChannel}s.
     */
    public ConcurrentHashMap<Long, NIOMessageChannel> getRegisteredReplicaChannelMap() {
        ConcurrentHashMap<Long, NIOMessageChannel> registered = new ConcurrentHashMap<>();
        for (NIOMessageChannel ch : peerChannels.values()) {
            Long pid = ch.getServerPID();
            if (pid != 0L) {
                registered.put(pid, ch);
            }
        }
        return registered;
    }

    public ConcurrentHashMap<Long, NIOMessageChannel> getRegisteredReplicaChannelMapNoFailedPID(Long failedPID) {
        ConcurrentHashMap<Long, NIOMessageChannel> registered = new ConcurrentHashMap<>();
        for (NIOMessageChannel ch : peerChannels.values()) {
            Long pid = ch.getServerPID();
            if (pid != 0L && pid != failedPID) {
                registered.put(pid, ch);
            }
        }
        return registered;
    }


    /**
     * The registry containing all known {@code AddrServerRecord} entries,
     * used to track state across the distributed network of AddressingServers.
     */
    private final AddrServerRegistry registry;

    public void debugPrintAllServers() {
        this.registry.debugPrintAllServers();
    }

    /**
     * Updates or inserts a record into the shared AddrServer registry.
     * <p>
     * This method is typically called when receiving an {@code UpdateMessage}
     * containing new or modified AddrServer information.
     * </p>
     *
     * @param record the record to insert or update.
     */
    public void updateRecords(AddrServerRecord record) {
        registry.updateOrInsertRecord(record);
    }


    /**
     * Constructs a {@code PeerManager} and binds it to a shared {@code AddrServerRegistry}.
     */
    public PeerManager(AddressingServer server) {
        this.server = server;
        this.registry = server.getAddrServerRegistry();
        this.peerChannels = new ConcurrentHashMap<>();
    }

    /**
     * Removes a persistent connection and its associated record, logging the removal using the connection’s network PID.
     * <p>
     * This method performs the following actions:
     * <ul>
     *   <li>Removes the mapping between the {@code SocketChannel} and its corresponding {@code NIOMessageChannel}
     *       from the internal collection of persistent connections in the PeerManager.</li>
     *   <li>Removes the associated {@code AddrServerRecord}
     *       that identifies the remote process connected via the {@code SocketChannel}.</li>
     * </ul>
     * </p>
     *
     * @param channel the {@code SocketChannel} representing the connection to remove.
     *                <strong>NOTE:</strong> This method does not close the {@code SocketChannel}; closing the channel is the responsibility
     *                of the caller.
     * @return true if a record for the remote process existed in the {@link AddrServerRegistry}; false otherwise.
     */
    public boolean removeRemoteProcess(SocketChannel channel) {
        NIOMessageChannel ch = this.peerChannels.get(channel);
        if (ch == null) return false;

        Long pidFromChannel = ch.getServerPID();

        // Stage 1: Remove Channel whether the process is registered or not.
        this.peerChannels.remove(channel);
        try {
            System.out.printf("Purging peer connection [%s] for PID %d %n", channel.getRemoteAddress(), pidFromChannel);
        } catch (IOException ignore) {
        }

        // Stage 2: Remove any record that may exist for the peer connection from the registry
        return this.registry.removeRecordByKey(pidFromChannel);
    }

    /**
     * Removes the remote process associated with the given channel and then closes the channel.
     * <p>
     * This method performs two main actions:
     * <ol>
     *   <li>Deregisters the remote process by calling {@code removeRemoteProcess(channelToRemove)},
     *       removing any references to the remote process from internal data structures.</li>
     *   <li>Attempts to close the provided {@code SocketChannel}. If an {@code IOException} occurs during
     *       the close operation, it is caught and ignored.</li>
     * </ol>
     * </p>
     *
     * @param channelToRemove the {@code SocketChannel} representing the connection to be removed and closed.
     * @return true if a record for the remote process existed in the {@link AddrServerRegistry}; false otherwise.
     */
    public boolean removeProcessCloseConnection(SocketChannel channelToRemove) {
        boolean recordRemoved = this.removeRemoteProcess(channelToRemove);
        try {
            channelToRemove.close();
        } catch (IOException ignored) {
        }
        ;
        return recordRemoved;
    }

    /**
     * Removes a failed server from the network based on its process ID.
     * <p>
     * This method checks the peer channels for a connection associated with the given failed process ID.
     * If it finds a channel in which the associated {@code NIOMessageChannel} has a matching server PID,
     * it removes the connection and any {@code AddrServerRecord} in the registry by calling the local
     * {@link #removeProcessCloseConnection(SocketChannel)} method.
     * </p>
     * <p>
     * If no channel with a matching server PID is found, the method falls back to removing any
     * AddrServerRecord with the same PID directly from the local registry.
     * </p>
     *
     * @param failedPID the process ID of the failed server to remove
     * @return true if a record or channel corresponding to the PID was actually found and removed; false otherwise.
     */
    public boolean removeFailedAddrServer(Long failedPID) {
        debug(DEBUG_NORMAL, "Attempting to remove failed AddrServer: PID " + failedPID);
        // NIOChannel objects should always have an instance variable set that references the PID of the remote process.
        // We iterate through all the channels(keys) and respective NIOMessageChannels(values) until we find a match.
        for (Map.Entry<SocketChannel, NIOMessageChannel> entry : peerChannels.entrySet()) {
            if (entry.getValue().getServerPID().equals(failedPID)) {
                debug(DEBUG_DETAILED, "Found active peer channel for AddrServer PID " + failedPID + ". Closing connection.");
                return removeProcessCloseConnection(entry.getKey());
            }
        }

        debug(DEBUG_NORMAL, "No active channel found for AddrServer PID " + failedPID + ". Performing registry fallback removal.");
        return this.registry.removeRecordByKey(failedPID);
    }

    /**
     * Synchronizes the Peer (Replica Addressing Server) registry with the current
     * active network connections to identify and purge stale records.
     * <p>
     * During a leadership transition, the new Primary must reconcile its inherited
     * registry against its actual {@code peerChannels}. If a Replica crashed during
     * the failover, it can remain in the registry, but lack an active connection to the new PRIMARY.
     * </p>
     * <p>The audit is executed in three stages:</p>
     * <ul>
     * <li><b>Stage 1:</b> Maps all active {@code NIOMessageChannel} instances to a set of PIDs.</li>
     * <li><b>Stage 2:</b> Identifies PIDs present in the {@code AddrServerRegistry} that
     * do not have a corresponding active connection.</li>
     * <li><b>Stage 3:</b> For each "ghost" PID, it triggers a system-wide failure broadcast
     * and removes the record from the local registry to ensure cluster consistency.</li>
     * </ul>
     * @param myPid          The PID of this process to ensure self-exclusion from the purge.
     * @param cleanupManager The manager used to broadcast failure messages.
     */
    public void auditRegistryConnections(Long myPid, ConnectionCleanupManager cleanupManager) {
        // Stage 1: Create a set containing all REPLICA PIDs who have active connections to the PRIMARY.
        Set<Long> connectedPids = peerChannels.values().stream()
                .map(NIOMessageChannel::getServerPID)
                .collect(Collectors.toSet());

        // Add my (the PRIMARY addressing server) PID to ensure my record is not tagged for removal.
        connectedPids.add(myPid);

        // Stage 2: Identify any "Ghosts" (REPLICA processes who are in the registry but NOT in active connections)
        Set<Long> failedPids = this.registry.getRecords().keySet().stream()
                .filter(pid -> !connectedPids.contains(pid))  // if pid not in connectedPids, then collect the pid
                .collect(Collectors.toSet());

        if (failedPids.isEmpty()) {
            debug(DEBUG_DETAILED, "AddrServer Registry audit complete: No stale records found.");
            return;
        }

        debug(DEBUG_BASIC, "[PeerManager] An audit of the AddrServerRegistry detected ghost records. Triggering failure broadcast.");

        // Stage 3: Remove all failed REPLICA processes from the registry and broadcast their failure
        for (Long failedPid : failedPids) {
            debug(DEBUG_BASIC, "Handling ghost record for PID: " + failedPid + ".");
            ServerFailureMessage<Long> msg = ServerFailureMessage.addrServerFailed(myPid, Roles.PRIMARY, Roles.REPLICA, failedPid);
            cleanupManager.broadcastFailureToReplicas(msg, myPid, failedPid, Roles.REPLICA);
            this.registry.removeRecordByKey(failedPid);
        }
    }

    /**
     * Registers a replica AddressingServer and sets up a persistent connection to it.
     * <p>
     * This method also updates the replica’s {@code AddrServerRecord} with its resolved host address and PID,
     * stores it in the shared registry, and sends a confirmation {@code AckMessage} followed by
     * the current state of all known AddrServer records.
     * </p>
     *
     * @param socketChannel the socket channel for the replica connection.
     * @param nioChannel    the messaging channel used to communicate with the replica.
     * @param record        a partially populated record to complete and store.
     * @throws IllegalArgumentException if there is a mismatch between the PID stored in the NIOMessageChannel and the AddrServerRecord.
     */
    public void registerPeer(SocketChannel socketChannel, NIOMessageChannel nioChannel,
                             AddrServerRecord record) throws IllegalArgumentException {

        if (!nioChannel.getServerPID().equals(record.getPID())) {
            String err = String.format("Peer PID in nioChannel : %d does not match Peer PID in AddrServerRecord: %d%n",
                    nioChannel.getServerPID(), record.getPID());
            throw new IllegalArgumentException(err);
        }
        // This already occurs in AddrServerNetworkManager - channels are stored before dispatching of any kind
        //peerChannels.put(socketChannel, nioChannel);
        // Update network topology storing the AddrServerRecord, thus updating the local state of the Primary
        registry.putAddrServerRecord(record.getPID(), record);
        System.out.println("New replica successfully registered within the network.");
    }

    /**
     * Registers a replica AddressingServer and sets up a persistent connection to it.
     *
     * @param socketChannel the socket channel for the replica connection.
     * @param nioChannel    the messaging channel used to communicate with the replica.
     * @param peerPID       the process ID assigned to the replica.
     * @param primaryPID    the process ID of the primary server.
     * @param record        a fully populated (PID and Host Address set) {@link AddrServerRecord}
     * @throws IOException if an error occurs during network communication.
     */
    public void registerPeerSendACK(SocketChannel socketChannel, NIOMessageChannel nioChannel,
                                    Long primaryPID, Long peerPID, AddrServerRecord record) throws IOException {
        nioChannel.setServerPID(peerPID);
        peerChannels.put(socketChannel, nioChannel);
        System.out.println("PRIMARY AddrServer has registered a new REPLICA process with network PID: " + peerPID);
        System.out.println("NIOChannel PID = " + nioChannel.getServerPID());
        System.out.println("Socket Channel ID = " + socketChannel.toString());

        registry.putAddrServerRecord(peerPID, record);

        // Send an ACK to notify the server it has been registered.
        nioChannel.sendMessage(AckMessage.replicaRegistered(primaryPID, peerPID).toJson());
    }

    /**
     * Generic helper method for broadcasting {@code UpdateMessage<T>} to all connected peer addressing servers.
     * <p>
     * This method handles JSON serialization and transmission errors consistently,
     * logging any failures without interrupting the loop.
     * </p>
     *
     * @param message the update message to be broadcast.
     * @param <T>     the type of record being broadcast (e.g., {@code AddrServerRecord}, {@code ChatServerRecord}).
     */
    private <T> void broadcastLeadershipStatus(UpdateMessage<T> message) {
        try {
            String jsonMessage = message.toJson();
            for (NIOMessageChannel nioChannel : peerChannels.values()) {
                try {
                    nioChannel.sendMessage(jsonMessage);
                } catch (IOException ioe) {
                    System.err.println("Failed to send UpdateMessage<" + message.getObjectType() + ">: " + ioe.getMessage());
                    removeProcessCloseConnection(nioChannel.getSocketChannel());
                }
            }
        } catch (JsonProcessingException e) {
            System.err.println("Failed to serialize UpdateMessage<" + message.getObjectType() + ">: " + e.getMessage());
        }
    }


    /**
     * Broadcasts a message to all peer replicas in the {@code peerChannels}.
     * <p>
     * This method is used to send one-off messages (e.g. heartbeats)
     * to all replicas, using their open persistent connections.
     * </p>
     *
     * @param message the {@code BaseAddrServerMessage} to be serialized and sent.
     */
    public void broadcast(BaseAddrServerMessage<?> message) {
        if (peerChannels.size() != (registry.getRecords().size() - 1)) {
            System.err.println("NETWORK ERROR - More address server records exist than persistent connections.\n" +
                    "Refactoring necessary.");
        }
        String json;
        try {
            json = message.toJson();
        } catch (JsonProcessingException e) {
            System.err.println("Failed to serialize message: " + e.getMessage());
            return;
        }
        for (NIOMessageChannel nioChannel : peerChannels.values()) {
            try {
                nioChannel.sendMessage(json);
            } catch (IOException e) {
                System.err.println("Failed to send to process with PID " + nioChannel.getServerPID() + ": " + e.getMessage());
                removeProcessCloseConnection(nioChannel.getSocketChannel());
            }
        }
    }



    /**
     * Core logic to establish a connection to the primary AddressingServer and transmit an initial message.
     *
     * @param host    the IP address of the PRIMARY AddressingServer.
     * @param port    the port used by the PRIMARY for registration/synchronization.
     * @param message the initial message (Registration or Synchronization) to be sent.
     * @return An {@link Optional} containing the connected {@code SocketChannel}, or empty if the connection failed.
     */
    private Optional<SocketChannel> transmitDiscoveryMessage(String host, int port, BaseAddrServerMessage<?> message) {
        try {
            SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(true);
            channel.connect(new InetSocketAddress(host, port));
            while (!channel.finishConnect()) {
                Thread.sleep(100);
            }

            NIOMessageChannel nioChannel = new NIOMessageChannel(channel);
            peerChannels.put(channel, nioChannel);

            nioChannel.sendMessage(message.toJson());

            channel.configureBlocking(false);
            return Optional.of(channel);
        } catch (IOException | InterruptedException e) {
            System.err.println("Failed to register this replica with primary: " + e.getMessage());
            e.printStackTrace();
            return Optional.empty();
        }
    }


    /**
     * Triggers a registration handshake.
     * This is the entry point for brand new Replicas.
     */
    public boolean registerWithPrimary() {
        AddrServerConfig config = server.getConfig();
        RegisterMessage<AddrServerRecord> register =
                RegisterMessage.fromReplica(server.getMessageIDGenerator().nextID(),
                        NetworkUtils.getSerializedIdentity(Roles.REPLICA), config.getClientPort(),
                        config.getReplicaPort(), config.getChatServerPort());

        System.out.println("Registration handshake message prepared for new process.");
        return initiatePrimaryHandshake(register, null);
    }

    /**
     * Triggers a synchronization handshake.
     * This is the entry point for REPLICA's during leader failover.
     */
    public boolean synchronizeWithPrimary(AddrServerRecord record) {
        AddrServerRecord myRecord = server.getAddrServerRegistry().getRecords().get(server.getConfig().getPID());
        if (myRecord == null) {
            System.err.println("[HANDSHAKE ERROR] Cannot sync: Local record for this process not found.");
            return false;
        }
        SyncRegisterMessage<AddrServerRecord> syncMsg =
                SyncRegisterMessage.fromReplica(server.getMessageIDGenerator().nextID(), myRecord);
        System.out.println("Synchronization handshake message prepared for PID: " + myRecord.getPID());
        return initiatePrimaryHandshake(syncMsg, record);
    }

    /**
     * Unifies the connection and handshake logic for both new registrations and
     * post-election state synchronization.
     * <p>
     * This method determines the appropriate message (Register vs. Sync), transmits it
     * via {@link #transmitDiscoveryMessage(String, int, BaseAddrServerMessage)}, and then ensures the resulting channel
     * is registered with the {@code Selector} for ongoing communication.
     * </p>
     *
     * @param handshakeMsg The specific message (Register or Sync) to send.
     * @return true if the link is established and registered with the Selector.
     * @see AddrServerNetworkManager#openPersistentChannel(SocketChannel)
     * @see RegisterMessage#fromReplica(long, String, int, int, int)
     */
    public boolean initiatePrimaryHandshake(BaseAddrServerMessage<AddrServerRecord> handshakeMsg,
                                            AddrServerRecord knownHost) {

        // Stage 1: Open the pipe, send the JSON, and add to peerChannels map
        int attempts = 0;
        int maxAttempts = 5;
        Optional<SocketChannel> maybeChannel = Optional.empty();
        // Retry Connection Loop
        while (maybeChannel.isEmpty() && attempts < maxAttempts) {
            String hostAddress = null;
            int port = -1;
            // Update the global config from the shared filesystem.
            // Ensures every connection attempt uses the most recently published network details of the PRIMARY.
            if (knownHost != null) {
                hostAddress = knownHost.getHostAddress();
                port = knownHost.getPeerPort();
            } else if (server.getConfig().refreshPrimaryDetails()) {
                hostAddress = server.getConfig().getPrimaryHostAddress();
                port = server.getConfig().getPrimaryReplicaPort();

            } else {
                System.err.println("[HANDSHAKE] Primary discovery details not available.");
                continue;
            }
            // Attempt the connection/handshake
            try {
                maybeChannel = transmitDiscoveryMessage(hostAddress, port, handshakeMsg);
            } catch (UnresolvedAddressException e) {
                System.err.printf("[DNS ERROR] Hostname '%s' could not be resolved. Docker networking may still be initializing.%n", hostAddress);
                maybeChannel = Optional.empty();
            } catch (Exception e) {
                System.err.println("[CRITICAL] Unexpected error during handshake: " + e.getMessage());
                maybeChannel = Optional.empty();
            }


            if (maybeChannel.isEmpty()) {
                attempts++;
                if (attempts < maxAttempts) {
                    long sleepTime = (long) Math.pow(2, attempts - 1) * 1000;
                    System.err.printf("[HANDSHAKE] Connection failed. Retrying in %d seconds (Attempt %d/%d)...%n",
                            sleepTime / 1000, attempts, maxAttempts);
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            }
        }

        // Stage 3: Register with the Selector to allow the AddrServerNetworkManager to receive messages from the PRIMARY.
        if (maybeChannel.isPresent()) {
            SocketChannel channel = maybeChannel.get();
            try {
                this.server.getNetworkManager().openPersistentChannel(channel);
            } catch (IOException e) {
                System.err.println("[HANDSHAKE ERROR] Failed to register channel with Selector: " + e.getMessage());
                // Cleanup the map entry if the Selector registration fails
                peerChannels.remove(channel);
                try {
                    channel.close();
                } catch (IOException ignored) {
                }
                return false;
            }
            return true;
        }
        return false;
    }


//    /**
//     * Initializes a connection to the primary AddressingServer and sends a registration request.
//     * <p>
//     * This is invoked by REPLICA processes on startup to formally register themselves with the PRIMARY.
//     * Once connected, the replica will receive back its PID and the full registry of AddrServer records.
//     * </p>
//     *
//     * @param primaryHostAddress the IP address of the PRIMARY AddressingServer.
//     * @param primaryReplicaPort the port used by the PRIMARY for peer registration.
//     * @param clientPort         the replica’s client port.
//     * @param peerPort           the replica’s peer communication port.
//     * @param chatServerPort     the replica’s chat server communication port.
//     * @return The SocketChannel used to register with the PRIMARY {@code AddressingServer}. This channel must
//     * be registered with the {@code Selector} in the {@code AddrServerNetworkManager} for this REPLICA server.
//     */
//    public Optional<SocketChannel> registerWithPrimary(String primaryHostAddress, int primaryReplicaPort,
//                                                       int clientPort, int peerPort, int chatServerPort) {
//        try {
//            SocketChannel channel = SocketChannel.open();
//            channel.configureBlocking(true);
//            channel.connect(new InetSocketAddress(primaryHostAddress, primaryReplicaPort));
//            while (!channel.finishConnect()) {
//                Thread.sleep(100);
//            }
//
//            NIOMessageChannel nioChannel = new NIOMessageChannel(channel);
//            peerChannels.put(channel, nioChannel);
//            String publicAddress = getThisDockerAddress();
//            RegisterMessage<AddrServerRecord> register =
//                    RegisterMessage.fromReplica(publicAddress, clientPort, peerPort, chatServerPort);
//            nioChannel.sendMessage(register.toJson());
//
//            channel.configureBlocking(false);
//
//            System.out.println("Registration from REPLICA sent to PRIMARY.");
//            return Optional.of(channel);
//        } catch (IOException | InterruptedException e) {
//            System.err.println("Failed to register replica with primary: " + e.getMessage());
//            e.printStackTrace();
//            return Optional.empty();
//        }
//    }

    /**
     * This is a hell of an obtuse way of finding out an addressing servers role, but if you need it,
     * here you go.
     *
     * @param pid The process id of the addressing server you want to know the role of.
     * @return An {@code ServerRole} String - REPLICA or PRIMARY
     */
    public String getServerRole(Long pid) {
        return this.registry.getRecords().get(pid).getRole().toString();
    }

    /**
     * Returns the network process ID (PID) of the current Primary Addressing Server
     * by looking through all the current AddrServer records in the network.
     *
     * @return A Long integer containing the PID of the Primary Addressing Server or
     * 0L to indicate no primary was found in the records.
     */
    public Long getPrimaryPID() {

        Long primaryPID = 0L;

        for (AddrServerRecord record : registry.getRecords().values()) {
            if (record.getRole().equals(ServerRole.PRIMARY)) {
                primaryPID = record.getPID();
            }
        }
        return primaryPID;
    }

    /**
     * Retrieves the active {@link NIOMessageChannel} for the current Primary {@code AddressingServer}, if one exists.
     * <p>
     * This method searches the internal peer channel map to find the {@code NIOMessageChannel}
     * associated with the server process whose {@code ServerRole} is {@code PRIMARY}.
     * It first determines the primary server's PID using the known {@link AddrServerRecord}s,
     * and then attempts to match it with an established network channel.
     * </p>
     *
     * <p>
     * If the primary server has not yet been registered or the connection is not established,
     * this method returns {@code null}. This allows consumers (e.g. background sync threads)
     * to delay execution until the primary becomes reachable.
     * </p>
     *
     * @return the {@code NIOMessageChannel} tied to the current {@code PRIMARY} {@code AddressingServer},
     * or {@code null} if the primary has not yet been registered or is not connected.
     */
    public NIOMessageChannel getPrimaryNIOChannel() {
        Long primaryPid = this.getPrimaryPID();
        if (primaryPid != null && primaryPid != 0L) {
            for (NIOMessageChannel ch : this.peerChannels.values()) {
                if (ch.getSocketChannel().isOpen() && ch.getServerPID().equals(primaryPid))
                    return ch;
            }
        }
        return null;
    }

    /**
     * Retrieves the {@link SocketChannel} associated with a specific server PID.
     * <p>
     * This method iterates over the internal channel map and returns the first {@code SocketChannel}
     * whose associated {@link NIOMessageChannel} has a matching {@code serverPID}. If no such entry
     * is found, it returns {@code null}.
     * </p>
     *
     * @param pid the process ID of the server to look for.
     * @return the matching {@code SocketChannel}, or {@code null} if no match is found.
     */
    public SocketChannel getSocketChannelByPID(Long pid) {
        if (pid != null) {
            for (Map.Entry<SocketChannel, NIOMessageChannel> entry : peerChannels.entrySet()) {
                if (entry.getValue().getServerPID().equals(pid)) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }


}
