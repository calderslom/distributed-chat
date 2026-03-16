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
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.github.cpsc559.team16.common.dto.AddrServerRecord;
import io.github.cpsc559.team16.common.dto.ServerRole;
import io.github.cpsc559.team16.common.messaging.*;
import io.github.cpsc559.team16.common.utilities.NIOMessageChannel;
import io.github.cpsc559.team16.common.utilities.NetworkUtils;

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
        // NIOChannel objects should always have an instance variable set that references the PID of the remote process.
        // We iterate through all the channels(keys) and respective NIOMessageChannels(values) until we find a match.
        for (Map.Entry<SocketChannel, NIOMessageChannel> entry : peerChannels.entrySet()) {
            if (entry.getValue().getServerPID().equals(failedPID)) {
                return removeProcessCloseConnection(entry.getKey());
            }
        }
        return this.registry.removeRecordByKey(failedPID);
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


    private String getThisDockerAddress() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            System.out.println("[Replica] Could not retrieve internal Docker address - defaulting to 'localhost'");
            return "localhost";
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
     * Triggers a state synchronization handshake between this REPLICA and the current PRIMARY.
     * <p>
     * This method serves as the critical entry point for a REPLICA following a leader election
     * or failover event. It ensures that the local node re-establishes its identity within
     * the new PRIMARY's registry and synchronizes any divergent state.
     * </p>
     * * <p><b>Process Flow:</b></p>
     * <ol>
     * <li>Retrieves the local {@link AddrServerRecord} using the process PID.</li>
     * <li>Constructs a {@link SyncRegisterMessage} containing the local record and a unique message ID.</li>
     * <li>Delegates the network transmission and Selector registration to {@link #initiatePrimaryHandshake}.</li>
     * </ol>
     *
     * @param record The {@link AddrServerRecord} of the target PRIMARY. If null, the method
     * will attempt to discover the PRIMARY via the shared filesystem.
     * @return {@code true} if the handshake message was successfully sent and the
     * resulting channel is registered with the Selector; {@code false} otherwise.
     * @see SyncRegisterMessage
     * @see #initiatePrimaryHandshake(BaseAddrServerMessage, AddrServerRecord)
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

    /**
     * This is a hell of an obtuse way of finding out an addressing servers role, but if you need it,
     * here you go.
     *
     * @param pid The process id of the addressing server you want to know the role of.
     * @return A {@code ServerRole} String - REPLICA or PRIMARY
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
        if (primaryPid != 0L) {
            for (NIOMessageChannel ch : this.peerChannels.values()) {
                if (ch.getServerPID().equals(primaryPid) && ch.getSocketChannel().isOpen())
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


//    /**
//     * Updates the provided {@link AddrServerRecord} with runtime information from the given socket connection and PID.
//     * <p>
//     * This method is typically called during replica registration to ensure that the record accurately reflects
//     * the replica's actual host address and assigned PID. The host address is extracted directly from the
//     * {@link SocketChannel}'s remote address to avoid relying on potentially incorrect values sent by the remote process.
//     * </p>
//     *
//     * @param socketChannel the channel representing the remote replica's connection
//     * @param record        the {@link AddrServerRecord} instance provided by the replica
//     * @param peerPID       the process ID assigned to the replica by the primary
//     * @return the updated {@link AddrServerRecord} with corrected host address and assigned PID
//     * @throws IOException if the remote address cannot be resolved from the socket
//     */
//    public static AddrServerRecord updateServerRecord(SocketChannel socketChannel,
//                                               AddrServerRecord record, Long peerPID) throws IOException {
//        // Retrieve the remote process Host Address.
//        InetSocketAddress remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
//        String replicaHostAddr = remoteAddress.getAddress().getHostAddress();
//        // Update the incoming AddrServerRecord provided by the remote process.
//        record.setHostAddress(replicaHostAddr);
//        record.setPID(peerPID);
//        return record;
//    }
//    /**
//     * Sends all currently known {@code AddrServerRecord} entries from the primary
//     * to a newly connected replica.
//     * <p>
//     * This ensures that the new replica is fully synchronized with the current
//     * network topology known to the primary.
//     * </p>
//     *
//     * @param primaryPID the PID of the primary server sending the updates.
//     * @param nioChannel the channel over which to send the records.
//     */
//    public void sendAllAddrServerRecords(Long primaryPID, NIOMessageChannel nioChannel) throws IOException {
//        for (AddrServerRecord record : this.registry.getRecords().values()) {
//            UpdateMessage<AddrServerRecord> message = UpdateMessage.asRecordPrimaryToReplica(primaryPID, record);
//            try {
//                nioChannel.sendMessage(message.toJson());
//            } catch (JsonProcessingException e) {
//                System.err.println("Failed to serialize UpdateMessage<AddrServerRecord>: " + e.getMessage());
//            } catch (IOException ioe) {
//                System.err.println("Failed to send UpdateMessage<AddrServerRecord>: " + ioe.getMessage());
//                throw ioe;
//            }
//        }
//        System.out.println("Done sending all AddrServerRecords to newly registered REPLICA.");
//    }

//    /**
//     * Sends all currently known {@code ChatServerRecord} entries in the network.
//     * <p>
//     * This is typicall used to ensure that a new replica is fully synchronized with all of the
//     * active ChatServer's known to the primary.
//     * </p>
//     *
//     *
//     * @param primaryPID  the PID of the primary server sending the updates.
//     * @param nioChannel  the channel over which to send the records.
//     * @param chatRecords a {@code HashMap} containing all {@code ChatServerRecord} entries.
//     */
//    public void sendAllChatServerRecords(Long primaryPID, NIOMessageChannel nioChannel,
//                                         Map<Long, ChatServerRecord> chatRecords) throws IOException {
//        for (ChatServerRecord record : chatRecords.values()) {
//            UpdateMessage<ChatServerRecord> message = UpdateMessage.csRecordPrimaryToReplica(primaryPID, record);
//            try {
//                nioChannel.sendMessage(message.toJson());
//            } catch (JsonProcessingException e) {
//                System.err.println("Failed to serialize UpdateMessage<ChatServerRecord>: " + e.getMessage());
//            } catch (IOException ioe) {
//                System.err.println("Failed to send UpdateMessage<ChatServerRecord>: " + ioe.getMessage());
//                throw ioe;
//            }
//        }
//        System.out.println("Done sending all ChatServerRecords to newly registered REPLICA.");
//    }
//    /**
//     * Sends a single {@link AddrServerRecord} to all the process tied to the NIOChannel.
//     * <p>
//     * This method is used by the PRIMARY {@code AddressingServer} to notify all registered
//     * REPLICA servers about a new or updated {@code AddrServerRecord} - a necessary part of
//     * maintaining network consistency.
//     * </p>
//     *
//     * @param primaryPID the PID of the primary server issuing the update.
//     * @param record     the {@link AddrServerRecord} to broadcast.
//     */
//    public void sendAddrServerRecord(long messageID, Long primaryPID, AddrServerRecord record, NIOMessageChannel nioChannel) throws IOException {
//        UpdateMessage<AddrServerRecord> message = UpdateMessage.asRecordPrimaryToReplica(messageID, primaryPID, record);
//        try {
//            nioChannel.sendMessage(message.toJson());
//        }
//        catch (JsonProcessingException e) {
//            System.err.printf(
//                    "Failed to serialize UpdateMessage<%s>. Context: messageID=%d, senderPID=%d, senderRole=%s, receiverPID=%d. Exception: %s%n",
//                    message.getObjectType(), messageID, primaryPID, Roles.PRIMARY, nioChannel.getServerPID(), e.getMessage()
//            );
//        }
//        catch (IOException ioe) {
//            System.err.println("Failed to send UpdateMessage<AddrServerRecord> for message ID: " + message.getMessageID());
//            throw ioe;
//        }
//    }
//
//    /**
//     * Broadcasts a single {@link AddrServerRecord} to all connected peer replicas.
//     * <p>
//     * This method is used by the PRIMARY {@code AddressingServer} to notify all registered
//     * REPLICA servers about a new or updated {@code AddrServerRecord} - a necessary part of
//     * maintaining network consistency.
//     * </p>
//     *
//     * @param primaryPID the PID of the primary server issuing the update.
//     * @param record     the {@link AddrServerRecord} to broadcast.
//     */
//    public void broadcastAddrServerRecord(Long primaryPID, AddrServerRecord record) {
//        UpdateMessage<AddrServerRecord> message = UpdateMessage.asRecordPrimaryToReplica(primaryPID, record);
//        broadcastServerRecord(message);
//    }
//
//    /**
//     * Broadcasts a single {@link ChatServerRecord} to all connected peer replicas.
//     * <p>
//     * This method is used by the PRIMARY {@code AddressingServer} to notify all registered
//     * REPLICA servers about a new or updated {@code ChatServerRecord} - a necessary part of
//     * maintaining network consistency.
//     * </p>
//     *
//     * @param primaryPID the PID of the primary server issuing the update.
//     * @param record     the {@link ChatServerRecord} to broadcast.
//     *
//     */
//    public void broadcastChatServerRecord(Long primaryPID, ChatServerRecord record) {
//        UpdateMessage<ChatServerRecord> message = UpdateMessage.csRecordPrimaryToReplica(primaryPID, record);
//        broadcastServerRecord(message);
//    }
//
//    /**
//     * Generic helper method for broadcasting {@code UpdateMessage<T>} to all connected peer addressing servers.
//     * <p>
//     * This method handles JSON serialization and transmission errors consistently,
//     * logging any failures without interrupting the loop.
//     * </p>
//     *
//     * @param message the update message to be broadcast.
//     * @param <T>     the type of record being broadcast (e.g., {@code AddrServerRecord}, {@code ChatServerRecord}).
//     */
//    private <T> void broadcastServerRecord(UpdateMessage<T> message) {
//        try {
//            String jsonMessage = message.toJson();
//            for (NIOMessageChannel nioChannel : peerChannels.values()) {
//                try {
//                    nioChannel.sendMessage(jsonMessage);
//                } catch (IOException ioe) {
//                    System.err.println("Failed to send UpdateMessage<" + message.getObjectType() + ">: " + ioe.getMessage());
//                    removeProcessCloseConnection(nioChannel.getSocketChannel());
//                }
//            }
//        } catch (JsonProcessingException e) {
//            System.err.println("Failed to serialize UpdateMessage<" + message.getObjectType() + ">: " + e.getMessage());
//            return;
//        }
//    }


}
