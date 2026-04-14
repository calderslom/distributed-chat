package io.github.cpsc559.team16.addressingserver;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import io.github.cpsc559.team16.common.dto.ChatServerRecord;
import io.github.cpsc559.team16.common.logging.ServerDebugLogger;
import io.github.cpsc559.team16.common.messaging.AckMessage;
import io.github.cpsc559.team16.common.messaging.Roles;
import io.github.cpsc559.team16.common.messaging.ServerFailureMessage;
import io.github.cpsc559.team16.common.messaging.UpdateMessage;
import io.github.cpsc559.team16.common.utilities.NIOMessageChannel;

import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

public class ChatServerManager {


    /**
     * This Hashmap is used by each AddressingServer to keep {@code ChatServerRecord}'s
     * of all chat servers in the network. It is maintained by the ChatServerRegistry class.
     */
    private ChatServerRegistry registry;

    public void debugPrintAllServers() {
        this.registry.debugPrintAllServers();
    }

    private final Map<SocketChannel, NIOMessageChannel> chatServerChannels;


    public ChatServerManager(ChatServerRegistry registry) {
        this.chatServerChannels = new ConcurrentHashMap<>();
        this.registry = registry;
    }

    /**
     * Returns a HashMap of SocketChannel and NIOChannel for all the
     * current {@code ChatServer} connections.
     *
     * @return a map of {@code SocketChannel} to {@code NIOMessageChannel} for chat server tracking.
     */
    public Map<SocketChannel, NIOMessageChannel> getChannels() {
        return this.chatServerChannels;
    }

    /**
     * Removes a ChatServer connection and logs the removal using its known network PID.
     * This is achieved by removing the SocketChannel:NioChannel key-value pair from
     * the HashMap of persistent connections {@code chatServerChannels}, as well as the
     * ChatServerRecord associated with the remote process that was connected to the SocketChannel.
     *
     * @param channel the {@code SocketChannel} representing the peer connection to remove.
     *                <strong>NOTE:</strong> This method does not close the SocketChannel connection. It is up to the calling
     *                code to enact this behaviour.
     * @return true if a record for the remote process existed in the {@link ChatServerRegistry}; false otherwise.
     */
    public boolean removeRemoteProcess(SocketChannel channel) {
        NIOMessageChannel ch = this.chatServerChannels.get(channel);
        if (ch == null) return false;

        Long pidFromChannel = ch.getServerPID();

        // Stage 1: Remove Channel whether the process is registered or not.
        this.chatServerChannels.remove(channel);
        try {
            System.out.printf("Purging ChatServer connection [%s] for PID %d %n", channel.getRemoteAddress(), pidFromChannel);
        } catch (IOException ignore) {}

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
     * @return true if a record for the remote process existed in the {@link ChatServerRegistry}; false otherwise.
     */
    public boolean removeProcessCloseConnection(SocketChannel channelToRemove) {
        boolean recordRemoved = this.removeRemoteProcess(channelToRemove);
        try { channelToRemove.close(); }
        catch(IOException ignored) {};
        return recordRemoved;
    }


    /**
     * Removes a failed chat server's connection and its registry record using only its process ID.
     * <p>
     * This method iterates over the chat server channels to locate a connection whose associated
     * {@code NIOMessageChannel} has a matching server PID. If found, it removes the connection and any
     * {@code ChatServerRecord} in the registry by calling
     * {@code removeProcessCloseConnection} on that channel and then returns. If no channel is found,
     * the method directly removes the record from the local registry.
     * </p>
     *
     * @param failedPID the process ID of the failed chat server to remove
     * @return true if a record for the remote process existed in the {@link ChatServerRegistry}; false otherwise.
     */
    public boolean removeFailedChatServer(Long failedPID) {
        debug(DEBUG_NORMAL, "Attempting to remove failed ChatServer: PID " + failedPID);
        // NIOChannel objects should always have an instance variable set that references the PID of the remote process.
        // We iterate through all the channels(keys) and respective NIOMessageChannels(values) until we find a match.
        for (Map.Entry<SocketChannel, NIOMessageChannel> entry : chatServerChannels.entrySet()) {
            if (entry.getValue().getServerPID().equals(failedPID)) {
                debug(DEBUG_DETAILED, "Found active channel for ChatServer PID " + failedPID + ". Closing connection.");
                return removeProcessCloseConnection(entry.getKey()); // Successfully found and cleaned up via channel
            }
        }
        debug(DEBUG_NORMAL, "No active channel found for ChatServer PID " + failedPID + ". Attempting direct registry removal.");
        return this.registry.removeRecordByKey(failedPID); // Channel did not exist, try and remove record anyways.
    }

    /**
     * Synchronizes the ChatServer registry with the current network state by identifying
     * and removing "ghost" records.
     * <p>
     * A ghost record occurs when a ChatServer crashes simultaneously with the Primary
     * Addressing Server. Since the record exists in the replicated registry but the
     * physical TCP connection is gone, this method performs a proactive reconciliation.
     * </p>
     * * <p>The audit follows a three-stage process:</p>
     * <ul>
     * <li><b>Stage 1:</b> Collects PIDs from all active {@code NIOMessageChannel} connections.</li>
     * <li><b>Stage 2:</b> Filters the registry to identify PIDs that exist in the records
     * but lack an active connection.</li>
     * <li><b>Stage 3:</b> Triggers a formal system-wide failure broadcast for each ghost
     * and purges the stale record from the local registry.</li>
     * </ul>
     *
     * @param myPid      The PID of the current server (the new Primary) performing the audit.
     * @param cleanupManager The manager used to broadcast failure messages to the rest of the network.
     */
    public void auditRegistryConnections(Long myPid, ConnectionCleanupManager cleanupManager) {
        // Stage 1: Create a set containing the PIDs of all ChatServers who have active connections to the PRIMARY.
        Set<Long> connectedPids = chatServerChannels.values().stream()
                .map(NIOMessageChannel::getServerPID)
                .collect(Collectors.toSet());

        // Add my (the PRIMARY addressing server) PID to ensure my record is not tagged for removal.
        connectedPids.add(myPid);

        // Stage 2: Identify any "Ghosts" (ChatServer processes who are in the registry but NOT in active connections)
        Set<Long> failedPids = this.registry.getRecords().keySet().stream()
                .filter(pid -> !connectedPids.contains(pid))  // if pid not in connectedPids, then collect the pid
                .collect(Collectors.toSet());

        if (failedPids.isEmpty()) {
            debug(DEBUG_DETAILED, "ChatServer Registry audit complete: No stale records found.");
            return;
        }

        debug(DEBUG_BASIC, "[ChatServerManager] An audit of the ChatServerRegistry detected ghost records. Triggering failure broadcast.");

        // Stage 3: Remove all failed ChatServer processes from the registry and broadcast their failure
        for (Long failedPid : failedPids) {
            debug(DEBUG_BASIC, "Handling ghost record for PID: " + failedPid + ".");
            ServerFailureMessage<Long> msg = ServerFailureMessage.chatServerFailed(myPid, Roles.PRIMARY, Roles.CHATSERVER, failedPid);
            cleanupManager.broadcastFailureToReplicas(msg, myPid, failedPid, Roles.CHATSERVER);
            this.registry.removeRecordByKey(failedPid);
        }
    }

    /**
     * Updates or inserts a record into the shared ChatServerRegistry registry.
     * <p>
     * This method is typically called when receiving an {@code UpdateMessage}
     * containing new or modified ChatServer information.
     * </p>
     *
     * @param record the record to insert or update.
     */
    public void updateRecords(ChatServerRecord record) {
        registry.updateOrInsertRecord(record);
    }


    /**
     * Registers a new {@code ChatServer} and sets up a persistent connection to it.
     * <p>
     * This method also updates the replica’s {@link ChatServerRecord} with its resolved host address and PID,
     * stores it in the shared registry, and sends a confirmation {@link AckMessage} followed by
     * the current state of all known AddrServer records.
     * </p>
     *
     * @param socketChannel the socket channel for the ChatServer connection.
     * @param nioChannel    the messaging channel used to communicate with the rChatServer.
     * @param record        a partially populated record to complete and store.
     * @throws IllegalArgumentException  if there is a mismatch between the PID stored in the NIOMessageChannel and the ChatServerRecord.
     */
    public void registerServer(SocketChannel socketChannel, NIOMessageChannel nioChannel,
                             ChatServerRecord record) throws IllegalArgumentException {

        if (!nioChannel.getServerPID().equals(record.getPID())) {
            String err = String.format("Peer PID in nioChannel : %d does not match Peer PID in AddrServerRecord: %d%n",
                    nioChannel.getServerPID(), record.getPID());
            throw new IllegalArgumentException(err);
        }
        // Store the chat server channel for future use.
        chatServerChannels.put(socketChannel, nioChannel);
        // Update network topology storing the ChatServerRecord, thus updating the local state of the Primary
        registry.putChatServerRecord(record.getPID(), record);
    }


    /**
     * Registers a ChatServer and sets up a persistent connection to it.
     * <p>
     *     This is typically used to register a chat server <strong>when no REPLICA addressing server exists.</strong>
     * </p>
     *
     * @param socketChannel the socket channel for the ChatServer connection.
     * @param nioChannel    the messaging channel used to communicate with the ChatServer.
     * @param peerPID       the process ID assigned to the ChatServer.
     * @param primaryPID    the process ID of the primary server.
     * @param record        a fully populated (PID and Host Address set) {@link ChatServerRecord}
     * @throws IOException if an error occurs during network communication.
     */
    public void registerServerSendACK(SocketChannel socketChannel, NIOMessageChannel nioChannel,
                                    Long primaryPID, Long peerPID, ChatServerRecord record) throws IOException {
        nioChannel.setServerPID(peerPID);
        // This occurs in AddrServerNetworkManager already
        //chatServerChannels.put(socketChannel, nioChannel);
        System.out.println("PRIMARY AddrServer has registered a new ChatServer process with network PID: " + peerPID);
        System.out.println("NIOChannel PID = " + nioChannel.getServerPID());
        System.out.println("Socket Channel ID = " + socketChannel.toString());
        record.setPID(peerPID);
        registry.putChatServerRecord(peerPID, record);
        // Send an ACK to notify the server it has been registered.
        nioChannel.sendMessage(AckMessage.chatServerRegistered(primaryPID, peerPID).toJson());
        System.out.println("New Chat Server successfully registered within the network.");
    }

    /**
     * Returns a concurrent map of all connected ChatServer channels that have been assigned a non-zero PID.
     * <p>
     * The map is keyed by the chat server's PID, with values being their corresponding {@link NIOMessageChannel}.
     * Unregistered channels (PID == 0L) are excluded.
     * </p>
     *
     * @return a {@link ConcurrentHashMap} of chat server PIDs to active {@link NIOMessageChannel}s.
     */
    public ConcurrentHashMap<Long, NIOMessageChannel> getRegisteredServerChannelMap() {
        ConcurrentHashMap<Long, NIOMessageChannel> registered = new ConcurrentHashMap<>();
        for (NIOMessageChannel ch : chatServerChannels.values()) {
            Long pid = ch.getServerPID();
            if (pid != 0L) {
                registered.put(pid, ch);
            }
        }
        return registered;
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
    public SocketChannel getChannelByPID(Long pid) {
        if (pid != null) {
            for (Map.Entry<SocketChannel, NIOMessageChannel> entry : chatServerChannels.entrySet()) {
                if (entry.getValue().getServerPID().equals(pid)) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }


    public boolean hasActiveConnection(Long pid) {
        return this.getChannelByPID(pid) != null;
    }
}
