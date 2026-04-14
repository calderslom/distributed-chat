package io.github.cpsc559.team16.addressingserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.github.cpsc559.team16.common.dto.AddrServerRecord;
import io.github.cpsc559.team16.common.dto.ChatServerRecord;
import static io.github.cpsc559.team16.common.logging.DebugLogger.*;
import io.github.cpsc559.team16.common.messaging.*;
import io.github.cpsc559.team16.common.utilities.NIOMessageChannel;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.github.cpsc559.team16.common.logging.DebugLogger.debug;

public class BroadcastManager {

    ConnectionCleanupManager cleanupManager;

    private final Map<SocketChannel, NIOMessageChannel> chatServerChannels;

    /**
     * A thread-safe mapping of persistent peer connections.
     * Each peer (replica AddressingServer) is tracked by its associated {@code SocketChannel}
     * and wrapped in an {@code NIOMessageChannel} for structured messaging.
     */
    private final Map<SocketChannel, NIOMessageChannel> peerChannels;

    private final MessageIDGenerator genMID;

    public BroadcastManager(MessageIDGenerator genMID, Map<SocketChannel, NIOMessageChannel> peerChannels,
                            Map<SocketChannel, NIOMessageChannel> chatServerChannels, ConnectionCleanupManager cleanupManager) {
        this.genMID = genMID;
        this.peerChannels = peerChannels;
        this.chatServerChannels = chatServerChannels;
        this.cleanupManager = cleanupManager;
    }


    /**
     * Broadcasts a single {@link ChatServerRecord} to all connected servers.
     * <p>
     * This method is used by the PRIMARY {@code AddressingServer} to inform ChatServers and Replica AddrServers
     * about a new or updated {@code ChatServerRecord}. This ensures the distributed state
     * remains consistent across all registered nodes.
     * </p>
     *
     * @param senderPID the PID of the primary server issuing the update.
     * @param record        the {@link AddrServerRecord} containing the update information.
     */
    public void broadcastChatServerRecord(Long senderPID, ChatServerRecord record) {
        UpdateMessage<ChatServerRecord> forChatServer = UpdateMessage.csRecordPrimaryToCS(senderPID, record);
        broadcastServerRecordNoEvent(forChatServer, chatServerChannels);
        UpdateMessage<ChatServerRecord> forReplica = UpdateMessage.csRecordPrimaryToReplica(senderPID, record);
        broadcastServerRecordNoEvent(forReplica, peerChannels);
    }

    /**
     * Broadcasts a single {@link AddrServerRecord} to all connected {@code ChatServer}s.
     * <p>
     * This method is used by the PRIMARY {@code AddressingServer} to inform chat servers
     * of changes to the network topology, such as the registration or update of a replica.
     * The update is packaged as an {@code UpdateMessage} with objectType {@code "AddrServerInfo"},
     * and is sent over all currently active {@code NIOMessageChannel}s.
     * </p>
     * <p>Updating processes throughout the distributed network is necessary to maintain consistency.</p>
     *
     * @param senderPID the PID of the primary server issuing the update.
     * @param record        the {@link AddrServerRecord} containing the update information.
     */
    public void broadcastAddrServerRecordToCS(Long senderPID, AddrServerRecord record) {
        UpdateMessage<AddrServerRecord> forChatServer = UpdateMessage.asRecordPrimaryToCS(senderPID, record);
        broadcastServerRecordNoEvent(forChatServer, chatServerChannels);
    }



    /**
     * Broadcasts a single {@link ChatServerRecord} to all connected {@code ChatServer}s.
     * <p>
     * This method is used by the PRIMARY {@code AddressingServer} to inform chat servers
     * about a new or updated {@code ChatServerRecord}. This ensures the distributed state
     * remains consistent across all registered nodes.
     * </p>
     *
     * @param senderPID the PID of the primary server issuing the update.
     * @param record        the {@link AddrServerRecord} containing the update information.
     */
    public void broadcastChatServerRecordToCS(Long senderPID, ChatServerRecord record) {
        UpdateMessage<ChatServerRecord> forChatServer = UpdateMessage.csRecordPrimaryToCS(senderPID, record);
        broadcastServerRecordNoEvent(forChatServer, chatServerChannels);
    }


    /**
     * Generic helper method for broadcasting {@code UpdateMessage<T>} to all connected peer addressing servers.
     * <p>
     * This method handles JSON serialization and transmission errors consistently,
     * logging any failures without interrupting the loop.
     * </p>
     *
     *<strong>NOTE:</strong> This method automatically handles any I/O exceptions and cleans up the connection that
     * triggered it.
     *
     * @param message the update message to be broadcast.
     * @param <T>     the type of record being broadcast (e.g., {@code AddrServerRecord}, {@code ChatServerRecord}).
     */
    public <T> void broadcastServerRecordNoEvent(UpdateMessage<T> message, Map<SocketChannel, NIOMessageChannel> channelHashMap) {
        try {
            String jsonMessage = message.toJson();
            for (NIOMessageChannel nioChannel : channelHashMap.values()) {
                try {
                    // Only new connections have a PID set to zero. ALL registered connections have the PID of the remote process.
                    if (nioChannel.getServerPID() == 0) continue;
                    nioChannel.sendMessage(jsonMessage);
                } catch (IOException ioe) {
                    System.err.println("Failed to send UpdateMessage<" + message.getObjectType() + ">: " + ioe.getMessage());
                    cleanupManager.cleanupPersistentConnectionNIO(nioChannel,true);
                    // TODO - remove this from the list of channels passed in
                }
            }
        } catch (JsonProcessingException e) {
            System.err.println("Failed to serialize UpdateMessage<" + message.getObjectType() + ">: " + e.getMessage());
        }
    }

    /**
     * Sends a broadcast {@link UpdateMessage} to all remaining recipients tracked by the given {@link PendingEvent}.
     * <p>
     * This method serializes the message and attempts delivery to each {@link NIOMessageChannel} still listed
     * in the {@code PendingEvent}'s recipient map. If a channel fails to deliver the message due to an
     * {@link IOException}, it is cleaned up and removed from the pending recipient list.
     * </p>
     *
     * <p>
     * This method ensures consistency between the state of the messaging layer and the {@code PendingEvent},
     * allowing retry logic or completion checks to rely on the recipient map accurately.
     * </p>
     *
     * @param message       the {@link UpdateMessage} to broadcast to registered replicas
     * @param pendingEvent  the {@link PendingEvent} tracking the recipients and acknowledgments for this update
     * @param <T>           the type of object included in the {@code UpdateMessage} payload (e.g., {@code AddrServerRecord})
     */
    public <T> void broadcastServerRecord(UpdateMessage<T> message, PendingEvent pendingEvent)
    {
        try {
            String jsonMessage = message.toJson();
            for (NIOMessageChannel nioChannel : pendingEvent.getPendingRecipients().values()) {
                try {
                    nioChannel.sendMessage(jsonMessage);
                    System.out.println("Sending ChatServerRecord update to replicas: " + message.getMsgType());
                } catch (IOException ioe) {
                    System.err.println("Failed to send UpdateMessage<" + message.getObjectType() + ">: " + ioe.getMessage());
                    cleanupManager.cleanupPersistentConnectionNIO(nioChannel,true);
                    pendingEvent.removeRecipientChannel(nioChannel.getServerPID());
                }
            }
        } catch (JsonProcessingException e) {
            System.err.println("Failed to serialize UpdateMessage<" + message.getObjectType() + ">: " + e.getMessage());
        }
    }

    /**
     * Broadcasts a single {@link AddrServerRecord} update to all registered {@code AddressingServer} replicas,
     * and associates the broadcast with a {@link PendingEvent} for coordination and retry tracking.
     *
     * <p>
     * This method wraps the update in an {@link UpdateMessage}, stores it in the pending event,
     * and delegates actual message transmission to {@link #broadcastServerRecord}.
     * </p>
     *
     * <p>
     * This supports eventual consistency by ensuring all known replicas receive and acknowledge
     * the updated addressing server state. Any replicas that fail to receive the message are removed
     * from the pending set and will not be expected to ACK.
     * </p>
     *
     * @param messageID        the unique message ID used to correlate ACKs and retries
     * @param senderPID       the PID of the primary addressing server broadcasting the update
     * @param record           the {@link AddrServerRecord} containing the update to replicate
     * @param pendingEvent     the {@link PendingEvent} tracking acknowledgments and recipient state
     */
    public void broadcastASRecordToReplicas(long messageID, Long senderPID, AddrServerRecord record,
                                            PendingEvent pendingEvent)
    {
        // Create the message to send to Replicas for synchronization of state across all Addressing Servers.
        UpdateMessage<AddrServerRecord> updateMessage = UpdateMessage.asRecordPrimaryToReplica(messageID, senderPID, record);
        // Add this message to the pending event in case we need to retry sending the message.
        pendingEvent.setMessageRequiringACK(updateMessage);
        // Broadcast the message to all registered replicas - pass the map that is referenced by the pending event
        // so that we can remove any channels that had an I/O failure and had their connections cleaned up (we can't be waiting for ACK's that will never come!)
        broadcastServerRecord(updateMessage, pendingEvent);
    }

    /**
     * Broadcasts a single {@link ChatServerRecord} update to all registered {@code AddressingServer} replicas,
     * and associates the broadcast with a {@link PendingEvent} for coordination and retry tracking.
     *
     * <p>
     * This method wraps the update in an {@link UpdateMessage}, stores it in the pending event,
     * and delegates actual message transmission to {@link #broadcastServerRecord}.
     * </p>
     *
     * <p>
     * This supports eventual consistency by ensuring all known replicas receive and acknowledge
     * the updated addressing server state. Any replicas that fail to receive the message are removed
     * from the pending set and will not be expected to ACK.
     * </p>
     *
     * @param messageID        the unique message ID used to correlate ACKs and retries
     * @param senderPID       the PID of the primary addressing server broadcasting the update
     * @param record           the {@link ChatServerRecord} containing the update to replicate
     * @param pendingEvent     the {@link PendingEvent} tracking acknowledgments and recipient state
     */
    public void broadcastCSRecordToReplicas(long messageID, Long senderPID, ChatServerRecord record,
                                            PendingEvent pendingEvent) {
        // Create the message to send to Replicas for synchronization of state across all Addressing Servers.
        UpdateMessage<ChatServerRecord> updateMessage = UpdateMessage.csRecordPrimaryToReplica(messageID, senderPID, record);
        // Add this message to the pending event in case we need to retry sending the message.
        pendingEvent.setMessageRequiringACK(updateMessage);
        // Broadcast the message to all registered replicas - pass the map that is referenced by the pending event
        // so that we can remove any channels that had an I/O failure and had their connections cleaned up (we can't be waiting for ACK's that will never come!)
        broadcastServerRecord(updateMessage, pendingEvent);
    }



    /**
     * Sends all currently known {@code AddrServerRecord} entries from the primary
     * to a newly connected replica.
     * <p>
     * This ensures that the new replica is fully synchronized with the current
     * network topology known to the primary.
     * </p>
     *
     * <strong>NOTE:</strong> We want this to throw an IOException - we do not want to clean up the channel in this class.
     * This is because we are only sending data to one recipient. No actions involving this recipient should continue, thus
     * an error is thrown and the calling code will be notified and have the option of handling it there, or passing it "up the chain".
     *
     * @param senderPID the PID of the primary server sending the updates.
     * @param nioChannel the channel over which to send the records.
     * @param records the map of PIDs to AddrServerRecords to be synchronized.
     */
    public void sendAllAddrServerRecordsToReplica(Long senderPID, NIOMessageChannel nioChannel,
                                                  Map<Long, AddrServerRecord> records) throws IOException {
        // Safety Guard: Ensure records is not null and not empty
        if (records == null || records.isEmpty()) {
            debug(DEBUG_NORMAL, "No AddrServerRecords found to synchronize with REPLICA PID: " + nioChannel.getServerPID());
            return;
        }

        for (AddrServerRecord record : records.values()) {
            UpdateMessage<AddrServerRecord> message = UpdateMessage.asRecordPrimaryToReplica(senderPID, record);
            try {
                nioChannel.sendMessage(message.toJson());
            } catch (JsonProcessingException e) {
                debug(DEBUG_BASIC, "Failed to serialize UpdateMessage<AddrServerRecord>: " + e.getMessage());
            } catch (IOException ioe) {
                debug(DEBUG_BASIC, "Failed to send UpdateMessage<AddrServerRecord>: " + ioe.getMessage());
                throw ioe;
            }
        }
        debug(DEBUG_NORMAL, "Done sending all " + records.size() + " AddrServerRecords to REPLICA with PID: " + nioChannel.getServerPID());
    }

    /**
     * Sends all currently known {@code ChatServerRecord} entries in the network.
     * <p>
     * This is typicall used to ensure that a new replica is fully synchronized with all of the
     * active ChatServer's known to the primary.
     * </p>
     *
     * <strong>NOTE:</strong> We want this to throw an IOException - we do not want to clean up the channel in this class.
     * This is because we are only sending data to one recipient. No actions involving this recipient should continue, thus
     * an error is thrown and the calling code will be notified and have the option of handling it there, or passing it "up the chain".
     *
     * @param senderPID  the PID of the (primary) server sending the updates.
     * @param nioChannel  the channel over which to send the records.
     * @param records a {@code HashMap} containing all {@code ChatServerRecord} entries.
     */
    public void sendAllChatServerRecordsToReplica(Long senderPID, NIOMessageChannel nioChannel,
                                                  Map<Long, ChatServerRecord> records) throws IOException {
        if (records == null || records.isEmpty()) {
            debug(DEBUG_DETAILED, "No ChatServerRecords found to synchronize with REPLICA PID: " + nioChannel.getServerPID());
            return;
        }
        for (ChatServerRecord record : records.values()) {
            UpdateMessage<ChatServerRecord> message = UpdateMessage.csRecordPrimaryToReplica(senderPID, record);
            try {
                nioChannel.sendMessage(message.toJson());
            } catch (JsonProcessingException e) {
                debug(DEBUG_BASIC, "Failed to serialize UpdateMessage<ChatServerRecord>: " + e.getMessage());
            } catch (IOException ioe) {
                debug(DEBUG_BASIC, "Failed to send UpdateMessage<ChatServerRecord>: " + ioe.getMessage());
                throw ioe;
            }
        }
        debug(DEBUG_DETAILED, "Done sending all " + records.size() + " ChatServerRecords to REPLICA with PID: " + nioChannel.getServerPID());
    }


    /**
     * Sends all known records from the primary server to a newly connected replica.
     * <p>
     * This method consolidates the functionality of sending both Addressing Server records
     * (AddrServerRecord) and Chat Server records (ChatServerRecord) to a single recipient.
     * It sequentially invokes {@code sendAllAddrServerRecords} to send all Addressing Server records,
     * followed by {@code sendAllChatServerRecords} to send all Chat Server records.
     * </p>
     * <p>
     * <strong>NOTE:</strong> If an {@code IOException} occurs during the sending of any record,
     * the exception is propagated to the caller. This ensures that any issues with the network channel
     * are handled by the calling code, as no cleanup or recovery actions are performed within this method.
     * </p>
     *
     * @param senderPID         the PID of the (primary) addressing server sending the records.
     * @param nioChannel         the NIOMessageChannel over which the records are sent.
     * @param chatServerRegistry a {@code Map} containing all active {@code ChatServerRecord} entries,
     *                           keyed by their process IDs.
     * @param addrServerRegistry a {@code Map} containing all active {@code AddrServerRecord} entries,
     *                           keyed by their process IDs.
     * @throws IOException if an error occurs while sending any of the records.
     */
    public void sendAllRecordsToReplica(Long senderPID, NIOMessageChannel nioChannel,
                                        Map<Long, ChatServerRecord> chatServerRegistry,
                                        Map<Long, AddrServerRecord> addrServerRegistry) throws IOException {
        this.sendAllAddrServerRecordsToReplica(senderPID, nioChannel, addrServerRegistry);
        this.sendAllChatServerRecordsToReplica(senderPID, nioChannel, chatServerRegistry);
    }



    /**
     * Sends all currently known {@link AddrServerRecord} entries from the Primary
     * to a newly registered {@code ChatServer}.
     *
     * <p>This ensures that the ChatServer is fully synchronized with the current
     * AddressingServer topology known to the Primary.</p>
     *
     * <p><strong>Note:</strong> If an {@link IOException} occurs while sending,
     * this method delegates channel cleanup to the {@code ConnectionCleanupManager}.
     * It is assumed that an unrecoverable error means the channel should no longer be trusted.
     * </p>
     *
     * @param senderPID the PID of the (Primary) AddressingServer sending the updates.
     * @param nioChannel the {@link NIOMessageChannel} over which records are sent.
     * @param registry a {@code Map} of {@code AddrServerRecord} entries keyed by PID.
     */
    public void sendAllAddrServerRecordsToCS(Long senderPID, NIOMessageChannel nioChannel,
                                                  Map<Long, AddrServerRecord> registry) throws IOException {
        for (AddrServerRecord record : registry.values()) {
            UpdateMessage<AddrServerRecord> message = UpdateMessage.asRecordPrimaryToCS(senderPID, record);
            try {
                nioChannel.sendMessage(message.toJson());
            } catch (JsonProcessingException e) {
                System.err.println("Failed to serialize UpdateMessage<AddrServerRecord>: " + e.getMessage());
            } catch (IOException ioe) {
                System.err.println("Failed to send UpdateMessage<AddrServerRecord>: " + ioe.getMessage());
                throw ioe;
            }
        }
        System.out.println("Done sending all AddrServerRecords to CHATSERVER with PID: " + nioChannel.getServerPID());
    }

    /**
     * Sends all currently known {@link ChatServerRecord} entries from the Primary
     * to a newly registered {@code ChatServer}.
     *
     * <p>This ensures that the ChatServer is fully synchronized with the current
     * set of active ChatServers in the network.</p>
     *
     * <p><strong>Note:</strong> If an {@link IOException} occurs during sending,
     * this method invokes the {@code ConnectionCleanupManager} to remove the
     * broken connection. Since this method is only used for targeted record sync,
     * cleanup is preferred over retry logic here.</p>
     *
     * @param senderPID the PID of the (Primary) AddressingServer sending the updates.
     * @param nioChannel the {@link NIOMessageChannel} used to send the updates.
     * @param registry a {@code Map} of {@code ChatServerRecord} entries keyed by PID.
     */
    public void sendAllChatServerRecordsToCS(Long senderPID, NIOMessageChannel nioChannel,
                                                  Map<Long, ChatServerRecord> registry) throws IOException {
        for (ChatServerRecord record : registry.values()) {
            UpdateMessage<ChatServerRecord> message = UpdateMessage.csRecordPrimaryToCS(senderPID, record);
            try {
                nioChannel.sendMessage(message.toJson());
            } catch (JsonProcessingException e) {
                System.err.println("Failed to serialize UpdateMessage<ChatServerRecord>: " + e.getMessage());
            } catch (IOException ioe) {
                System.err.println("Failed to send UpdateMessage<ChatServerRecord>: " + ioe.getMessage());
                throw ioe;
            }
        }
        System.out.println("Done sending all ChatServerRecords to CHATSERVER with PID: " + nioChannel.getServerPID());
    }

    /**
     * Sends all known {@link AddrServerRecord} and {@link ChatServerRecord} entries
     * from the Primary to a newly registered {@code ChatServer}.
     *
     * <p>This method sequentially invokes {@link #sendAllAddrServerRecordsToCS} and
     * {@link #sendAllChatServerRecordsToCS} to ensure full synchronization.</p>
     *
     * <p><strong>Note:</strong> If an error occurs during the sending of either set of records,
     * it will be caught and logged internally. This method does not propagate exceptions
     * upward or handle full synchronization retries. It assumes the calling component
     * will decide whether recovery, retry, or channel cleanup is appropriate.</p>
     *
     * @param senderPID the PID of the (Primary) AddressingServer sending the records.
     * @param nioChannel the {@link NIOMessageChannel} used for the transmission.
     * @param chatServerRegistry a map of all {@code ChatServerRecord} entries, keyed by PID.
     * @param addrServerRegistry a map of all {@code AddrServerRecord} entries, keyed by PID.
     */
    public void sendAllRecordsToCS(Long senderPID, NIOMessageChannel nioChannel,
                                        Map<Long, ChatServerRecord> chatServerRegistry,
                                        Map<Long, AddrServerRecord> addrServerRegistry) throws IOException {
        this.sendAllAddrServerRecordsToCS(senderPID, nioChannel, addrServerRegistry);
        this.sendAllChatServerRecordsToCS(senderPID, nioChannel, chatServerRegistry);
    }


    /**
     * Synchronizes the set of active Addressing Server Process IDs (PIDs) with a Replica.
     * <p>
     * This method encapsulates the provided PID set into a {@link PrimaryResponseMessage}
     * and transmits it over the specified {@link NIOMessageChannel}. It allows Replicas
     * to maintain a consistent view of the network and identify Addressing Server
     * records that should be purged from their local registries.
     * </p>
     *
     * @param senderPID  The process ID of the Primary Addressing Server initiating the sync.
     * @param nioChannel The communication channel to the target Replica Addressing Server.
     * @param activePids The current set of active Addressing Server PIDs in the network.
     * @throws IOException If a network error occurs during transmission, requiring the caller to handle connection cleanup.
     */
    public void sendAddrServerPidsToReplica(Long senderPID, NIOMessageChannel nioChannel,
                                            Set<Long> activePids) throws IOException {
        if (activePids == null || activePids.isEmpty() || nioChannel == null) {
            debug(DEBUG_BASIC, "[Broadcast Manager] Aborting PID sync: records or channel is null.");
            return;
        }

        PrimaryResponseMessage<Set<Long>> message =
                PrimaryResponseMessage.addrServerPidList(this.genMID.nextID(), senderPID, Roles.REPLICA, activePids);

        try {
            nioChannel.sendMessage(message.toJson());
            debug(DEBUG_DETAILED, String.format("Successfully synced %d Peer PIDs to REPLICA (PID: %d)",
                    activePids.size(), nioChannel.getServerPID()));

        } catch (JsonProcessingException e) {
            debug(DEBUG_BASIC, "Serialization error for Peer PID list: " + e.getMessage());
        } catch (IOException ioe) {
            debug(DEBUG_BASIC, "Network error syncing PIDs to REPLICA " + nioChannel.getServerPID() + ": " + ioe.getMessage());
            throw ioe; // Re-throw error so the caller knows the connection is (likely) dead
        }
    }

    /**
     * Synchronizes the set of active Chat Server Process IDs (PIDs) with a Replica.
     * <p>
     * This method encapsulates the provided PID set into a {@link PrimaryResponseMessage}
     * using the {@link ObjectTypes#PID_SET} identifier. This enables the Replica to
     * verify its local registry against the Primary's state and remove stale
     * Chat Server records.
     * </p>
     *
     * @param senderPID  The process ID of the Primary Addressing Server initiating the sync.
     * @param nioChannel The communication channel to the target Replica Addressing Server.
     * @param activePids The current set of active Chat Server PIDs in the network.
     * @throws IOException If a network error occurs during transmission, requiring the caller to handle connection cleanup.
     */
    public void sendChatServerPidsToReplica(Long senderPID, NIOMessageChannel nioChannel,
                                            Set<Long> activePids) throws IOException {
        if (activePids == null || activePids.isEmpty() || nioChannel == null) {
            debug(DEBUG_BASIC, "[Broadcast Manager] Aborting ChatServer PID sync: records or channel is null.");
            return;
        }

        PrimaryResponseMessage<Set<Long>> message =
                PrimaryResponseMessage.chatServerPidList(this.genMID.nextID(), senderPID, Roles.REPLICA, activePids);

        try {
            nioChannel.sendMessage(message.toJson());
            debug(DEBUG_DETAILED, String.format("Successfully synced %d ChatServer PIDs to REPLICA (PID: %d)",
                    activePids.size(), nioChannel.getServerPID()));

        } catch (JsonProcessingException e) {
            debug(DEBUG_BASIC, "Serialization error for ChatServer PID list: " + e.getMessage());
        } catch (IOException ioe) {
            debug(DEBUG_BASIC, "Network error syncing ChatServer PIDs to REPLICA " + nioChannel.getServerPID() + ": " + ioe.getMessage());
            throw ioe;
        }
    }


}
