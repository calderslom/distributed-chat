package io.github.cpsc559.team16.addressingserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.github.cpsc559.team16.common.dto.AddrServerRecord;
import io.github.cpsc559.team16.common.dto.ChatServerRecord;
import io.github.cpsc559.team16.common.messaging.*;
import io.github.cpsc559.team16.common.utilities.NIOMessageChannel;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Coordinates replication and synchronization of state between the PRIMARY
 * and REPLICA AddressingServers in the distributed network.
 *
 * <p>
 * This class is responsible for tracking pending events that require acknowledgments (ACKs)
 * from replicas, processing those acknowledgments, and ensuring that updates to shared
 * state (e.g., {@link AddrServerRecord} or {@link ChatServerRecord}) are applied consistently
 * across all nodes.
 * </p>
 *
 * <p>
 * Key responsibilities include:
 * <ul>
 *   <li>Maintaining a thread-safe registry of {@link PendingEvent} objects representing
 *       in-progress operations that require confirmation from one or more replicas.</li>
 *   <li>Processing ACK messages from replicas and notifying the original requester
 *       when all expected acknowledgments are received.</li>
 *   <li>Handling updates to AddressingServer and ChatServer records from the PRIMARY,
 *       applying them locally, and sending ACKs back to the PRIMARY to confirm replication.</li>
 *   <li>Managing failure scenarios for both ChatServer and AddressingServer processes,
 *       including cleanup of persistent connections and registry updates.</li>
 *   <li>Facilitating strong consistency guarantees across the distributed addressing server network.</li>
 * </ul>
 * </p>
 *
 * <p>
 * Typically used on REPLICA servers, this class ensures that replicated state changes
 * initiated by the PRIMARY are reliably acknowledged and that any failed replicas or
 * servers are properly cleaned up to maintain network consistency.
 * </p>
 */

public class ReplicaSyncCoordinator {

    private final PeerManager peerManager;
    private final BroadcastManager broadcastManager;
    private final ConnectionCleanupManager cleanupManager;

    /**
     * A thread-safe map that tracks message or event IDs and their corresponding {@link PendingEvent} instances.
     * <p>
     * Each entry in this map represents an in-progress network operation — such as a registration or broadcast —
     * that requires acknowledgments (ACKs) from one or more remote processes before it is considered complete.
     * </p><p>
     * This structure enables the system to:
     * <ul>
     *   <li>Track which events are still awaiting ACKs.</li>
     *   <li>Trigger follow-up actions once all expected responses are received.</li>
     *   <li>Retry or timeout events as needed based on elapsed time and retry limits.</li>
     * </ul>
     * Keys must be unique and are typically generated using the {@link MessageIDGenerator}.
     * </p>
     */
    private final ConcurrentMap<Long, PendingEvent> pendingEvents = new ConcurrentHashMap<>();


    public void addPendingEvent(Long messageID, PendingEvent event) {
        if (event.isComplete()) {
            try {
                // If no replicas are expected, execute the state change immediately
                event.respondToRequester();
            } catch (IOException e) {
                System.err.println("Immediate completion failed: " + e.getMessage());
            }
        } else {
            pendingEvents.put(messageID, event);
        }
    }

    public ConcurrentMap<Long, PendingEvent> getPendingEvents() { return pendingEvents; }


    public ReplicaSyncCoordinator(PeerManager peerManager, BroadcastManager broadcastManager, ConnectionCleanupManager cleanupManager) {
        this.peerManager = peerManager;
        this.broadcastManager = broadcastManager;
        this.cleanupManager = cleanupManager;
    }

    public void trackEvent(Long messageID, PendingEvent event) {
        pendingEvents.put(messageID, event);
    }

    /**
     * Processes an acknowledgment from a replica for a previously broadcasted message.
     *
     * <p>This method tracks the receipt of an acknowledgment for a given message ID and replica PID.
     * Once all expected replicas have acknowledged the message, it attempts to respond to the original
     * requester via the stored {@link NIOMessageChannel}. If this final response fails due to an
     * {@link IOException}, the channel used to communicate with the requester is returned so it can be
     * cleaned up by the caller (e.g., closed or deregistered).
     *
     * @param messageID the ID of the message that was acknowledged
     * @param recipientPID the process ID of the replica that sent the acknowledgment
     * @return the {@link NIOMessageChannel} of the original requester if responding failed,
     *         or {@code null} if no cleanup is needed.
     */
    public NIOMessageChannel processAck(Long messageID, Long recipientPID) {
        PendingEvent event = pendingEvents.get(messageID);
        if (event != null) {
            // DEBUG
            System.out.println("ACK received from network process with PID: " + recipientPID + " for message type: " + event.getMessageRequiringACK().getMsgType());
            event.removePendingRecipient(recipientPID);
            if (event.isComplete()) {
                pendingEvents.remove(messageID);
                try {
                    event.respondToRequester();
                } catch (IOException e) {
                    System.err.println("Failed to respond to process with PID: " + event.getRequestChannel().getServerPID());
                    return event.getRequestChannel(); // return the channel to the caller for cleanup (non-null return indicates failure)
                }
            }
        }
        return null; // no cleanup needed
    }


    /**
     * Used to clean up failed ChatServer processes.
     *
     * @param failureMessage
     * @param nioChannel
     * @param localPID
     * @param cleanupManager
     * @param failedPID
     */
    public void processFailureMessageSendAck(BaseAddrServerMessage<?> failureMessage,
                                               NIOMessageChannel nioChannel,
                                               Long localPID,
                                               ConnectionCleanupManager cleanupManager, Long failedPID) {
        if (failureMessage.getMessageID() != 0) {
            try {
                System.out.println("Sending *ServerFailureMessage* 'Replicated' ACK to Primary for message ID: " + failureMessage.getMessageID());
                nioChannel.sendMessage(AckMessage.replicated(
                        failureMessage.getMessageID(), localPID, true).toJson());
                if (failureMessage.getObjectType().equals(ObjectTypes.CHATSERVER_FAILURE)){
                    this.cleanupManager.getChatServerManager().removeFailedChatServer(failedPID);
                    this.cleanupManager.getChatServerManager().debugPrintAllServers();
                }
                else {
                    this.peerManager.removeFailedAddrServer(failedPID);
                    this.peerManager.debugPrintAllServers();
                }
            } catch (JsonProcessingException e) {
                System.err.printf(
                        "Failed to serialize AckMessage<%s> for broadcast. Context: messageID=%d, senderPID=%d, senderRole=%s. Exception: %s%n",
                        failureMessage.getObjectType(), failureMessage.getMessageID(), localPID, Roles.REPLICA, e.getMessage()
                );
            } catch (IOException ioe) {
                System.err.println("Failed to send ACK for message ID: " + failureMessage.getMessageID());
                cleanupManager.cleanupPersistentConnection(nioChannel.getSocketChannel(), true);
            }
        }
    }

    /**
     * Processes an incoming {@link UpdateMessage} containing an {@link AddrServerRecord},
     * updates the registry, and conditionally responds with an ACK if the message
     * is part of a synchronization event (i.e., message ID > 0).
     *
     * <p>
     * This method is typically used on REPLICA servers to respond to broadcasted
     * updates from the PRIMARY server. It ensures strong consistency by acknowledging
     * only those updates tied to a {@link PendingEvent}.
     * </p>
     *
     * @param updateMessage the {@link UpdateMessage} containing the {@link AddrServerRecord} update
     * @param nioChannel the {@link NIOMessageChannel} used to reply to the PRIMARY
     * @param localPID the process ID of the local replica
     * @param cleanupManager the {@link ConnectionCleanupManager} used to close faulty channels
     */
    public void processAddrServerUpdateSendAck(BaseAddrServerMessage<?> updateMessage,
                                               NIOMessageChannel nioChannel,
                                               Long localPID,
                                               ConnectionCleanupManager cleanupManager) {
        if (updateMessage.getMessageID() != 0) {
            try {
                System.out.println("Sending *AddrServerRecord* 'Replicated' ACK to Primary for message ID: " + updateMessage.getMessageID());
                nioChannel.sendMessage(AckMessage.replicated(
                        updateMessage.getMessageID(), localPID, true).toJson());
                this.peerManager.updateRecords(updateMessage.safeCastPayload(AddrServerRecord.class));
                this.peerManager.debugPrintAllServers();
            } catch (JsonProcessingException e) {
                System.err.printf(
                        "Failed to serialize AckMessage<%s> for broadcast. Context: messageID=%d, senderPID=%d, senderRole=%s. Exception: %s%n",
                        updateMessage.getObjectType(), updateMessage.getMessageID(), localPID, Roles.REPLICA, e.getMessage()
                );
            } catch (IOException ioe) {
                System.err.println("Failed to send ACK for message ID: " + updateMessage.getMessageID());
                cleanupManager.cleanupPersistentConnection(nioChannel.getSocketChannel(), true);
            }
        }
    }

    /**
     * Processes an incoming {@link UpdateMessage} containing an {@link ChatServerRecord},
     * updates the ChatServerRegistry, and responds with an ACK message.
     * <p>
     * This method is typically used on REPLICA servers to respond to broadcasted
     * updates from the PRIMARY server. It ensures strong consistency by acknowledging
     * update messages that are tied to a {@link PendingEvent} on the PRIMARY addressing server.
     * </p>
     *
     * @param updateMessage the {@link UpdateMessage} containing the {@link ChatServerRecord} update
     * @param nioChannel the {@link NIOMessageChannel} used to reply to the PRIMARY
     * @param localPID the process ID of the local replica
     * @param cleanupManager the {@link ConnectionCleanupManager} used to close faulty channels
     * @param registry the {@link ChatServerRegistry} that stores all the ChatServerRecords for this process.
     */
    public void processChatServerUpdateSendAck(BaseAddrServerMessage<?> updateMessage,
                                               NIOMessageChannel nioChannel,
                                               Long localPID,
                                               ConnectionCleanupManager cleanupManager,
                                               ChatServerRegistry registry) {
        if (updateMessage.getMessageID() != 0) {
            try {
                System.out.println("Sending *ChatServerRecord* 'Replicated' ACK to Primary for message ID: " + updateMessage.getMessageID());
                nioChannel.sendMessage(AckMessage.replicated(
                        updateMessage.getMessageID(), localPID, true).toJson());
                registry.updateOrInsertRecord(updateMessage.safeCastPayload(ChatServerRecord.class));
                registry.debugPrintAllServers();
            } catch (JsonProcessingException e) {
                System.err.printf(
                        "Failed to serialize AckMessage<%s> for broadcast. Context: messageID=%d, senderPID=%d, senderRole=%s. Exception: %s%n",
                        updateMessage.getObjectType(), updateMessage.getMessageID(), localPID, Roles.REPLICA, e.getMessage()
                );
            } catch (IOException ioe) {
                System.err.println("Failed to send ACK for message ID: " + updateMessage.getMessageID());
                cleanupManager.cleanupPersistentConnection(nioChannel.getSocketChannel(), true);
            }
        }
    }

}
