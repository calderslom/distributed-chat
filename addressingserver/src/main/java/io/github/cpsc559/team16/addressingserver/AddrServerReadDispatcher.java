package io.github.cpsc559.team16.addressingserver;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.github.cpsc559.team16.common.dto.AddrServerRecord;
import io.github.cpsc559.team16.common.dto.ChatServerRecord;
import io.github.cpsc559.team16.common.exceptions.ConnectionClosedException;
import io.github.cpsc559.team16.common.logging.DebugLogger;
import io.github.cpsc559.team16.common.logging.ServerDebugLogger;
import static io.github.cpsc559.team16.common.logging.DebugLogger.*;
import io.github.cpsc559.team16.common.messaging.*;


import static io.github.cpsc559.team16.common.messaging.MessageDeserializer.deserializeMessage;
import io.github.cpsc559.team16.common.utilities.NIOMessageChannel;

/**
 * Handles read events from registered {@code SocketChannel}'s and routes them
 * based on the server's role.
 * <p>
 * This class is responsible for delegating read events to the appropriate
 * handlers.
 * It uses a cascading sequence of switch statements to decide which method to
 * call:
 * </p>
 * <ul>
 * <li>{@code Message Type} - e.g. UPDATE, REQUEST, PING</li>
 * <li>{@code Sender Role} - e.g. REPLICA, PRIMARY, CHATSERVER</li>
 * <li>{@code Object Type} - e.g. ChatServerRecord, AddrServerRecord.
 * <p>
 * This can be an actual object, or simply an "identifier" - e.g.
 * AllChatServerInfo -
 * the {@code MessageDeserializer} is capable of parsing non-objects and will
 * not cause an error.
 * </p>
 * </li>
 * </ul>
 */
public class AddrServerReadDispatcher {
    private final AddressingServer server;
    private final PeerManager peerManager;
    private final ClientManager clientManager;
    private final ChatServerManager chatServerManager;
    private final BroadcastManager broadcastManager;
    private final MessageIDGenerator genMID;
    private final ConnectionCleanupManager cleanupManager;
    private final ReplicaSyncCoordinator replicaCoordinator;

    /**
     * A fixed-size thread pool used to offload network I/O processing from the main
     * selector loop.
     * <p>
     * This {@code ExecutorService} executes read and dispatch tasks asynchronously
     * to prevent
     * the main event loop from blocking during expensive operations such as
     * deserialization,
     * registration, or broadcast updates (multi-message streams).
     * </p>
     * <p>
     * The pool size is typically based on the number of available CPU cores on the
     * host system,
     * but can be adjusted for high-throughput scenarios.
     * </p>
     */
    private final ExecutorService executorService = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors());;

    public void shutdownExecutorService() {
        this.executorService.shutdownNow();
    }

    public AddrServerReadDispatcher(AddressingServer server) {
        this.server = server;
        this.peerManager = server.getPeerManager();
        this.clientManager = server.getClientManager();
        this.chatServerManager = server.getChatServerManager();
        this.broadcastManager = server.getBroadcastManager();
        this.genMID = server.getMessageIDGenerator();
        this.cleanupManager = server.getCleanupManager();
        this.replicaCoordinator = server.getReplicaSyncCoordinator();
    }

    /**
     * Retrieves the process ID for this AddrssingServer
     * 
     * @return A Long representing the process ID for this process in the network.
     */
    private Long getMyPID() {
        return this.server.getConfig().getPID();
    }

    /**
     * Uses an {@link NIOMessageChannel} for safe and abstracted message I/O between
     * two processes
     * on a {@link SocketChannel}. JSON messages are automatically deserialized into
     * a {@link BaseAddrServerMessage}
     * and routed through the {@code dispatchMsgType} method - the first in a
     * cascading sequence of routing methods.
     *
     * @param channel    The {@link SocketChannel} used to receive the message.
     * @param nioChannel The {@link NIOMessageChannel} used to decode and encode the
     *                   message.
     *
     * @throws ConnectionClosedException if the remote process has closed the
     *                                   connection.
     * @throws IOException               if an I/O error occurs during message
     *                                   reading or processing.
     */
    public void dispatch(SocketChannel channel, NIOMessageChannel nioChannel)
            throws ConnectionClosedException, IOException {
        String msgJson;
        while ((msgJson = nioChannel.receiveMessage()) != null) {
            BaseAddrServerMessage<?> message = deserializeMessage(msgJson);
            if (message != null) {
                dispatchMsgType(channel, nioChannel, message);
            } else {
                System.err.println("Could not deserialize incoming message: " + msgJson);
            }
        }
    }

    /**
     * Dispatches a received message based on its type, sender role, and object
     * type.
     *
     * @param channel    The {@link SocketChannel} used to receive the message.
     * @param nioChannel The {@link NIOMessageChannel} used to decode and encode the
     *                   message.
     * @param message    The parsed {@link BaseAddrServerMessage} containing the
     *                   message details.
     *
     */
    public void dispatchMsgType(SocketChannel channel, NIOMessageChannel nioChannel, BaseAddrServerMessage<?> message) {
        switch (message.getMsgType()) {
            case MessageTypes.ACK -> handleAck(channel, nioChannel, message);
            // I have removed this case so that ONLY a new connection request can register
            // with the Primary. This is handled in the main event loop.
            // case MessageTypes.REGISTER -> handleRegistration(channel, nioChannel,
            // message);
            case MessageTypes.UPDATE -> handleUpdate(channel, nioChannel, message);
            case MessageTypes.REQUEST -> handleRequest(channel, nioChannel, message);
            case MessageTypes.PING -> handlePing(channel, nioChannel, message);
            case MessageTypes.NOTIFICATION -> handleNotification(channel, nioChannel, message);
            case MessageTypes.SERVERFAILURE -> handleServerFailure(channel, nioChannel, message);
            case MessageTypes.ELECTION -> handleElection(channel, nioChannel, message);
            case MessageTypes.SHUTDOWN -> handleShutdownRequest(message);
            case MessageTypes.PRIMARY_RESPONSE -> handlePrimaryResponse(nioChannel, message);
            default -> System.err.println("Unrecognized message type: " + message.getMsgType());
        }
    }

    private void handleReplicationAck(BaseAddrServerMessage<?> ackMessage) {
        // The payload is a boolean - True if the message this ACK is for was
        // successfully processed.
        if (ackMessage.safeCastPayload(Boolean.class)) {
            // System.out.println("Replicated ACK received, success? " +
            // ackMessage.safeCastPayload(Boolean.class));
            // The messageID of this ackMessage is the same unique message ID as the message
            // that triggered it.
            // This is how we know if a message sent has been successfully received and
            // processed.
            Long eventID = ackMessage.getMessageID(); // adjust extraction as needed
            Long replicaPID = ackMessage.getSenderPID();
            System.out.printf("Primary has received ACK for message ID: %d - from Replica with PID: %d%n", eventID,
                    replicaPID);
            // This will return null if the message is sent successfully to the original
            // requested.
            // Otherwise, the channel is returned so that we can take appropriate action and
            // shut it down/cleanup.
            // NOTE: This is not the channel of a replica, it is a channel tied to whichever
            // process originally made a
            // request of the Primary addressing server which required a write/update to the
            // state -> SC action required.
            NIOMessageChannel senderChannel = replicaCoordinator.processAck(eventID, replicaPID);
            if (senderChannel != null) {
                this.server.getCleanupManager().cleanupPersistentConnection(senderChannel.getSocketChannel(), true);
            }
        } else {
            // We already handle retries with PendingEventWatchdog. This is here largely in
            // case our code grew and we
            // end up in the situation where we need to send an ACK indicating failure.
            // Currently we don't do that, because
            // there are no cases where a failure to process an update from the
            // AddressingServer occurs/matters.
        }
    }

    /**
     * <NOTE> Can probably remove the channel parameters, but I'm keeping them for
     * now in case an ACK needs to trigger an action.</NOTE>
     */
    private void handleAck(SocketChannel channel, NIOMessageChannel nioChannel, BaseAddrServerMessage<?> ackMessage) {
        switch (ackMessage.getObjectType()) {
            case AckObjectTypes.REGISTERED -> {
                // This should ONLY ever be received by a REPLICA from the PRIMARY AddressingServer
                // A Registration ACK is always sent with the pid as a string for the process as the payload.
                Long assignedPID = ackMessage.safeCastPayload(Long.class);
                long primaryPID = ackMessage.getSenderPID();

                if (primaryPID <= 0) {
                    System.err.println("[ERROR] Primary sent a REGISTRATION ACK with an invalid PID: " + primaryPID);
                    return;
                }
                else if (assignedPID == null) {
                    System.err.println("[REGISTRATION ERROR] Assigned PID is 'null'; registration failed.");
                    return;
                }
                server.getConfig().setPID(assignedPID);
                // This nioChannel was used to send the REGISTER message that triggered this ACK.
                // We didn't know the PRIMARY AddressingServer PID when making that initial connection -> Set it now
                nioChannel.setServerPID(primaryPID);
                debug(DEBUG_NORMAL, "Registration ACK received. This process has been assigned PID #"
                        + server.getConfig().getPID());
            }
            case AckObjectTypes.REPLICATED -> {
                DebugLogger.debug(DEBUG_DETAILED, "Replicated ACK received.");
                handleReplicationAck(ackMessage);
            }
            case AckObjectTypes.SYNCHRONIZED -> {
                // Stage 1: Validate the PID in the Payload
                Long assignedPID = ackMessage.safeCastPayload(Long.class);
                long primaryPID = ackMessage.getSenderPID();
                // Stage 2: Ensure the Primary actually identified itself
                if (primaryPID <= 0) {
                    System.err.println("[SYNC ERROR] Primary sent a SYNCHRONIZED ACK with an invalid PID: " + primaryPID);
                    return;
                }
                // Stage 4: Ensure the assigned PID matches my PID
                if (assignedPID == null || !assignedPID.equals(this.server.getConfig().getPID())) {
                    System.err.println("[SYNC ERROR] PID Mismatch. Expected: " + this.getMyPID() + ", Got: " + assignedPID);
                    return;
                }
                // Stage 5: Link this NIOMessageChannel to its process by setting the PID
                nioChannel.setServerPID(primaryPID);

                debug(DEBUG_NORMAL,
                        "Synchronization successful. Established persistent link to PRIMARY (PID: " + primaryPID + ")");
            }

            default -> debug(DEBUG_BASIC, "Unrecognized ACK response: " + ackMessage.getObjectType());
        }
    }

    /**
     * Handles a register message based on the sender role and object type.
     *
     * @param channel         The channel from which the message originated.
     * @param registerMessage The received update message.
     */
    public void handleRegistration(SocketChannel channel, NIOMessageChannel nioChannel,
            BaseAddrServerMessage<?> registerMessage)
            throws IOException {
        switch (registerMessage.getSenderRole()) {
            case Roles.CLIENT -> {
                // WE CAN TUNE THE RESPONSE HERE. FOR NOW I WILL SIMPLY DO AN ACK WITH THE
                // CHATSERVER info as - PID-IPADDRESS:PORTNUMBER
                ChatServerRecord updatedRecord = this.clientManager.sendHostAck(server.getConfig().getPID(),
                        nioChannel);
                if (updatedRecord != null) { // Broadcast ClientCountMessage to all servers.
                    System.out.println("Client directed to an active host.");
                    Long pid = this.getMyPID();
                    this.broadcastManager.broadcastChatServerRecord(pid, updatedRecord); // Broadcast the updated
                                                                                         // (client count) record to all
                                                                                         // servers.
                    ServerDebugLogger.printChatServer(updatedRecord);
                } else {
                    System.out.println("All ChatServer's are either FULL or INACTIVE");
                    // TODO: respond to client, informing them the service is currently at max capacity
                }
            }
            case Roles.CHATSERVER -> {
                System.out.println("ChatServer registration message has been received.");
                this.server.getRegistrationCoordinator().handleChatServerRegistration(channel, nioChannel,
                        registerMessage);
            }
            case Roles.REPLICA -> {
                System.out.println("Replica registration message has been received.");
                this.server.getRegistrationCoordinator().handleReplicaRegistration(channel, nioChannel,
                        registerMessage);
            }
            default -> throw new IllegalArgumentException(
                    "Unrecognized sender role for REGISTER: " + registerMessage.getSenderRole());
        }
    }

    private void handleUpdate(SocketChannel channel, NIOMessageChannel nioChannel,
            BaseAddrServerMessage<?> updateMessage) {
        System.out.printf("Replica handling update. MsgType: %s | ObjectType: %s | MsgID: %d | SenderRole: %s%n.",
                updateMessage.getMsgType(), updateMessage.getObjectType(), updateMessage.getMessageID(),
                updateMessage.getSenderRole());
        switch (updateMessage.getSenderRole()) {
            case Roles.CHATSERVER -> {
                switch (updateMessage.getObjectType()) { // THIS IS THE CASE WHERE A CHAT SERVER UPDATES THE ADDRESSING
                                                         // SERVER WITH ITS NEW CLIENT COUNT.
                    case ObjectTypes.CLIENT_COUNT -> {
                        int newClientCount = updateMessage.safeCastPayload(Integer.class);
                        Long csPid = updateMessage.getSenderPID();
                        try {
                            ChatServerRecord updatedRecord = this.server.getChatServerRegistry()
                                    .updateClientCount(newClientCount, csPid);
                            broadcastManager.broadcastChatServerRecordToCS(this.getMyPID(), updatedRecord);
                            // peerManager.broadcastChatServerRecord(this.getPID(), updatedRecord);
                            // chatServerManager.broadcastChatServerRecord(this.getPID(), updatedRecord);
                        } catch (NullPointerException e) {
                            System.err.println(e.getMessage());
                            // TODO - Add response that tell the ChatServer to re-register.
                        }
                    }
                    case ObjectTypes.CHAT_SERVER_RECORD -> {
                    }
                }
            }
            case Roles.PRIMARY -> {
                switch (updateMessage.getObjectType()) {
                    case ObjectTypes.ADDR_SERVER_RECORD -> {
                        // TODO - remove this conditional once all messages require an ACK and only use
                        // processAddrServerUpdateSendAck
                        if (updateMessage.getMessageID() == 0) {
                            // System.out.println("Replica in AddrServerRecord update - no message ID");
                            this.peerManager.updateRecords(updateMessage.safeCastPayload(AddrServerRecord.class));
                            this.peerManager.debugPrintAllServers();
                        } else {
                            // System.out.println("Replica in AddrServerRecord update - message ID: " +
                            // updateMessage.getMessageID());
                            this.replicaCoordinator.processAddrServerUpdateSendAck(updateMessage, nioChannel,
                                    this.server.getConfig().getPID(), cleanupManager);
                        }
                    }
                    case ObjectTypes.CHAT_SERVER_RECORD -> {
                        // TODO - remove this conditional once all messages require an ACK and only use
                        // processChatServerUpdateSendAck
                        if (updateMessage.getMessageID() == 0) {
                            // System.out.println("Replica in ChatServerRecord update - no message ID");
                            this.server.getChatServerRegistry()
                                    .updateOrInsertRecord(updateMessage.safeCastPayload(ChatServerRecord.class));
                            this.server.getChatServerRegistry().debugPrintAllServers();
                        } else {
                            // System.out.println("Replica in ChatServerRecord update - message ID: " +
                            // updateMessage.getMessageID());
                            this.replicaCoordinator.processChatServerUpdateSendAck(updateMessage, nioChannel,
                                    this.server.getConfig().getPID(), cleanupManager, server.getChatServerRegistry());
                        }
                    }
                    default ->
                        System.err.println("Unrecognized object type for UPDATE: " + updateMessage.getObjectType());
                }
            }
            case Roles.REPLICA -> {
                switch (updateMessage.getObjectType()) {
                    case ObjectTypes.ADDR_SERVER_RECORD -> {
                        System.out.println("REPLICA is in switch statement for updating AddrServerRecord from REPLICA");
                        peerManager.updateRecords(updateMessage.safeCastPayload(AddrServerRecord.class));
                    }
                    case ObjectTypes.CHAT_SERVER_RECORD -> {
                        System.out.println("REPLICA is in switch statement for updating ChatServerRecord from REPLICA");
                        chatServerManager.updateRecords(updateMessage.safeCastPayload(ChatServerRecord.class));
                    }
                }
            }
            default -> System.err.println("Unrecognized sender role for UPDATE: " + updateMessage.getSenderRole());
        }
    }

    /**
     * Processes incoming response messages from the Primary Addressing Server.
     * <p>
     * This handler identifies the requested data type (e.g., PID sets for active servers)
     * and uses {@code safeCastPayload} to retrieve the collection before triggering
     * the appropriate registry cleanup.
     * </p>
     *
     * @param nioChannel      The channel from which the message was received.
     * @param responseMessage The base message object containing the response payload.
     */
    private void handlePrimaryResponse(NIOMessageChannel nioChannel,
                                       BaseAddrServerMessage<?> responseMessage) {

        // Ensure the sender is actually the PRIMARY before processing
        if (!responseMessage.getSenderRole().equals(Roles.PRIMARY)) {
            debug(DEBUG_BASIC, "Received PrimaryResponse from non-primary role: " + responseMessage.getSenderRole());
            return;
        }

        switch (responseMessage.getObjectType()) {
            case ResponseObjectTypes.ALL_PEER_PIDS -> {
                Set<Long> activePeerPids = responseMessage.safeCastPayloadToLongSet();
                if (activePeerPids != null) {
                    this.server.getAddrServerRegistry().purgeStaleRecords(activePeerPids);
                }
            }
            case ResponseObjectTypes.ALL_CS_PIDS -> {
                Set<Long> activeChatPids = responseMessage.safeCastPayloadToLongSet();
                if (activeChatPids != null) {
                    this.server.getChatServerRegistry().purgeStaleRecords(activeChatPids);
                }
            }
            default -> debug(DEBUG_LOW_LEVEL, "No handler defined for PrimaryResponse object type: " + responseMessage.getObjectType());
        }
    }

    /**
     * Handles a request message based on the sender role and object type.
     *
     * @param channel        The channel from which the request originated.
     * @param requestMessage The received request message.
     */
    private void handleRequest(SocketChannel channel, NIOMessageChannel nioChannel,
            BaseAddrServerMessage<?> requestMessage) {
        switch (requestMessage.getSenderRole()) {
            case Roles.CHATSERVER -> {
                switch (requestMessage.getObjectType()) {
                    case (RequestObjectTypes.ALL_SERVER_RECORDS) -> {
                        executorService.submit(() -> {
                            try {
                                broadcastManager.sendAllRecordsToCS(this.getMyPID(), nioChannel,
                                        this.server.getChatServerRegistry().getRecords(),
                                        this.server.getAddrServerRegistry().getRecords());
                            } catch (IOException e) {
                                System.err.printf(
                                        "IOException triggered while responding to ALL_SERVER_RECORDS request (PID: %d). Triggering connection cleanup.%n",
                                        nioChannel.getServerPID());
                            }
                        });
                    }
                    case (RequestObjectTypes.CHAT_SERVER_RECORDS) -> {
                        executorService.submit(() -> {
                            try {
                                broadcastManager.sendAllChatServerRecordsToCS(this.getMyPID(), nioChannel,
                                        this.server.getChatServerRegistry().getRecords());
                            } catch (IOException e) {
                                System.err.printf(
                                        "IOException triggered while responding to CHAT_SERVER_RECORDS request (PID: %d). Triggering connection cleanup.%n",
                                        nioChannel.getServerPID());
                            }
                        });
                    }
                    case (RequestObjectTypes.ADDR_SERVER_RECORDS) -> {
                        executorService.submit(() -> {
                            try {
                                broadcastManager.sendAllAddrServerRecordsToCS(this.getMyPID(), nioChannel,
                                        this.server.getAddrServerRegistry().getRecords());
                            } catch (IOException e) {
                                System.err.printf(
                                        "IOException triggered while responding to ADDR_SERVER_RECORDS request (PID: %d). Triggering connection cleanup.%n",
                                        nioChannel.getServerPID());
                            }
                        });
                    }
                    default ->
                        System.err.println("Unrecognized ObjectType for REQUEST: " + requestMessage.getObjectType());
                }
            }
            case Roles.REPLICA -> {
                switch (requestMessage.getObjectType()) {
                    case (RequestObjectTypes.ALL_SERVER_RECORDS) -> {
                        executorService.submit(() -> {
                            try {
                                this.server.getLeaderElectionManager().performRegistryAudit();
                                broadcastManager.sendAllRecordsToReplica(this.getMyPID(), nioChannel,
                                        this.server.getChatServerRegistry().getRecords(),
                                        this.server.getAddrServerRegistry().getRecords());
                            } catch (IOException e) {
                                System.err.printf(
                                        "IOException triggered while responding to ALL_SERVER_RECORDS request (PID: %d). Triggering connection cleanup.%n",
                                        nioChannel.getServerPID());
                            }
                        });
                    }
                    case (RequestObjectTypes.CHAT_SERVER_RECORDS) -> {
                        executorService.submit(() -> {
                            try {
                                chatServerManager.auditRegistryConnections(this.getMyPID(), cleanupManager);
                                broadcastManager.sendAllChatServerRecordsToReplica(this.getMyPID(), nioChannel,
                                        this.server.getChatServerRegistry().getRecords());
                            } catch (IOException e) {
                                System.err.printf(
                                        "IOException triggered while responding to CHAT_SERVER_RECORDS request (PID: %d). Triggering connection cleanup.%n",
                                        nioChannel.getServerPID());
                            }
                        });
                    }
                    case (RequestObjectTypes.ADDR_SERVER_RECORDS) -> {
                        executorService.submit(() -> {
                            try {
                                peerManager.auditRegistryConnections(this.getMyPID(), cleanupManager);
                                broadcastManager.sendAllAddrServerRecordsToReplica(this.getMyPID(), nioChannel,
                                        this.server.getAddrServerRegistry().getRecords());
                            } catch (IOException e) {
                                System.err.printf(
                                        "IOException triggered while responding to ADDR_SERVER_RECORDS request (PID: %d). Triggering connection cleanup.%n",
                                        nioChannel.getServerPID());
                            }
                        });
                    }
                    case (RequestObjectTypes.ALL_PEER_PIDS) -> {
                        executorService.submit(() -> {
                            try {
                                broadcastManager.sendAddrServerPidsToReplica(this.getMyPID(), nioChannel,
                                        this.server.getAddrServerRegistry().getRecords().keySet());
                            } catch (IOException e) {
                                System.err.printf(
                                        "IOException triggered while responding to ALL_PEER_PIDS request (PID: %d). Triggering connection cleanup.%n",
                                        nioChannel.getServerPID());
                            }
                        });
                    }
                    case (RequestObjectTypes.ALL_CS_PIDS) -> {
                        executorService.submit(() -> {
                            try {
                                broadcastManager.sendChatServerPidsToReplica(this.getMyPID(), nioChannel,
                                        this.server.getChatServerRegistry().getRecords().keySet());
                            } catch (IOException e) {
                                System.err.printf(
                                        "IOException triggered while responding to ALL_CS_PIDS request (PID: %d). Triggering connection cleanup.%n",
                                        nioChannel.getServerPID());
                            }
                        });
                    }
                    default ->
                        System.err.println("Unrecognized ObjectType for REQUEST: " + requestMessage.getObjectType());
                }
            }
            default -> System.err.println("Unrecognized sender role for REQUEST: " + requestMessage.getSenderRole());
        }
    }

    /**
     * Handles a ping request, typically used for failure detection.
     *
     * @param channel The channel from which the ping request originated.
     * @param message The received ping message.
     */
    private void handlePing(SocketChannel channel, NIOMessageChannel nioChannel, BaseAddrServerMessage<?> message) {
        if (Roles.PRIMARY.equals(message.getTargetRole())) {
            // server.respondToPing(channel);
        }
    }

    /**
     * Handles server failure messages.
     *
     * @param channel The channel from which the message originated.
     * @param message The received server failure message.
     */
    private void handleServerFailure(SocketChannel channel, NIOMessageChannel nioChannel,
            BaseAddrServerMessage<?> message) {
        System.out.printf(
                "Handling failure message. MsgType: %s | ObjectType: %s | MsgID: %d | SenderRole: %s | FailedPID: %d%n.",
                message.getMsgType(), message.getObjectType(), message.getMessageID(), message.getSenderRole(),
                message.safeCastPayload(Long.class));
        if (message.getSenderRole().equals(Roles.PRIMARY)) {
            Long failedPID = message.safeCastPayload(Long.class);
            System.out.println("Replica received ServerFailure message for network PID: " + failedPID);
            this.replicaCoordinator.processFailureMessageSendAck(message, nioChannel,
                    this.getMyPID(), this.cleanupManager, failedPID);
        } else {
            String serverType = message.getObjectType();
            Long failedPID = message.safeCastPayload(Long.class);
            if (serverType.equals(ObjectTypes.ADDRSERVER_FAILURE)) {
                SocketChannel failedChannel = this.peerManager.getSocketChannelByPID(failedPID);
                if (failedChannel != null) {
                    // Sync state with replicas and then remove and broadcast to chat servers
                    cleanupManager.cleanupPersistentConnection(failedChannel, true);
                } else {
                    server.getAddrServerRegistry().removeRecordByKey(failedPID);
                }
            } else if (serverType.equals(ObjectTypes.CHATSERVER_FAILURE)) {
                SocketChannel failedChannel = this.chatServerManager.getChannelByPID(failedPID);
                if (failedChannel != null) {
                    System.out.println("going to cleanup");
                    // Sync state with replicas and then remove and broadcast to chat servers
                    cleanupManager.cleanupPersistentConnection(failedChannel, true);
                } else {
                    server.getChatServerRegistry().removeRecordByKey(failedPID);
                }

            }

        }
    }

    /**
     * Handles notification messages. THIS MAY NOT BE NEEDED
     *
     * @param channel The channel from which the notification originated.
     * @param message The received notification message.
     */
    private void handleNotification(SocketChannel channel, NIOMessageChannel nioChannel,
            BaseAddrServerMessage<?> message) {
        switch (message.getObjectType()) {
            case ObjectTypes.ELECTION_VOTE -> {

            }
            case ObjectTypes.CLIENT_COUNT -> {
                try {
                    // Get data directly from the BaseAddrServerMessage without casting to
                    // NotificationMessage
                    long chatServerId = message.getSenderPID();
                    Integer clientCount = message.safeCastPayload(Integer.class);

                    // Update the client count in the registry
                    synchronized (server.getChatServerRegistry()) {
                        ChatServerRecord record = server.getChatServerRegistry().getRecords().get(chatServerId);
                        if (record != null) {
                            record.setClientCount(clientCount);
                            System.out.println("Updated client count for " + chatServerId + " to " + clientCount);
                            // Broadcast the updated record to all connected AddressingServers
                            broadcastManager.broadcastChatServerRecord(this.getMyPID(), record);
                        } else {
                            System.err.println("Received client count update for unknown chat server: " + chatServerId);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error processing CLIENT_COUNT notification: " + e.getMessage());
                    e.printStackTrace();
                }
            }
            default -> System.err.println("Unknown notification message received and ignored.");
        }
    }

    /**
     * Handles election-related messages, used for leader election among Addressing
     * Servers.
     *
     * @param channel         The channel from which the election message
     *                        originated.
     * @param electionMessage The received election message.
     */
    private void handleElection(SocketChannel channel, NIOMessageChannel nioChannel,
            BaseAddrServerMessage<?> electionMessage) {
        server.getLeaderElectionManager().processElectionMessage(channel, nioChannel, electionMessage);
    }


    /**
     * Handles a shutdown request message.
     * <p>
     * This method validates that the shutdown request is explicitly targeted
     * at this AddressingServer instance by comparing the payload PID with
     * this process's PID. The payload is expected to contain a {@link Long}
     * representing the network PID of the process that should terminate.
     * </p>
     * <p>
     * If the PID matches, a graceful shutdown is initiated via the
     * {@code NetworkManager}. If the PID does not match, the message is ignored
     * and logged as invalid.
     * </p>
     *
     * @param shutdownMessage The received shutdown message containing the
     *                        target process PID as its payload.
     */
    private void handleShutdownRequest(BaseAddrServerMessage<?> shutdownMessage) {
        // Failsafe - check message (payload) PID to ensure request matches this processes PID.
        Long failedPID = shutdownMessage.safeCastPayload(Long.class);
        if (this.getMyPID().equals(failedPID)) {
            System.out.println("Shutdown request validated - this process has been flagged for termination.");
            server.shutdown();
            // Terminate the JVM immediately
            System.exit(0);
        }
        else {
            System.out.printf("Invalid shutdown message received - target PID = %d but this PID = %d\n", failedPID, this.getMyPID());
        }

    }

    /**
     * Routes a synchronization request to the centralized RegistrationCoordinator.
     * <p>
     * This method acts as a thin delegation layer, decoupling the network event
     * dispatching from the complex business logic of server identity validation.
     * </p>
     *
     * @param channel    the raw socket channel of the connecting process
     * @param nioChannel the message-oriented wrapper for the socket
     * @param msg        the incoming synchronization message
     * @param role       the claimed role of the sender (e.g., CHATSERVER or REPLICA)
     */
    public void handleSynchronizationRequest(SocketChannel channel, NIOMessageChannel nioChannel, BaseAddrServerMessage<?> msg, String role) {
        this.server.getRegistrationCoordinator().synchronizeServer(channel, nioChannel, msg, role);
    }



}
