package io.github.cpsc559.team16.chatserver;

import java.nio.channels.SelectionKey;
import java.util.Map;

import io.github.cpsc559.team16.common.messaging.AckObjectTypes;
import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.cpsc559.team16.common.utilities.BaseMessage;
import io.github.cpsc559.team16.common.utilities.ChatLog;
import io.github.cpsc559.team16.common.dto.AddrServerRecord;
import io.github.cpsc559.team16.common.dto.ChatServerRecord;
import io.github.cpsc559.team16.common.messaging.BaseAddrServerMessage;
import io.github.cpsc559.team16.common.messaging.MessageTypes;
import io.github.cpsc559.team16.common.messaging.ObjectTypes;
import io.github.cpsc559.team16.common.dto.ConnectionType;

/**
 * Handles all incoming messages from the Addressing Server.
 * <p>
 * This includes:
 * <ul>
 * <li>Processing ACK messages upon successful registration, which assigns a PID
 * and initializes the chat log.</li>
 * <li>Handling UPDATE messages containing lists of active peer servers,
 * forwarding them to the core ChatServer logic.</li>
 * </ul>
 * This handler is registered in the {@link ChatServer} and invoked via the
 * {@link ConnectionHandler} interface.
 */
class AddressingServerHandler implements ConnectionHandler {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Debug verbosity level, configurable via environment variable DEBUG_LEVEL
     * (default = 5 right now to make debuggin easy).
     */
    public static final int DEBUG_LEVEL = Integer.parseInt(System.getenv().getOrDefault("DEBUG_LEVEL", "5"));

    // Debug level constants
    private static final int DEBUG_NONE = 0; // No debug output (production mode)
    private static final int DEBUG_BASIC = 1; // Basic info: startup, shutdown, major events
    private static final int DEBUG_NORMAL = 2; // Normal operation details: connections, requests
    private static final int DEBUG_DETAILED = 3; // Detailed flow: entering methods, decision points
    private static final int DEBUG_LOW_LEVEL = 4; // Low-level operations: byte-level I/O, parsing
    private static final int DEBUG_EXTREME = 5; // Extreme detail: everything, for deep debugging

    /**
     * Outputs debug logs according to the specified verbosity level.
     *
     * @param level   the severity/detail level of the message
     * @param message the message content to print
     */
    private static void debug(int level, String message) {
        if (level <= DEBUG_LEVEL) {
            String prefix = switch (level) {
                case DEBUG_BASIC -> "[BASIC] ";
                case DEBUG_NORMAL -> "[NORMAL] ";
                case DEBUG_DETAILED -> "[DETAILED] ";
                case DEBUG_LOW_LEVEL -> "[LOW_LEVEL] ";
                case DEBUG_EXTREME -> "[EXTREME] ";
                default -> "[INFO] ";
            };
            System.out.println(prefix + message);
        }
    }

    /**
     * This method is part of the {@link ConnectionHandler} interface.
     * Since the Addressing Server only sends {@link BaseAddrServerMessage} objects,
     * this generic method should never be used. Logs an error if invoked.
     *
     * @param message ignored
     * @param ctx     connection context
     * @param key     selector key
     */
    @Override
    public void handle(BaseMessage message, ConnectionContext ctx, SelectionKey key) {
        debug(DEBUG_BASIC,
                "[ADDR_SERVER] Invalid message type passed to AddressingServerHandler: " + message.getClass());
    }

    /**
     * Main entry point for processing messages received from the Addressing Server.
     * Delegates to internal methods based on message type and objectType.
     *
     * @param message the incoming Addressing Server message
     * @param ctx     context for the connection
     * @param key     NIO selector key
     */
    public void handle(BaseAddrServerMessage<?> message, ConnectionContext ctx, SelectionKey key) {
        try {
            debug(DEBUG_LOW_LEVEL, "[ADDR_SERVER] Received message:\n" + message.toJson());

            String type = message.getMsgType();
            String objectType = message.getObjectType();

            debug(DEBUG_DETAILED, "[ADDR_SERVER] Dispatching message — type: " + type + ", objectType: " + objectType);

            if ("ACK".equalsIgnoreCase(type)) {
                handleAck(message, ctx, key);
            } else if ("UPDATE".equalsIgnoreCase(type) && "ChatServerRecord".equalsIgnoreCase(objectType)) {
                handleUpdate(message, ctx, key);
            } else if ("UPDATE".equalsIgnoreCase(type) && "AddressServerRecord".equalsIgnoreCase(objectType)) {
                handleUpdateAddr(message, ctx, key);
            } else if (MessageTypes.SERVERFAILURE.equals(type) && ObjectTypes.CHATSERVER_FAILURE.equals(objectType)) {

                handleServerFailure(message, ctx, key);
            } else {
                debug(DEBUG_NORMAL, "[ADDR_SERVER] Unhandled message — type: " + type + ", objectType: " + objectType);
            }

        } catch (Exception e) {
            debug(DEBUG_BASIC, "[ADDR_SERVER] Failed to handle message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Handles registration acknowledgment (ACK) messages from the Addressing
     * Server.
     * Sets the server's assigned PID, initializes the local chat log, and signals
     * registration success.
     *
     * @param message the ACK message containing the assigned PID
     * @param ctx     connection context
     * @param key     NIO selector key
     */
    private void handleAck(BaseAddrServerMessage<?> message, ConnectionContext ctx, SelectionKey key) {
        debug(DEBUG_NORMAL, "[ADDR_SERVER] Handling ACK...");

        try { // NOTE: Please try to use the AckObjectTypes and MessageTypes and ObjectTypes
              // declared in the Messaging module
              // This helps ensure there are no runtime errors due to syntax errors. It also
              // helps with readability IMO.
            if (!AckObjectTypes.REGISTERED.equals(message.getObjectType())) {
                debug(DEBUG_BASIC, "[ADDR_SERVER] Ignoring ACK with objectType: " + message.getObjectType());
                return;
            }

            String payload = message.getPayload().toString();
            int newID = Integer.parseInt(payload);

            // Set ID first
            ChatServer.setPID(newID);

            String CHATLOG_FILE = "src/main/java/com/Logs/chatlog_" + newID + ".log";
            String INDEX_FILE = "src/main/java/com/Logs/index_" + newID + ".json";
            ChatLog chatLog = new ChatLog(CHATLOG_FILE, INDEX_FILE);

            ChatServer.setChatLog(chatLog);

            // Then set registered flag and wake up selector
            ChatServer.setRegistered(true);
            key.selector().wakeup(); // Wake up the selector to check the registered flag

            debug(DEBUG_BASIC, String.format("\n[ADDR_SERVER] Registration successful — assigned PID: %d\n", newID));
        } catch (NumberFormatException e) {
            debug(DEBUG_BASIC, "[ADDR_SERVER] Failed to parse PID from ACK payload: " + message.getPayload());
        }
    }

    private void handleUpdateAddr(BaseAddrServerMessage<?> message, ConnectionContext ctx, SelectionKey key) {
        try {
            Object payload = message.getPayload();
            // Handle single ChatServerRecord
            if (payload instanceof AddrServerRecord record) {
                String host = record.getHostAddress();
                int port = record.getChatServerPort();
                System.out.println("New Adressing Server Primary" + host + " " + port);
                // TODO: Synchronize with Primary in much the same way that Replicas do!
                debug(DEBUG_BASIC, "Received AddrServerRecord from host: " + host + ", port: " + port);
            } else {
                debug(DEBUG_BASIC, "[ADDR_SERVER] Unknown payload type: " + payload.getClass().getName());
            }
        } catch (Exception e) {
            debug(DEBUG_BASIC, "[ADDR_SERVER] Failed to handle UPDATE: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Processes UPDATE messages containing a {@link ChatServerRecord} representing
     * a peer server.
     * Converts the payload to a JSON object and forwards it to
     * {@link ChatServer#processChatServerList}.
     *
     * @param message the UPDATE message with peer server details
     * @param ctx     connection context
     * @param key     NIO selector key
     */
    private void handleUpdate(BaseAddrServerMessage<?> message, ConnectionContext ctx, SelectionKey key) {
        debug(DEBUG_NORMAL, "[ADDR_SERVER] Handling UPDATE for ChatServerRecord...");

        if (!"ChatServerRecord".equals(message.getObjectType())) {
            debug(DEBUG_BASIC, "[ADDR_SERVER] UPDATE ignored — objectType is not ChatServerRecord");
            return;
        }

        try {
            Object payload = message.getPayload();
            JSONArray chatServersArray = new JSONArray();

            // Handle single ChatServerRecord
            if (payload instanceof ChatServerRecord record) {
                debug(DEBUG_LOW_LEVEL, "Directly processing ChatServerRecord for PID: " + record.getPID());
                ChatServer.processSingleChatServerRecord(record, ChatServer.getSelector());
                // Handle multiple ChatServerRecords
            } else if (payload instanceof Map<?, ?> map) {
                chatServersArray.put(new JSONObject(map));
                debug(DEBUG_LOW_LEVEL, "[ADDR_SERVER] Forwarding peer list to processChatServerList()");
                JSONObject wrapper = new JSONObject();
                wrapper.put("chatServers", chatServersArray);
                ChatServer.processChatServerList(wrapper.toString(), ChatServer.getSelector());
            } else {
                debug(DEBUG_BASIC, "[ADDR_SERVER] Unexpected payload type: " + payload.getClass());
                return;
            }

        } catch (Exception e) {
            debug(DEBUG_BASIC, "[ADDR_SERVER] Failed to handle UPDATE: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Processes server failure notifications received from the Addressing Server.
     * <p>
     * This method handles SERVERFAILURE messages with object type
     * CHATSERVER_FAILURE, which
     * are sent by the Addressing Server when a chat server has been detected as
     * failed.
     * It extracts the failed peer's PID from the message payload and removes it
     * from
     * the local peer connections.
     * </p>
     *
     * @param message the SERVERFAILURE message containing the failed peer's PID
     * @param ctx     connection context
     * @param key     NIO selector key
     */
    private void handleServerFailure(BaseAddrServerMessage<?> message, ConnectionContext ctx, SelectionKey key) {
        debug(DEBUG_NORMAL, "[ADDR_SERVER] Handling SERVERFAILURE for CHATSERVER_FAILURE...");

        try {
            Long failedPeerPID = message.safeCastPayload(Long.class);
            if (failedPeerPID == null) {
                debug(DEBUG_BASIC, "[ADDR_SERVER] Failed to extract PID from SERVERFAILURE message");
                return;
            }

            int failedPeerId = failedPeerPID.intValue();

            // Add this right before the containsKey check
            debug(DEBUG_BASIC, "--- SEARCHING FOR PID: " + failedPeerId + " ---");

            if (ChatServer.getConnectedPeers().isEmpty()) {
                debug(DEBUG_BASIC, "[!!!] Peer Map is EMPTY. No peers registered.");
            } else {
                for (Object keyInMap : ChatServer.getConnectedPeers().keySet()) {
                    boolean match = keyInMap.equals(failedPeerId);
                    debug(DEBUG_BASIC, String.format("Comparing: Target [%d] (Integer) vs MapKey [%s] (%s) | Match: %b",
                            failedPeerId,
                            keyInMap.toString(),
                            keyInMap.getClass().getSimpleName(),
                            match));
                }
            }

            // Remove the failed peer from our connected peers
            if (ChatServer.getConnectedPeers().containsKey(failedPeerId)) {
                debug(DEBUG_BASIC, "[ADDR_SERVER] Removing failed peer with ID: " + failedPeerId);
                ChatServer.getConnectedPeers().remove(failedPeerId);

                // If there's an active connection to this peer, close it
                for (SelectionKey peerKey : ChatServer.getSelector().keys()) {
                    if (!peerKey.isValid())
                        continue;

                    ConnectionContext peerCtx = (ConnectionContext) peerKey.attachment();
                    if (peerCtx != null && peerCtx.type == ConnectionType.SERVER
                            && peerCtx.peerPID == failedPeerId) {
                        debug(DEBUG_NORMAL, "[ADDR_SERVER] Closing connection to failed peer: " + failedPeerId);
                        try {
                            peerKey.cancel();
                            peerKey.channel().close();
                        } catch (Exception e) {
                            debug(DEBUG_BASIC, "[ADDR_SERVER] Error closing peer connection: " + e.getMessage());
                        }
                        break;
                    }
                }
            } else {
                debug(DEBUG_NORMAL, "[ADDR_SERVER] No connection found for failed peer ID: " + failedPeerId);
            }

        } catch (Exception e) {
            debug(DEBUG_BASIC, "[ADDR_SERVER] Failed to handle SERVERFAILURE: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Notifies the Addressing Server about a peer server crash.
     * <p>
     * This method is called when a peer is detected as unresponsive or has
     * disconnected unexpectedly.
     * It sends a message to the Addressing Server indicating which peer has
     * crashed, so that the
     * Addressing Server can update its registry and potentially notify other peers.
     * </p>
     *
     * @param crashedPeerId the ID of the peer that has crashed
     */
    public void notifyPeerCrash(long crashedPeerId) {
        try {
            // SAFETY CHECK: Do not notify for PID -1.
            // A PID of -1 means the connection failed before the handshake finished.
            if (crashedPeerId <= 0) {
                debug(DEBUG_NORMAL, "[ADDR_SERVER] Skipping notification for unidentified peer (ID: " + crashedPeerId + "). This is likely a startup race condition.");
                return;
            }

            debug(DEBUG_NORMAL, "[ADDR_SERVER] Preparing crash notification for peer ID: " + crashedPeerId);

            // Create a Standardized ServerFailureMessage using the factory method
            // Using the common utility classes from your imports
            io.github.cpsc559.team16.common.messaging.ServerFailureMessage<Long> crashMessage =
                    io.github.cpsc559.team16.common.messaging.ServerFailureMessage.chatServerFailed(
                            ChatServer.getPID(),
                            io.github.cpsc559.team16.common.messaging.Roles.CHATSERVER,
                            io.github.cpsc559.team16.common.messaging.Roles.PRIMARY,
                            (long) crashedPeerId
                    );

            String json = crashMessage.toJson() + "\n";

            // Find the addressing server connection by iterating through selector keys
            for (SelectionKey key : ChatServer.getSelector().keys()) {
                if (!key.isValid())
                    continue;

                Object attachment = key.attachment();

                // FIX: Verify attachment type before casting to avoid ClassCastException
                // Your listener keys have 'ConnectionType' enums as attachments, not 'ConnectionContext'
                if (attachment instanceof ConnectionContext) {
                    ConnectionContext ctx = (ConnectionContext) attachment;

                    if (ctx.type == ConnectionType.ADDRESSING_SERVER) {
                        // Queue the message to be sent via the non-blocking write logic
                        synchronized (ctx.writeQueue) {
                            ctx.writeQueue.add(java.nio.ByteBuffer.wrap(
                                    json.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                            ));
                        }

                        // Set OP_WRITE so the main loop handles the actual socket write
                        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                        key.selector().wakeup();

                        debug(DEBUG_BASIC, "[ADDR_SERVER] Sent SERVERFAILURE message for peer " + crashedPeerId
                                + " to Addressing Server");
                        return;
                    }
                }
            }

            debug(DEBUG_BASIC, "[ADDR_SERVER] No active Addressing Server connection found to report crash of PID: " + crashedPeerId);
        } catch (Exception e) {
            debug(DEBUG_BASIC, "[ADDR_SERVER] Error in notifyPeerCrash: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
