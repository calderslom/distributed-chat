package io.github.cpsc559.team16.client;

import io.github.cpsc559.team16.common.utilities.ClientServerMessage;
import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

/**
 * Utility class for creating and sending messages in the chat client.
 * <p>
 * This class provides methods to create new messages with unique IDs and send
 * them to the server.
 * It handles message creation, sending, and error handling during the message
 * transmission process.
 * </p>
 */
public class MessageUtils {

    private Client client;

    public MessageUtils(Client client) {
        this.client = client;
    }

    /**
     * Creates a new chat message with a unique ID.
     * The message is created with:
     * - Current timestamp
     * - Incremented message counter
     * - Specified content and receiver
     * 
     * @param msgContents The content of the message to send
     * @param receiver    The intended recipient of the message (e.g., "fellow
     *                    clients" for broadcast)
     * @return A new ClientServerMessage with a unique ID and current timestamp
     */
    public ClientServerMessage createMessage(String msgContents, String receiver) {
        debug(DEBUG_DETAILED, "Creating message for receiver: " + receiver);
        return new ClientServerMessage(client.getUsername(), receiver, client.getNextSendCounter(), msgContents);
    }

    /**
     * Sends a message to the server.
     * If the message fails to send:
     * - It is added to the message queue for retry
     * - A reconnection attempt is triggered
     * - The error is logged at DEBUG_NORMAL level
     * 
     * @param msg The message to send to the server
     * @throws IllegalStateException if the client is not connected to the server
     */
    public void sendMessage(ClientServerMessage msg) {
        try {
            String json = msg.toJson();
            debug(DEBUG_DETAILED, "Outbound JSON: " + json);
            client.getOut().println(json);
        } catch (Exception e) {
            debug(DEBUG_NORMAL, "Error sending message: " + e.getMessage());
            client.getMessageQueue().add(msg);
            client.getConnectionManager().reconnect();
        }
    }

    /**
     * Posts a system message to the client's display log.
     * <p>
     * This method appends a formatted system notification to the display log,
     * which is then rendered by the {@link OutputThread} on the next UI refresh.
     * The message is prefixed with "[SYSTEM]" to distinguish it from regular
     * chat messages. Access to the display log is synchronized to prevent
     * concurrent modification by other threads.
     * </p>
     *
     * @param message the message content to display to the user
     */
    public void postSystemMessage(String message) {
        synchronized (client.getDisplayLog()) {
            ClientServerMessage msg = new ClientServerMessage("System", "all", -1, String.format("[SYSTEM] %s", message));
            msg.setCommand("INFO");
            client.getDisplayLog().add(msg);
        }
    }
    
}
