package io.github.cpsc559.team16.chatserver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cpsc559.team16.common.dto.ConnectionType;
import io.github.cpsc559.team16.common.utilities.BaseMessage;
import io.github.cpsc559.team16.common.utilities.ChatLog;
import io.github.cpsc559.team16.common.utilities.ChatLogUpdate;
import io.github.cpsc559.team16.common.utilities.ClientServerMessage;
import io.github.cpsc559.team16.common.utilities.ServerServerMessage;
import static io.github.cpsc559.team16.common.logging.DebugLogger.*;
/**
 * Handles incoming messages specifically for interactions between server
 * instances in the chat system.
 * <p>
 * The {@link ServerHandler} processes a variety of commands, including
 * requesting and responding to chat logs,
 * handling {@code PING} and {@code PONG} messages for connection health checks,
 * and merging chat logs from peers.
 * It also manages the registration of incoming peer connections and updates the
 * state of the server accordingly.
 * </p>
 *
 * <h3>Debugging:</h3>
 * <p>
 * The debug level controls the verbosity of logs throughout the server's
 * operations. Different debug levels are used for:
 * <ul>
 * <li>{@code #DEBUG_BASIC} - Logs basic events, such as startup and major
 * actions like connecting peers or sending messages.</li>
 * <li>{@code DEBUG_NORMAL} - Logs regular operations, including received
 * messages and actions taken.</li>
 * <li>{@code #DEBUG_DETAILED} - Logs detailed flow, like parsing messages and
 * responding to clients.</li>
 * <li>{@code #DEBUG_LOW_LEVEL} - Logs low-level operations, such as I/O
 * activities like reading and writing data.</li>
 * <li>{@code #DEBUG_EXTREME} - Logs everything, useful for debugging edge cases
 * and deep issues.</li>
 * </ul>
 * </p>
 *
 * <h3>Core Methods:</h3>
 * <ul>
 * <li>{@link #handle(BaseMessage, ConnectionContext, SelectionKey)} - Processes
 * incoming messages based on the command type.</li>
 * <li>{@link #mergeChatlog(ServerServerMessage)} - Merges chat log data from a
 * peer server into the local chat log.</li>
 * <li>{@link #getChatLogContents(String)} - Retrieves the chat log content and
 * formats it as a {@code RESPONSE_CHATLOG} message.</li>
 * <li>{@link #printPrettyJson(String)} - Prints JSON data in a readable,
 * formatted way for debugging purposes.</li>
 * </ul>
 *
 * <h3>Message Handling:</h3>
 * <p>
 * The server responds to different types of messages, such as:
 * <ul>
 * <li>{@code REQUEST_CHATLOG} - Retrieves and sends the chat log to the
 * requesting server.</li>
 * <li>{@code RESPONSE_CHATLOG} - Receives and merges chat logs from peer
 * servers.</li>
 * <li>{@code PING} - Responds with a {@code PONG} to verify the server is
 * alive.</li>
 * <li>{@code PONG} - Acknowledges receipt of a {@code PING} message and resets
 * the activity state.</li>
 * </ul>
 * </p>
 * 
 * @see ChatServer for the main chat server logic
 * @see ConnectionContext for connection-related details
 */
@SuppressWarnings("unused")
class ServerHandler implements ConnectionHandler {


    /**
     * Handles incoming messages, processes different types of commands, and updates
     * the connection state accordingly.
     * <p>
     * This method is the core message handler for the {@link ServerHandler},
     * responsible for handling communication
     * between server instances in the chat system. It processes different commands
     * from peer servers, such as
     * {@code REQUEST_CHATLOG}, {@code RESPONSE_CHATLOG}, {@code PING}, and
     * {@code PONG}, and takes appropriate actions
     * based on the command type.
     * </p>
     * <h3>Message Handling Flow:</h3>
     * <ul>
     * <li>First, checks if the received message is a {@link ServerServerMessage}.
     * If it's not, it delegates handling to the
     * {@link ClientHandler} for {@link ClientServerMessage} types.</li>
     * <li>If the message is a valid {@code ServerServerMessage}, the method logs
     * the received message details for debugging.</li>
     * <li>For {@code REQUEST_CHATLOG} command, the server checks the sender’s peer
     * ID and updates the connected peers map,
     * then retrieves and sends the chat log contents back to the requesting
     * peer.</li>
     * <li>For {@code RESPONSE_CHATLOG} command, the server receives a chat log from
     * a peer and merges it into the local chat log.</li>
     * <li>For {@code PING} command, the server responds with a {@code PONG} message
     * to confirm its availability and presence.</li>
     * <li>For {@code PONG} command, the server updates the connection's state,
     * acknowledging that the peer is still alive.</li>
     * </ul>
     * 
     * <h3>Exceptions:</h3>
     * <ul>
     * <li>If a {@code ServerServerMessage} cannot be processed (e.g., invalid JSON
     * format), an exception is logged, and the
     * connection is not updated.</li>
     * </ul>
     *
     * @param message the incoming {@link BaseMessage} (either
     *                {@link ServerServerMessage} or {@link ClientServerMessage})
     * @param ctx     the {@link ConnectionContext} associated with the connection
     * @param key     the {@link SelectionKey} representing the client channel in
     *                the selector
     */

    public void handle(BaseMessage message, ConnectionContext ctx, SelectionKey key) {
        // FIX C: Universal activity reset
        // Any valid message received on this channel proves the peer is alive.
        ctx.lastActivityTime = System.currentTimeMillis();
        ctx.awaitingPong = false;
        ctx.missedPongs = 0;

        if (!(message instanceof ServerServerMessage msg)) {
            if (message instanceof ClientServerMessage clientMsg) {
                ConnectionHandler clientHandler = ChatServer.getHandler(ConnectionType.CLIENT);
                if (clientHandler != null) {
                    clientHandler.handle(clientMsg, ctx, key);
                } else {
                    System.err.println("No ClientHandler found in handler map.");
                }
            } else {
                System.err.println("Unhandled message type in ServerHandler: " + message.getClass().getSimpleName());
            }
            return;
        } else {

            debug(DEBUG_DETAILED, String.format(
                    "[SERVER] Received ServerServerMessage:\n" +
                            "  Command   = %s\n" +
                            "  Content   = %s\n" +
                            "  MessageID = %s\n" +
                            "  Sender    = %s\n" +
                            "  Receiver  = %s\n" +
                            "  TimeSent  = %s",
                    msg.getCommand(),
                    msg.getContent(),
                    msg.getMessageId(),
                    msg.getSender(),
                    msg.getReceiver(),
                    msg.getTimeSent()));

            ServerServerMessage reply;

            try {
                if (msg.getCommand().equals("REQUEST_CHATLOG")) {

                    try {
                        long senderPID = Integer.parseInt(msg.getSender());
                        ctx.peerPID = senderPID;

                        if (!ChatServer.getConnectedPeers().containsKey(senderPID)) {
                            ChatServer.getConnectedPeers().put(senderPID, ctx);
                            debug(DEBUG_BASIC, "[PEER] Added incoming peer PID=" + senderPID + " to connectedPeers");
                        }

                    } catch (NumberFormatException e) {
                        debug(DEBUG_BASIC, "Invalid peer ID in REQUEST_CHATLOG sender field: " + msg.getSender());
                    }

                    reply = getChatLogContents(msg.getSender());
                    printPrettyJson(reply.toJson());
                    WriteUtils.enqueueResponse(ctx, key, reply.toJson() + "\n");
                    key.selector().wakeup(); // ensure selector wakes to handle OP_WRITE

                } else if (msg.getCommand().equals("RESPONSE_CHATLOG")) {

                    long senderPID = Integer.parseInt(msg.getSender());
                    ctx.peerPID = senderPID;

                    if (!ChatServer.getConnectedPeers().containsKey(senderPID)) {
                        ChatServer.getConnectedPeers().put(senderPID, ctx);
                        debug(DEBUG_BASIC, "[PEER] Added incoming peer PID=" + senderPID + " to connectedPeers");
                    }
                    mergeChatlog(msg);
                    System.out.println("merged chatlog with peer: " + msg.getSender());

                } else if (msg.getCommand().equals("PING")) {
                    debug(DEBUG_NORMAL, "Received PING from " + msg.getSender());
                    ServerServerMessage pong = new ServerServerMessage(
                            String.valueOf(ChatServer.getPID()),
                            msg.getSender(),
                            "PONG",
                            "");
                    WriteUtils.enqueueResponse(ctx, key, pong.toJson() + "\n");
                    key.selector().wakeup();
                    return;
                } else if (msg.getCommand().equals("PONG")) {
                    // Logic is now redundant due to universal reset at top, but kept for logging
                    debug(DEBUG_NORMAL, "Received PONG from " + msg.getSender());
                    return;
                }

            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Merges the chat log content received from a peer server into the local chat
     * log.
     * <p>
     * This method processes the received chat log, parses each line into individual
     * {@link ClientServerMessage} objects,
     * and adds them to the local chat log. If the received chat log is empty or
     * contains invalid messages, the operation
     * is skipped. If valid messages are found, they are merged into the chat log
     * and the update is logged.
     * </p>
     *
     * <h3>Steps:</h3>
     * <ul>
     * <li>Checks if the received content is empty. If it is, the merge process is
     * skipped.</li>
     * <li>Reads the content line by line, attempting to parse each line into a
     * {@link ClientServerMessage} object.</li>
     * <li>If parsing is successful, the message is added to the list of messages to
     * be merged.</li>
     * <li>If no valid messages are found in the content, the merge operation is
     * skipped, and a debug message is logged.</li>
     * <li>If valid messages are found, they are merged into the local chat log
     * using the {@link ChatLog#merge(ChatLogUpdate)} method.</li>
     * <li>The merge operation is logged for debugging purposes, indicating the
     * number of messages merged.</li>
     * </ul>
     *
     * <h3>Exceptions:</h3>
     * <ul>
     * <li>IOException: If there is an error reading the content of the received
     * chat log.</li>
     * <li>JsonProcessingException: If a message line cannot be parsed into a
     * {@link ClientServerMessage} object.</li>
     * </ul>
     *
     * @param msg the {@link ServerServerMessage} containing the chat log content to
     *            be merged
     */

    private static void mergeChatlog(ServerServerMessage msg) {
        String content = msg.getContent();
        if (content == null || content.isEmpty()) {
            debug(DEBUG_BASIC, "Received empty chat log. Nothing to merge.");
            return;
        }

        List<ClientServerMessage> messages = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new StringReader(content))) {
            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    ClientServerMessage message = BaseMessage.fromJson(line, ClientServerMessage.class);
                    messages.add(message);
                } catch (Exception e) {
                    System.err.println("Failed to parse chat log message line: " + line);
                    e.printStackTrace();
                }
            }

            if (messages.isEmpty()) {
                debug(DEBUG_BASIC, "No valid messages found in received chat log.");
                return;
            }

            ChatLogUpdate update = new ChatLogUpdate(
                    msg.getSender(),
                    msg.getReceiver(),
                    messages);

            ChatServer.getChatLog().merge(update);
            debug(DEBUG_BASIC, "Merged " + messages.size() + " messages from received chat log.");

        } catch (IOException e) {
            System.err.println("Error while reading chat log content: " + e.getMessage());
        }
    }

    /**
     * Retrieves the chat log content from a file and constructs a
     * {@link ServerServerMessage} containing the log data.
     * <p>
     * This method reads the chat log file line by line, appending the content to a
     * {@link StringBuilder}. If the file is
     * successfully read, the method creates a {@code RESPONSE_CHATLOG} message that
     * includes the chat log content as its
     * body. If there is an error reading the file, it logs an error and returns
     * {@code null}.
     * </p>
     * 
     * <h3>Steps:</h3>
     * <ul>
     * <li>Reads the chat log file specified by
     * {@link ChatServer#getChatLogFile()}.</li>
     * <li>Appends each line of the file to the {@link StringBuilder} to accumulate
     * the log contents.</li>
     * <li>If the file is read successfully, constructs a
     * {@link ServerServerMessage} containing the chat log content.</li>
     * <li>If an {@link IOException} occurs during file reading, logs an error
     * message and returns {@code null}.</li>
     * </ul>
     *
     * <h3>Return:</h3>
     * <ul>
     * <li>Returns a {@link ServerServerMessage} containing the chat log content if
     * the file was read successfully.</li>
     * <li>Returns {@code null} if an error occurred while reading the chat log
     * file.</li>
     * </ul>
     *
     * @param receiverPid the peer ID of the receiver to whom the chat log is being
     *                    sent
     * @return a {@link ServerServerMessage} containing the chat log content, or
     *         {@code null} if an error occurred
     * @see ChatServer#getChatLogFile() for the chat log file location
     */

    private static ServerServerMessage getChatLogContents(String receiverPid) {
        StringBuilder logContents = new StringBuilder();
        String file = ChatServer.getChatLogFile();

        try (BufferedReader logReader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = logReader.readLine()) != null) {
                logContents.append(line).append("\n");
            }
            System.out.println("[INFO] Chat log successfully loaded from file.");
        } catch (IOException e) {
            System.err.println("Error reading chat log: " + e.getMessage());
            return null;
        }

        return new ServerServerMessage(
                String.valueOf(ChatServer.getPID()), // sender PID
                receiverPid, // receiver PID
                "RESPONSE_CHATLOG", // command
                logContents.toString() // chat log as content
        );
    }

    /**
     * Prints the JSON string in a pretty, readable format to the console.
     * <p>
     * This method uses the Jackson {@link ObjectMapper} to parse and pretty-print
     * the provided JSON string.
     * If the string is valid JSON, it prints the formatted JSON to the console. If
     * the string is invalid, it logs an error.
     * </p>
     * 
     * @param jsonString the JSON string to be pretty-printed
     * @see ObjectMapper for JSON parsing and formatting
     */
    public static void printPrettyJson(String jsonString) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode json = mapper.readTree(jsonString);
            String prettyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
            System.out.println(prettyJson);
        } catch (Exception e) {
            System.err.println("Invalid JSON: " + e.getMessage());
        }
    }

}
