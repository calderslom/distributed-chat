package io.github.cpsc559.team16.client;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import io.github.cpsc559.team16.common.utilities.BaseMessage;
import io.github.cpsc559.team16.common.utilities.ClientServerMessage;
import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

/**
 * A thread responsible for receiving messages from the server.
 * <p>
 * This thread handles the real-time reception of messages from the chat server,
 * managing the acknowledgment and status updates of messages. It ensures that:
 * <ul>
 * <li>Messages are processed as they arrive.</li>
 * <li>Message IDs are checked to prevent processing duplicates.</li>
 * <li>Message logs are updated in a thread-safe manner using synchronized
 * collections.</li>
 * <li>Special handling is applied to registration and acknowledgment
 * messages.</li>
 * <li>Reconnection scenarios are handled if the server connection is lost.</li>
 * </ul>
 * </p>
 * <p>
 * Message Processing Flow:
 * <ol>
 * <li>Read a message from the server.</li>
 * <li>Parse the message from JSON into a {@link ClientServerMessage}
 * object.</li>
 * <li>Check for duplicate message IDs and skip if already processed.</li>
 * <li>Based on the message type:
 * <ul>
 * <li>If it's a "REGISTER" message, update the registration status and log
 * it.</li>
 * <li>If it's an acknowledgment message, update message status in logs.</li>
 * <li>If it's a regular chat message, add it to the chat log and display
 * it.</li>
 * </ul>
 * </li>
 * <li>Update the display log only if the message hasn't been displayed
 * yet.</li>
 * </ol>
 * </p>
 * <p>
 * The thread also manages the following:
 * <ul>
 * <li>Preventing duplicate processing of messages by maintaining a set of
 * processed message IDs.</li>
 * <li>Handling both regular chat messages and system-specific messages like
 * registration.</li>
 * <li>Gracefully handling server disconnections and reconnecting to the server
 * when necessary.</li>
 * </ul>
 * </p>
 * 
 * <p>
 * When the {@code terminate} flag is set to {@code true}, the thread will exit
 * gracefully. The thread also
 * uses {@link #shutdownLatch} to signal that it has finished and is ready for
 * shutdown.
 * </p>
 * 
 * @see ClientServerMessage
 * @see processedMessageIds
 * @see messageLock
 * @see displayLog
 */
public class ReceiverThread extends Thread {

        private final Client client;

        boolean historyReceived = false;
        boolean waitingForHistory = false;

        Queue<ClientServerMessage> bufferedMessages = new LinkedList<>();

        private volatile boolean isRegistered = false;
        private final Set<String> displayedMessageIds = Collections.synchronizedSet(new HashSet<>());

        public ReceiverThread(Client client) {
            this.client = client;
        }

        @Override
        public void run() {
            try {
                while (!client.isTerminated()) {
                    if (client.isConnected()) {
                        String serializedMsg = client.getIn().readLine();
                        if (serializedMsg == null) {
                            debug(DEBUG_NORMAL, "Connection closed by server");
                            client.getConnectionManager().reconnect();
                            continue;
                        }

                        debug(DEBUG_LOW_LEVEL, "Received message: " + serializedMsg);
                        ClientServerMessage msg = BaseMessage.fromJson(serializedMsg, ClientServerMessage.class);

                        synchronized (client.getMessageLock()) {
                            // Skip if we've already processed this message
                            if (!client.getProcessedMessageIds().add(msg.getMessageId())) {
                                continue;
                            }
                            if (waitingForHistory) {
                                // Handle message history response
                                if (msg.getCommand().equals("HISTORY_RESPONSE")) {
                                    debug(DEBUG_DETAILED, "Processing HISTORY_RESPONSE");
                                    String[] lines = msg.getContent().split("\n");
                                    for (String line : lines) {
                                        if (line.isBlank())
                                            continue;
                                        try {
                                            ClientServerMessage historicMsg = BaseMessage.fromJson(line,
                                                    ClientServerMessage.class);
                                            if (!client.getProcessedMessageIds().add(historicMsg.getMessageId()))
                                                continue;
                                            client.getMsgLog().add(historicMsg);
                                            client.getDisplayLog().add(historicMsg);
                                        } catch (Exception ex) {
                                            debug(DEBUG_NORMAL,
                                                    "Failed to parse message from history: " + ex.getMessage());
                                        }
                                    }
                                    historyReceived = true;
                                    waitingForHistory = false;

                                    // Replay buffered real-time messages
                                    for (ClientServerMessage buffered : bufferedMessages) {
                                        if (!client.getProcessedMessageIds().add(buffered.getMessageId()))
                                            continue;
                                        client.getMsgLog().add(buffered);
                                        client.getDisplayLog().add(buffered);
                                    }
                                    bufferedMessages.clear();
                                    continue;
                                }

                                // If history not yet received, buffer messages
                                if (!historyReceived) {
                                    debug(DEBUG_DETAILED,
                                            "Buffering message while waiting for history: " + msg.getMessageId());
                                    bufferedMessages.add(msg);
                                    continue;
                                }
                            }
                            if (msg.getCommand().equals("REGISTER")) {
                                client.getAwaitingAck()
                                        .removeIf(pendingMsg -> pendingMsg.getMessageId().equals(msg.getMessageId()));

                                // Only add registration success message if not already registered
                                if (!isRegistered) {
                                    isRegistered = true;

                                    // Add a success message to both logs
                                    ClientServerMessage successMsg = new ClientServerMessage("System", "all", -1,
                                            "Username: " + msg.getSender() + "\n-------------------\n");
                                    successMsg.setCommand("INFO");
                                    client.getMsgLog().add(successMsg);
                                    if (!displayedMessageIds.contains(successMsg.getMessageId())) {
                                        client.getDisplayLog().add(successMsg);
                                        displayedMessageIds.add(successMsg.getMessageId());
                                    }

                                    ClientServerMessage historyRequest = new ClientServerMessage(client.getUsername(), "server", -1,
                                            "10");
                                    historyRequest.setCommand("HISTORY");
                                    client.getMessageUtils().sendMessage(historyRequest);
                                    debug(DEBUG_DETAILED, "Requested message history after registration");

                                    waitingForHistory = true;
                                }
                            } else if (msg.getSender().equals(client.getUsername())) {
                                debug(DEBUG_DETAILED, "Message acknowledged by server");
                                client.getMsgLog().add(msg);
                                client.getPendingMessages().remove(msg.getMessageId());
                                client.getAwaitingAck()
                                        .removeIf(pendingMsg -> pendingMsg.getMessageId().equals(msg.getMessageId()));

                                // Only add to display log if not already displayed
                                if (!displayedMessageIds.contains(msg.getMessageId())) {
                                    client.getDisplayLog().add(msg);
                                    displayedMessageIds.add(msg.getMessageId());
                                }
                            } else {
                                debug(DEBUG_BASIC, "Received chat message from " + msg.getSender());
                                client.getMsgLog().add(msg);
                                if (!displayedMessageIds.contains(msg.getMessageId())) {
                                    client.getDisplayLog().add(msg);
                                    displayedMessageIds.add(msg.getMessageId());
                                }
                            }
                        }
                    } else {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            if (!client.isTerminated()) {
                                debug(DEBUG_NORMAL, "Receiver thread interrupted");
                            }
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                debug(DEBUG_NORMAL, "Receiver thread error: " + e.getMessage());
            } finally {
                client.getShutdownLatch().countDown();
            }
        }
    }
