package io.github.cpsc559.team16.client;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.jline.reader.LineReader;
import org.jline.reader.UserInterruptException;

import io.github.cpsc559.team16.common.utilities.ClientServerMessage;

import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

/**
 * A thread responsible for handling user input from the command line interface.
 * <p>
 * This thread processes user input in real-time, supports command-line editing
 * via JLine, and manages the
 * message sending process with rate limiting to prevent spamming. It also
 * supports special commands like "/exit"
 * and "/quit" to gracefully shut down the client.
 * </p>
 * 
 * <p>
 * Key features of the InputThread:
 * <ul>
 * <li>Reads user input from the terminal using JLine with line editing
 * support.</li>
 * <li>Processes special commands like "/exit" and "/quit" for client
 * shutdown.</li>
 * <li>Creates and queues new messages with unique message IDs to prevent
 * duplicates.</li>
 * <li>Ensures thread-safe management of message queues with proper
 * synchronization.</li>
 * <li>Implements rate limiting for message sending with a minimum interval of
 * 100 milliseconds between messages.</li>
 * <li>Prevents duplicate message sending by tracking sent message IDs.</li>
 * <li>Tracks pending messages and queues them for acknowledgment by the
 * server.</li>
 * </ul>
 * </p>
 * 
 * <p>
 * Input Processing Flow:
 * <ol>
 * <li>Read input from terminal using JLine's line reader.</li>
 * <li>Clear input buffer after reading the line.</li>
 * <li>Check if the input matches special commands such as "/exit" or "/quit" to
 * trigger client shutdown.</li>
 * <li>If the input is not a special command, apply rate limiting to prevent
 * sending messages too quickly (100ms minimum interval).</li>
 * <li>Create a new message with the content entered by the user and a unique
 * message ID.</li>
 * <li>Add the new message to the pending messages set and to the awaiting
 * acknowledgment and message queues.</li>
 * <li>Update the last message timestamp to enforce rate limiting and avoid
 * sending too many messages in a short time.</li>
 * </ol>
 * </p>
 * 
 * <p>
 * This thread also ensures that only one message is sent within the minimum
 * interval, preventing rapid, repeated submissions.
 * It will also gracefully handle any interruptions or errors during its
 * operation, with the ability to shut down the client
 * when appropriate.
 * </p>
 * 
 * @see ClientServerMessage
 * @see messageLock
 * @see pendingMessages
 * @see awaitingAck
 * @see messageQueue
 */
public class InputThread extends Thread {
        private final Client client;
        private final LineReader lineReader;
        private static final long MIN_MESSAGE_INTERVAL = 100; // Minimum time between messages in milliseconds
        private long lastMessageTime = 0;
        private final Set<String> sentMessageIds = Collections.synchronizedSet(new HashSet<>());

        /**
         * Constructs a new InputThread for getting user input.
         * 
         * @param lineReader The JLine LineReader instance used to read user input from
         *                   the terminal.
         */
        public InputThread(Client client, LineReader lineReader) {
            this.client = client;
            this.lineReader = lineReader;
        }

        /**
         * The main run method that continuously reads input from the user, processes
         * it, and sends messages to the server.
         * <p>
         * The method performs the following tasks:
         * <ul>
         * <li>Reads user input from the terminal using JLine.</li>
         * <li>Processes special commands like "/exit" and "/quit" to terminate the
         * client.</li>
         * <li>Applies rate limiting to prevent sending too many messages in a short
         * period.</li>
         * <li>Creates new messages with unique IDs and queues them for sending.</li>
         * <li>Manages synchronization of message queues to ensure thread-safe
         * operations.</li>
         * </ul>
         * </p>
         */
    public void run() {
        try {
            while (!client.isTerminated()) {
                while (!client.isConnected()) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                try {
                    String msgContents = lineReader.readLine();
                    if (msgContents == null) {
                        break; // EOF or interrupted
                    }

                    lineReader.getBuffer().clear();

                    if (msgContents.trim().equalsIgnoreCase("/exit") || msgContents.trim().equalsIgnoreCase("/quit")) {
                        debug(DEBUG_BASIC, "Received exit command. Shutting down client...");
                        client.shutdown();
                        return;
                    }

                    if (!msgContents.trim().isEmpty()) {
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastMessageTime < MIN_MESSAGE_INTERVAL) {
                            try {
                                Thread.sleep(MIN_MESSAGE_INTERVAL - (currentTime - lastMessageTime));
                            } catch (InterruptedException e) {
                                if (!client.isTerminated()) {
                                    debug(DEBUG_NORMAL, "Input thread interrupted during rate limiting");
                                }
                                break;
                            }
                        }

                        debug(DEBUG_DETAILED, "Processing user input: " + msgContents);
                        ClientServerMessage newMsg = client.getMessageUtils().createMessage(msgContents, "fellow clients");

                        synchronized (client.getMessageLock()) {
                            // Skip if we've already sent this message
                            if (!sentMessageIds.add(newMsg.getMessageId())) {
                                continue;
                            }

                            // Add to pending messages before sending
                            client.getPendingMessages().add(newMsg.getMessageId());

                            // Add to awaitingAck and messageQueue
                            client.getAwaitingAck().add(newMsg);
                            client.getMessageQueue().add(newMsg);
                            lastMessageTime = System.currentTimeMillis();
                        }
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        if (!client.isTerminated()) {
                            debug(DEBUG_NORMAL, "Input thread interrupted");
                        }
                        break;
                    }
                } catch (UserInterruptException e) {
                    if (!client.isTerminated()) {
                        debug(DEBUG_NORMAL, "[INPUT THREAD] Input interrupted");
                    } else {
                        debug(DEBUG_BASIC, "User interrupted input (Ctrl+C).");
                    }
                    client.shutdown();
                    break;
                } catch (Exception e) {
                    debug(DEBUG_NORMAL, "Input thread error: " + e.getMessage());
                    break;
                }
            }
        } finally {
            // Moved outside the while loop so it only fires ONCE upon thread exit
            debug(DEBUG_DETAILED, "InputThread releasing shutdown latch.");
            client.getShutdownLatch().countDown();
        }
    }

    }
