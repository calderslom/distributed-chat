package io.github.cpsc559.team16.client;

import io.github.cpsc559.team16.common.utilities.ClientServerMessage;
import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

/**
 * A thread responsible for sending messages to the server.
 * <p>
 * This thread manages the process of sending messages from a message queue to
 * the server,
 * with features such as:
 * <ul>
 * <li>Polling the message queue every 100ms for pending messages.</li>
 * <li>Implementing automatic retries for failed message sends.</li>
 * <li>Maintaining awareness of the connection state and handling
 * disconnections.</li>
 * <li>Ensuring thread-safe management of the message queue.</li>
 * </ul>
 * </p>
 * <p>
 * The {@link SenderThread} thread performs the following tasks:
 * <ol>
 * <li>Check if the client is connected to the server.</li>
 * <li>Poll the message queue for any pending messages to send.</li>
 * <li>Send messages to the server if the client is connected.</li>
 * <li>Sleep for 100ms between attempts if the queue is empty or there is no
 * connection.</li>
 * <li>Handle any exceptions or errors that occur while sending messages.</li>
 * </ol>
 * </p>
 * 
 * <p>
 * If the connection is lost or the message queue is empty, the thread will
 * sleep for
 * longer periods (up to 1 second) to reduce the load on the system.
 * </p>
 *
 * <p>
 * The {@link SenderThread} uses the {@code messageQueue} to manage messages
 * pending for delivery.
 * The thread ensures that messages are sent in the order they were added to the
 * queue, and it retries
 * messages that failed to send. If the client is disconnected, the thread waits
 * before attempting to
 * send again.
 * </p>
 *
 * <p>
 * After the message is successfully sent or the attempt fails, the thread will
 * proceed to the next message
 * or retry the failed message, depending on the connection status and available
 * messages in the queue.
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
 * @see shutdownLatch
 * @see messageQueue
 * @see sendMessage
 */
public class SenderThread extends Thread {

    private final Client client;

    public SenderThread(Client client) {
        this.client = client;
    }

    @Override
    public void run() {
        try {
            while (!client.isTerminated()) {
                if (client.isConnected()) {
                    ClientServerMessage msg = client.getMessageQueue().poll();
                    if (msg != null) {
                        debug(DEBUG_DETAILED, "Retrying to send message from queue");
                        client.getMessageUtils().sendMessage(msg);
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        if (!client.isTerminated()) {
                            debug(DEBUG_NORMAL, "Sender thread interrupted");
                        }
                        break;
                    }
                } else {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        if (!client.isTerminated()) {
                            debug(DEBUG_NORMAL, "Sender thread interrupted");
                        }
                        break;
                    }
                }
            }
        } catch (Exception e) {
            debug(DEBUG_NORMAL, "Sender thread error: " + e.getMessage());
        } finally {
            client.getShutdownLatch().countDown();
        }
    }
}