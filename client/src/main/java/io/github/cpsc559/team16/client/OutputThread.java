package io.github.cpsc559.team16.client;

import java.util.List;

import org.jline.reader.LineReader;

import io.github.cpsc559.team16.common.utilities.ClientServerMessage;
import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

/**
 * A thread responsible for managing and updating the user interface in the
 * terminal.
 * <p>
 * This thread is responsible for rendering the message area, updating the input
 * line, and refreshing the terminal display
 * periodically. It ensures that the chat messages and the input buffer are
 * displayed correctly while maintaining the
 * message history and ensuring that pending messages are displayed with the
 * [sending...] status. The thread also handles
 * terminal resizing and adapts the layout dynamically.
 * </p>
 * 
 * <p>
 * Key Features:
 * <ul>
 * <li>Manages terminal display updates with synchronized output to ensure
 * thread-safe terminal operations.</li>
 * <li>Handles dynamic resizing of the message area based on the terminal's
 * size.</li>
 * <li>Displays chat history, including timestamps, senders, and content, while
 * limiting the display to the most recent 100 messages.</li>
 * <li>Displays pending messages with a [sending...] status for messages
 * awaiting acknowledgment.</li>
 * <li>Special formatting for system messages (INFO command), which are
 * displayed without sender information.</li>
 * <li>Ensures thread-safe management of the display and message updates using
 * synchronized collections.</li>
 * </ul>
 * </p>
 * 
 * <p>
 * Display Layout:
 * <ol>
 * <li>Header (lines 1-3):
 * <ul>
 * <li>Title of the application (Chat Client)</li>
 * <li>Connection status (Connected or Disconnected)</li>
 * <li>Separator lines</li>
 * </ul>
 * </li>
 * <li>Message Area (starting at line 4):
 * <ul>
 * <li>Most recent messages displayed first</li>
 * <li>Timestamp and sender information for each message</li>
 * <li>Special formatting for system messages (INFO command)</li>
 * </ul>
 * </li>
 * <li>Pending Messages Section:
 * <ul>
 * <li>Displays messages awaiting acknowledgment with a [sending...] status</li>
 * </ul>
 * </li>
 * <li>Input Line (bottom of the terminal):
 * <ul>
 * <li>Displays the command prompt and the current input buffer</li>
 * </ul>
 * </li>
 * </ol>
 * </p>
 * 
 * <p>
 * Thread Safety:
 * <ul>
 * <li>All display operations are synchronized to prevent concurrent access
 * issues.</li>
 * <li>Message lists (client.getDisplayLog() and client.getAwaitingAck()) are thread-safe collections to
 * ensure consistent access from multiple threads.</li>
 * <li>Terminal operations are atomic to avoid inconsistencies during UI
 * updates.</li>
 * </ul>
 * </p>
 * 
 * <p>
 * Performance:
 * <ul>
 * <li>The display only redraws when necessary (if content changes or terminal
 * dimensions change).</li>
 * <li>Maintains the display position to minimize unnecessary screen
 * updates.</li>
 * <li>Efficient message history management by limiting the display to the most
 * recent 100 messages.</li>
 * </ul>
 * </p>
 * 
 * @see ClientServerMessage
 * @see client.getDisplayLog()
 * @see client.getAwaitingAck()
 * @see terminal
 * @see inputLine
 */

public class OutputThread extends Thread {
    private final Client client;
    private final LineReader lineReader;
    private int lastDisplaySize = 0; // Track how many messages we've displayed
    private int lastAwaitingSize = 0; // Track how many messages are awaiting ACK
    private boolean needsRedraw = true; // Force a redraw if needed

    private static final int MAX_MESSAGES = 100; // Show up to this many recent messages

    public OutputThread(Client client, LineReader lineReader) {
        this.client = client;
        this.lineReader = lineReader;
    }

    /**
     * Re-draws the entire terminal screen from top to bottom, but only if
     * something has changed (new messages, etc.) or if we explicitly force a
     * redraw.
     */
    private void render() {
        synchronized (System.out) {
            // 1) Check if anything has changed
            int currentDisplaySize = client.getDisplayLog().size();
            int currentAwaitingSize = client.getAwaitingAck().size();
            boolean sizeChanged = (currentDisplaySize != lastDisplaySize
                    || currentAwaitingSize != lastAwaitingSize);

            // If nothing has changed and we don't need a forced redraw, skip.
            if (!sizeChanged && !needsRedraw) {
                return;
            }

            // 2) Clear the screen from the top
            // \033[H moves cursor to top-left
            // \033[2J clears entire screen
            System.out.print("\033[H\033[2J");
            System.out.flush();

            // 3) Print header / status
            System.out.println("\n=== Chat Room ===");
            System.out.println("Status: " + (client.isConnected() ? "Connected" : "Disconnected"));
            System.out.println("-------------------");

            // 4) Print the most recent chat messages
            List<ClientServerMessage> recentMessages;
            synchronized (client.getDisplayLog()) {
                recentMessages = client.getDisplayLog().stream()
                        .skip(Math.max(0, client.getDisplayLog().size() - MAX_MESSAGES))
                        .toList();
            }
            for (ClientServerMessage msg : recentMessages) {
                if ("INFO".equals(msg.getCommand())) {
                    // System (info) messages don't show sender
                    System.out.println(msg.getContent());
                } else {
                    String timeStr = msg.getTimeSent().toString().split(" ")[3]; // e.g. "HH:MM:SS"
                    System.out.printf("[%s] %s: %s%n", timeStr, msg.getSender(), msg.getContent());
                }
            }

            // 5) Print messages still awaiting an ACK with a special suffix
            for (ClientServerMessage msg : client.getAwaitingAck()) {
                // Skip REGISTER duplicates
                if (!"REGISTER".equals(msg.getCommand())) {
                    String timeStr = msg.getTimeSent().toString().split(" ")[3];
                    System.out.printf("[%s] %s: %s [sending...]%n", timeStr, msg.getSender(), msg.getContent());
                }
            }

            // 6) A blank line before the input prompt
            System.out.println();

            // 7) Print the input line (what the user has typed so far)
            String currentInput = lineReader.getBuffer().toString();
            System.out.print("> " + currentInput);
            System.out.flush();

            // Update counters
            lastDisplaySize = currentDisplaySize;
            lastAwaitingSize = currentAwaitingSize;
            needsRedraw = false;
        }
    }

    /**
     * Main loop: periodically render if there have been changes.
     */
    @Override
    public void run() {
        try {
            while (!client.isTerminated()) {
                try {
                    // Sleep a short time to avoid spamming the screen
                    Thread.sleep(100);
                    // Then render if needed
                    render();
                } catch (InterruptedException e) {
                    if (!client.isTerminated()) {
                        debug(DEBUG_NORMAL, "Output thread interrupted");
                    }
                    break;
                } catch (Exception e) {
                    // Minor errors can be ignored; just continue
                }
            }
        } catch (Exception e) {
            debug(DEBUG_NORMAL, "Output thread error: " + e.getMessage());
        } finally {
            client.getShutdownLatch().countDown();
        }
    }
}