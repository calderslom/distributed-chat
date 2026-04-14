package io.github.cpsc559.team16.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.github.cpsc559.team16.common.dto.PrimaryAddress;
import io.github.cpsc559.team16.common.logging.DebugLogger;
import io.github.cpsc559.team16.common.utilities.PrimaryDiscoveryReader;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import io.github.cpsc559.team16.common.utilities.ClientServerMessage;
import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

/**
 * A client implementation for an IRC-style chat application.
 * This client supports:
 * - Connection to a chat server through an addressing server
 * - Real-time message sending and receiving with acknowledgment tracking
 * - Automatic reconnection on connection loss with exponential backoff
 * - Thread-safe message handling with separate threads for input, output,
 * sending, and receiving
 * - Interactive command-line interface with JLine terminal support
 * - Dynamic terminal resizing and display management
 * - Debug logging with configurable levels
 *
 * <h2>Key Functionalities</h2>
 * <ul>
 * <li>Client connects to an addressing server, retrieves chat server
 * information, and connects to the chat server.</li>
 * <li>Manages sending and receiving messages asynchronously with retry
 * mechanisms on failure.</li>
 * <li>Handles real-time user input, message sending, and updates the display
 * with chat history and pending messages.</li>
 * <li>Handles graceful shutdown, including cleanup of threads and
 * resources.</li>
 * <li>Provides interactive command-line interface (CLI) with JLine, allowing
 * input from users.</li>
 * <li>Logs debug messages at different levels based on a configurable debug
 * level.</li>
 * </ul>
 *
 * 
 */
// @SuppressWarnings("unused")
public class Client {
    
    // Client state
    /**
     * The username assigned to the client for display purposes in the chat
     * application.
     * This value is set when the client registers with the chat server.
     */
    private String username;
    public String getUsername() {
        return username;
    }

    /**
     * A counter that increments each time a message is sent.
     * It is used to generate unique message IDs for tracking sent messages.
     */
    private int sendCounter = 0;

    /**
     * Returns the next message ID to be used for sending messages.
     * This method increments the send counter and returns its current value.
     * 
     * @return The next message ID to be used for sending messages
     */
    public int getNextSendCounter() {
        return sendCounter++;
    }

    // Server connection information
    /**
     * The hostname or IP address of the addressing server that facilitates the
     * initial connection to the chat server.
     */
    private volatile String primaryHostAddress;
    public String getPrimaryHostAddress() {
        return primaryHostAddress;
    }

    /**
     * The port number of the addressing server used for connecting and retrieving
     * chat server information.
     */
    private volatile int asPort;
    public int getAsPort() {
        return asPort;
    }

    /**
     * Re-reads the shared discovery file to update the current known Primary.
     * Sets static variables PRIMARY host address and port for client connections.
     *
     * @return A {@link PrimaryAddress} if the shared file was found and its details were loaded; null otherwise.
     */
    public PrimaryAddress retrievePrimaryDetails() {
        try {
            PrimaryAddress details = PrimaryDiscoveryReader.readPrimaryDetails();
            if (details != null) {
                this.primaryHostAddress = details.hostAddress();
                this.asPort = details.clientPort();
                return details;
            }
        } catch (IOException e) {
            System.err.println("Config: Error reading primary addressing server discovery file: " + e.getMessage());
        }

        return null;
    }

    /**
     * Re-reads the shared discovery file to update the current known Primary.
     * Sets instance variables for PRIMARY host address and port for client connections.
     *
     * @return true if a Primary was found and its details were loaded, false otherwise.
     */
    public boolean refreshPrimaryDetails() {
        try {
            PrimaryAddress details = PrimaryDiscoveryReader.readPrimaryDetails();
            if (details != null) {
                this.primaryHostAddress = details.hostAddress();
                this.asPort = details.clientPort();
                return true;
            }
        } catch (IOException e) {
            System.err.println("Config: Error reading primary addressing server discovery file: " + e.getMessage());
        }

        // If we reach here, no primary was found. Clear old stale data.
        this.primaryHostAddress = null;
        this.asPort = -1;
        return false;
    }

    // Chat server connection
    /**
     * The socket used to establish a connection to the chat server.
     * This is where messages are sent and received during the chat session.
     */
    private Socket chatServer;
    public Socket getChatServer() {
        return chatServer;
    }
    public void setChatServer(Socket chatServer) {
        this.chatServer = chatServer;
    }

    /**
     * The input stream reader used to read messages received from the chat server.
     */
    private BufferedReader in;
    public BufferedReader getIn() {
        return in;
    }
    public void setIn(BufferedReader in) {
        this.in = in;
    }

    /**
     * The output stream writer used to send messages to the chat server.
     */
    private PrintWriter out;
    public PrintWriter getOut() {
        return out;
    }
    public void setOut(PrintWriter out) {
        this.out = out;
    }

    // Message management
    /**
     * A synchronized list that stores the complete chat history of messages
     * exchanged
     * between the client and other peers.
     */
    private final List<ClientServerMessage> msgLog = Collections.synchronizedList(new LinkedList<>());
    public List<ClientServerMessage> getMsgLog() {
        return msgLog;
    }

    /**
     * A synchronized list that holds the messages ready to be displayed in the
     * client
     * interface. These messages are selected from the complete chat history.
     */
    private final List<ClientServerMessage> displayLog = Collections.synchronizedList(new LinkedList<>());
    public List<ClientServerMessage> getDisplayLog() {
        return displayLog;
    }

    /**
     * A thread-safe queue holding the messages that are pending to be sent to the
     * chat server.
     */
    private final Queue<ClientServerMessage> messageQueue = new ConcurrentLinkedQueue<>();
    public Queue<ClientServerMessage> getMessageQueue() {
        return messageQueue;
    }

    /**
     * A thread-safe queue holding the messages that have been sent but are still
     * awaiting
     * acknowledgment from the server.
     */
    private final Queue<ClientServerMessage> awaitingAck = new ConcurrentLinkedQueue<>();
    public Queue<ClientServerMessage> getAwaitingAck() {
        return awaitingAck;
    }

    /**
     * A synchronized set that tracks the message IDs of messages that have already
     * been
     * processed by the client to prevent duplicate handling.
     */
    private final Set<String> processedMessageIds = Collections.synchronizedSet(new HashSet<>());
    public Set<String> getProcessedMessageIds() {
        return processedMessageIds;
    }

    /**
     * A synchronized set that tracks the message IDs of messages that are still
     * awaiting
     * acknowledgment from the server. This helps in ensuring messages are properly
     * acknowledged.
     */
    private final Set<String> pendingMessages = Collections.synchronizedSet(new HashSet<>());
    public Set<String> getPendingMessages() {
        return pendingMessages;
    }

    /**
     * An object used as a lock to synchronize message operations, ensuring
     * thread-safety when
     * processing messages.
     */
    private final Object messageLock = new Object();
    public Object getMessageLock() {
        return messageLock;
    }

    // Thread management
    /**
     * The thread responsible for handling user input.
     * It reads commands and messages from the user, processes them, and adds them
     * to the
     * message queue for sending.
     */
    private InputThread inputThread;

    /**
     * The thread responsible for updating the user interface.
     * It manages the display of messages, system information, and the input prompt.
     */
    private OutputThread outputThread;

    /**
     * The thread responsible for sending messages to the server.
     * It manages the queue of pending messages and ensures they are sent to the
     * server.
     */
    private SenderThread senderThread;

    /**
     * The thread responsible for receiving messages from the server.
     * It processes incoming messages and updates the chat logs and display
     * accordingly.
     */
    private ReceiverThread receiverThread;

    /**
     * The connection manager responsible for handling the connection to the
     * addressing server.
     * It manages the registration process and retrieves chat server information.
     */
    private ConnectionManager connectionManager;
    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    /**
     * The message utility class that provides methods for creating and sending
     * messages.
     * It handles message formatting, ID generation, and sending messages to the
     * server.
     */
    private MessageUtils messageUtils;
    public MessageUtils getMessageUtils() {
        return messageUtils;
    }

    /**
     * A CountDownLatch used to coordinate the shutdown of the client.
     * It ensures that all four threads (input, output, sender, receiver) complete
     * their work
     * before the client shuts down completely.
     */
    private final CountDownLatch shutdownLatch = new CountDownLatch(4);
    public CountDownLatch getShutdownLatch() {
        return shutdownLatch;
    }

    // System utilities
    /**
     * A flag that indicates whether the client should shut down.
     * Set to true when the client is being terminated.
     */
    private boolean terminate = false;
    public boolean isTerminated() {
        return terminate;
    }

    /**
     * A flag that indicates the connection state of the client.
     * It is set to true when the client is successfully connected to the chat
     * server.
     */
    private boolean isConnected = false;
    public boolean isConnected() {
        return isConnected;
    }
    public boolean setConnected(boolean isConnected) {
        this.isConnected = isConnected;
        return this.isConnected;
    }

    // UI components
    /**
     * The terminal interface used for displaying the chat interface to the user.
     * It is managed by the JLine library and provides terminal-based input/output
     * handling.
     */
    private Terminal terminal;

    /**
     * The command-line reader used for capturing user input in the terminal.
     * It allows for command-line editing and handles user input interactively.
     */
    private LineReader lineReader;

    /**
     * Constructs a new chat client with the specified configuration.
     * This constructor sets up the client with necessary information for connecting
     * to an addressing server, initializes the terminal interface, and prepares
     * the line reader for command-line input.
     * 
     * @param username   The display name for this client. This will be used as the
     *                   sender's identifier in the chat system.
     * @param terminal   The terminal interface instance to be used for rendering
     *                   the
     *                   command-line interface. It enables interactive
     *                   input/output.
     * @param lineReader The line reader instance used to capture and process user
     *                   input
     *                   from the command line. This allows the user to interact
     *                   with the
     *                   chat client.
     */
    public Client(String username, Terminal terminal, LineReader lineReader) {
        debug(DEBUG_BASIC, "Initializing client for user: " + username);
        this.username = username;
        this.terminal = terminal;
        this.lineReader = lineReader;
        this.connectionManager = new ConnectionManager(this);
        this.messageUtils = new MessageUtils(this);
    }




    /**
     * Starts the client application. This method performs the following steps:
     * <ol>
     * <li>Sets up the terminal interface for the client.</li>
     * <li>Establishes a connection to the server via the addressing server.</li>
     * <li>Registers the client with the server.</li>
     * <li>Initializes and starts the necessary threads for input, output, message
     * sending, and receiving.</li>
     * <li>Maintains the main application loop that runs until termination.</li>
     * </ol>
     * 
     * The method ensures that all components of the client application are running
     * and handles errors gracefully.
     * If any error occurs during the execution, the client will shut down properly.
     * 
     * <h2>Key Features:</h2>
     * <ul>
     * <li>Concurrent operations with separate threads for user input, message
     * sending, and message receiving.</li>
     * <li>Graceful shutdown process with cleanup of resources and thread
     * management.</li>
     * <li>Debug logging to monitor client behavior at various stages.</li>
     * </ul>
     * 
     * <h2>Exception Handling:</h2>
     * <ul>
     * <li><code>InterruptedException</code>: Triggered if the client is interrupted
     * during the main loop sleep.</li>
     * <li><code>IOException</code>: Triggered if an error occurs while closing the
     * chat server connection.</li>
     * </ul>
     * 
     * @throws InterruptedException If the thread is interrupted during sleep.
     * @throws IOException          If there is an error closing the chat server
     *                              connection.
     */
    public void run() {
        debug(DEBUG_BASIC, "Starting main execution loop...");
        terminate = false;

        // Stage 1: Discovery Phase
        int discoveryAttempts = 1;
        while (!terminate && !refreshPrimaryDetails()) {
            discoveryAttempts++;

            // Developer output
            debug(DEBUG_NORMAL, "Resolving PRIMARY addressing server network configuration... Attempt " + discoveryAttempts);
            // User output
            if (DebugLogger.getDebugLevel() < DEBUG_BASIC) {
                System.out.print("\r[STATUS] Network discovery... (Attempt " + discoveryAttempts + ") [Ctrl+C to Quit]");
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                terminate = true;
                Thread.currentThread().interrupt();
            }
        }
        if (terminate) {
            System.out.println("\n[SYSTEM] Discovery cancelled. Shutting down...");
            return;
        }
        System.out.println();
        debug(DEBUG_BASIC, "Found PRIMARY Addressing Server at " + primaryHostAddress + ":" + asPort);

        // Stage 2: Connection Phase
        try {
            connectionManager.connect();
            // Initialize and start worker threads
            debug(DEBUG_DETAILED, "Creating client threads...");
            inputThread = new InputThread(this, lineReader);
            outputThread = new OutputThread(this, lineReader);
            senderThread = new SenderThread(this);
            receiverThread = new ReceiverThread(this);

            inputThread.start();
            outputThread.start();
            senderThread.start();
            receiverThread.start();

            debug(DEBUG_DETAILED, "Client threads started successfully.");

            // Main application loop
            while (!terminate) {
                Thread.sleep(1000); // refresh rate
            }

        } catch (Exception e) {
            debug(DEBUG_BASIC, "Error in client run: " + e.getMessage());
            shutdown();
        } finally {
            try {
                if (chatServer != null) {
                    chatServer.close();
                    debug(DEBUG_NORMAL, "Chat server connection closed");
                }
            } catch (IOException e) {
                debug(DEBUG_NORMAL, "Error closing chat server: " + e.getMessage());
            }
        }
    }

    /**
     * Gracefully shuts down all client threads.
     * This method ensures that all running client threads are cleanly terminated.
     * It interrupts each thread to signal them to stop their execution, ensuring
     * that resources are properly released, and no background processes are left
     * running.
     * 
     * <p>
     * This method is called during the shutdown process to ensure that all
     * active threads are safely interrupted before the client application
     * terminates.
     * It helps avoid issues such as memory leaks, incomplete operations, or
     * unhandled exceptions in any of the worker threads.
     * </p>
     * 
     * <p>
     * The threads that are interrupted include:
     * </p>
     * <ul>
     * <li>{@code senderThread} - Handles message sending.</li>
     * <li>{@code receiverThread} - Handles receiving messages.</li>
     * <li>{@code inputThread} - Handles reading user input.</li>
     * <li>{@code outputThread} - Handles updating the user interface.</li>
     * </ul>
     * 
     * <p>
     * By interrupting the threads, we signal them to stop their tasks and exit
     * cleanly.
     * Each thread will be responsible for handling its own shutdown operations once
     * interrupted.
     * </p>
     */
    private void shutdownThreads() {
        debug(DEBUG_DETAILED, "Shutting down client threads");
        if (senderThread != null)
            senderThread.interrupt();
        if (receiverThread != null)
            receiverThread.interrupt();
        if (inputThread != null)
            inputThread.interrupt();
        if (outputThread != null)
            outputThread.interrupt();
    }

    /**
     * Performs a clean shutdown of the client application.
     * This method ensures all client resources are properly cleaned up and all
     * threads
     * are terminated before the client shuts down. It handles the following steps:
     * <ul>
     * <li>Setting the terminate flag to signal that the client should stop.</li>
     * <li>Signaling shutdown to all running threads to stop their execution.</li>
     * <li>Waiting for all threads to complete their tasks with a timeout (5
     * seconds).</li>
     * <li>Closing network connections (e.g., chat server connection).</li>
     * <li>Cleaning up other resources such as input/output streams and
     * terminal.</li>
     * </ul>
     *
     * <p>
     * The shutdown process utilizes a {@link CountDownLatch} to coordinate the
     * termination
     * of threads, ensuring they finish their operations before the client
     * application fully shuts down.
     * </p>
     * 
     * <p>
     * If any thread fails to terminate within the specified timeout, the shutdown
     * will proceed
     * with a forced exit. This prevents the client from hanging indefinitely due to
     * unresponsive threads.
     * </p>
     *
     * <p>
     * Steps involved in this process:
     * </p>
     * <ol>
     * <li>Set the {@code terminate} flag to {@code true} to signal all threads to
     * stop.</li>
     * <li>Interrupt all running threads using {@link #shutdownThreads()}.</li>
     * <li>Wait for all threads to finish with a timeout of 5 seconds using the
     * {@code shutdownLatch}.</li>
     * <li>Close all network connections (chat server and input/output streams) and
     * cleanup resources.</li>
     * <li>If any resources cannot be cleaned up, handle the errors gracefully and
     * log them.</li>
     * </ol>
     * 
     * <p>
     * Note: The method gracefully handles interruptions and errors during shutdown
     * to ensure
     * that no resources are left in an inconsistent state.
     * </p>
     * 
     * @throws InterruptedException if the current thread is interrupted while
     *                              waiting for other threads to finish
     * @throws IOException          if there is an error while closing network
     *                              connections or resources
     */
    public void shutdown() {
        debug(DEBUG_BASIC, "Initiating client shutdown");
        terminate = true;

        // Signal shutdown to all threads
        shutdownThreads();

        // Close the socket so that the receiver thread is interrupted.
        try {
            if (chatServer != null && !chatServer.isClosed()) {
                chatServer.close();
                chatServer = null;
                debug(DEBUG_DETAILED, "Socket closed to unblock receiver");
            }
        } catch (IOException e) {
            debug(DEBUG_DETAILED, "Error closing socket: " + e.getMessage());
        }

        try {
            if (lineReader != null) {
                lineReader.getTerminal().close();
            }

            // Wait for all threads to complete (with timeout)
            if (!shutdownLatch.await(5, TimeUnit.SECONDS)) {
                debug(DEBUG_NORMAL, "Shutdown timeout - forcing exit");
            }

            if (in != null) {
                in.close();
                in = null;
            }
            if (out != null) {
                out.close();
                out = null;
            }
            debug(DEBUG_NORMAL, "All resources closed");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            debug(DEBUG_NORMAL, "Shutdown interrupted");
        } catch (IOException e) {
            debug(DEBUG_NORMAL, "Cleanup error: " + e.getMessage());
        }
    }


    /**
     * Main entry point for the client application.
     * <p>
     * This method performs the following tasks:
     * <ul>
     * <li>Initializes the terminal interface for user interaction using JLine.</li>
     * <li>Prompts the user to enter a username. If the user fails to provide one,
     * the default username "Anonymous" is used.</li>
     * <li>Clears the terminal screen after the username input to prepare for the
     * chat application.</li>
     * <li>Reads environment variables to configure the client:
     * <ul>
     * <li>SERVER_ADDRESS: Specifies the hostname of the addressing server (defaults
     * to "localhost").</li>
     * <li>SERVER_PORT: Specifies the port of the addressing server (defaults to
     * 49800).</li>
     * <li>CLIENT_DEBUG_LEVEL: Specifies the level of debug output (0-5, defaults to
     * 0).</li>
     * </ul>
     * </li>
     * <li>Logs the client configuration using the debug level specified in the
     * environment variables.</li>
     * <li>Instantiates and runs the client application with the provided
     * configuration.</li>
     * </ul>
     * </p>
     * 
     * Features:
     * <ul>
     * <li>Interactive username prompt with JLine support for reading input from the
     * terminal.</li>
     * <li>If the user does not provide a username, it defaults to "Anonymous".</li>
     * <li>The terminal is cleared after username input to clean up the screen
     * before starting the chat application.</li>
     * <li>Configurable debug logging based on the provided environment variables,
     * helping track the application's state.</li>
     * </ul>
     * 
     * @param args Command line arguments (not used in this implementation).
     */
    public static void main(String[] args) {
        String debugEnv = System.getenv().getOrDefault("CLIENT_DEBUG_LEVEL", "0");
        int debugLevel = Integer.parseInt(debugEnv);
        setDebugLevel(debugLevel);
        debug(DEBUG_BASIC, "DEBUG SYSTEM INITIALIZED TO LEVEL: " + debugLevel);

        // Initialize terminal for username input
        Terminal terminal = null;
        LineReader lineReader = null;
        String username = "Anonymous"; // Default username
        try {
            terminal = TerminalBuilder.builder().system(true).build();
            lineReader = LineReaderBuilder.builder().terminal(terminal).build();

            // Print splash and prompt the user for their username
            System.out.println("================================================================");
            System.out.println("  DISTRIBUTED CHAT SYSTEM v1.0.4 - [Client Node]                ");
            System.out.println("================================================================");

            try {
                System.out.print("Initializing Chat Client...");
                Thread.sleep(400);
                System.out.println(" DONE");

                System.out.print("\nPlease enter your username: ");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            username = lineReader.readLine().trim();

            // If no username is provided, fall back to "Anonymous"
            if (username.isEmpty()) {
                username = "Anonymous";
            }
            // Clear the terminal after username input
            terminal.writer().print("\033[H\033[2J");
            terminal.writer().flush();
        } catch (Exception e) {
            debug(DEBUG_BASIC, "Error reading username, using default: Anonymous");
        }

        debug(DEBUG_NORMAL, String.format("Client configuration - Username: %s", username));

        // Instantiate and run the client with the provided configuration
        Client client = new Client(username, terminal, lineReader);
        client.run();
    }



}