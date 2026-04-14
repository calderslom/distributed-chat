package io.github.cpsc559.team16.addressingserver;

// For thread management
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import io.github.cpsc559.team16.common.dto.ServerRole;
import io.github.cpsc559.team16.common.exceptions.ConnectionClosedException;
import io.github.cpsc559.team16.common.logging.DebugLogger;
import io.github.cpsc559.team16.common.messaging.BaseAddrServerMessage;
import static io.github.cpsc559.team16.common.messaging.MessageDeserializer.deserializeMessage;
import io.github.cpsc559.team16.common.messaging.MessageTypes;
import io.github.cpsc559.team16.common.messaging.Roles;
import io.github.cpsc559.team16.common.utilities.NIOMessageChannel;

import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

/**
 * Manages the network interactions for the AddressingServer.
 * <p>
 * This class handles accepting incoming connections, registering them for
 * non-blocking I/O,
 * and dispatching read events. It utilizes a
 * {@code PersistentConnectionManager} to track
 * and manage persistent connections for chat servers and replicas.
 * </p>
 */
public class AddrServerNetworkManager {

    /**
     * Indicates whether the server has been signaled to shut down or exit its main
     * event loop.
     * <p>
     * This flag is marked {@code volatile} to ensure visibility across threads. It
     * is used by the
     * {@link AddrServerNetworkManager} to detect whether a controlled shutdown or
     * restart has been
     * requested (e.g. in response to a restart message or failure condition).
     * </p>
     * <p>
     * When set to {@code true}, the main network event loop will
     * close all connections and exit gracefully, allowing the server
     * to clean up resources and reinitialize.
     * </p>
     */
    private volatile boolean shutdownRequested = false;

    /**
     * Triggers shutdown of the network event loop.
     * Called externally by the {@code AddrServerReadDispatcher}
     * when the AddressingServer needs to exit its main loop (e.g. after a failure
     * message is received).
     */
    public void requestShutdown() {
        this.shutdownRequested = true;
        if (this.selector != null) {
            this.selector.wakeup(); // Interrupts the blocking select() immediately so the thread is not waiting for a connection.
        }
    }

    private volatile boolean midElection = false;

    public void setMidElection(boolean running) {
        this.midElection = running;
    }

    /**
     * The ServerSocketChannel that listens for incoming connection requests from
     * chat servers.
     * When a connection is accepted, a new data channel is created for
     * communicating with that chat server.
     */
    private ServerSocketChannel chatServerListenerChannel;

    /**
     * The ServerSocketChannel that listens for incoming connection requests from
     * clients.
     * When a connection is accepted, a new data channel is created for
     * communicating with that client.
     * Clients connect to this channel to be assigned to an active chat server.
     */
    // private ServerSocketChannel clientListenerChannel;

    /**
     * The ServerSocketChannel that listens for incoming connection requests from
     * replica servers.
     * This channel is used for establishing (but not maintaining) peer-to-peer
     * communication between
     * the primary addressing server and its backup replicas.
     */
    private ServerSocketChannel peerListenerChannel;

    /**
     * The ServerSocketChannel that listens for incoming TCP health check requests.
     * <p>
     * This channel responds to external health probes by sending a lightweight
     * response,
     * indicating the server is alive and accepting connections. It is not
     * registered
     * as a persistent channel and is closed immediately after replying.
     * </p>
     */
    private ServerSocketChannel healthCheckListenerChannel;

    private AddrServerReadDispatcher readDispatcher;

    /**
     * The Selector used for multiplexing non-blocking I/O operations on the
     * registered channels.
     * This allows the AddressingServer to monitor multiple channels using a single
     * thread.
     */
    private final Selector selector;

    /**
     * The network configuration for this {@code AddressingServer} process.
     */
    private final AddrServerConfig config;

    /**
     * Handles cleanup of unresponsive or failed ChatServer and AddressingServer
     * processes.
     * <p>
     * This manager provides logic to remove broken persistent connections and
     * broadcast
     * failure notifications to the remaining network. It is shared with the
     * watchdog
     * and other components that need to remove stale or disconnected processes.
     * </p>
     */
    private final ConnectionCleanupManager cleanupManager;

    /**
     * A thread-safe map of pending synchronization events that require
     * acknowledgment (ACK) messages.
     * <p>
     * Each entry corresponds to a message that was broadcast to peers or chat
     * servers and is still
     * waiting for acknowledgments from one or more recipients. This map is
     * monitored by the watchdog
     * thread to retry, finalize, or fail messages that do not receive timely
     * responses.
     * </p>
     */
    private final ConcurrentMap<Long, PendingEvent> pendingSyncEvents;

    /**
     * A background thread that monitors {@code pendingSyncEvents} for timeouts and
     * retry logic.
     * <p>
     * This watchdog resends unacknowledged messages, tracks retry counts, and
     * invokes cleanup
     * procedures for unresponsive network participants.
     * </p>
     */
    PendingEventWatchdog eventWatchdog;

    /**
     * Returns the {@link PendingEventWatchdog} responsible for monitoring retry
     * timeouts
     * and failed network events.
     *
     * @return the active {@code PendingEventWatchdog} instance for this coordinator
     */
    public PendingEventWatchdog getEventWatchdog() {
        return eventWatchdog;
    }

    /**
     * Background thread responsible for periodically requesting synchronization
     * data from the {@code PRIMARY} server.
     * <p>
     * This thread is only active on {@code REPLICA} AddressingServers. It
     * coordinates periodic sync requests to
     * retrieve up-to-date
     * {@link io.github.cpsc559.team16.common.dto.ChatServerRecord} and
     * {@link io.github.cpsc559.team16.common.dto.AddrServerRecord} entries.
     * </p>
     * <p>
     * The coordinator uses a {@link java.util.function.Supplier} to retrieve the
     * latest {@link NIOMessageChannel}
     * connected to the {@code PRIMARY}. If the channel is unavailable at startup
     * (e.g., connection has not yet been
     * established), it will wait and retry until the primary becomes reachable.
     * </p>
     * <p>
     * Once a connection is active, the coordinator will:
     * <ul>
     * <li>Send {@code ChatServerRecord} requests on a regular interval.</li>
     * <li>Send {@code AddrServerRecord} requests once every N cycles (default is
     * 6).</li>
     * </ul>
     * This component is lifecycle-managed by the {@code AddrServerNetworkManager},
     * which starts and stops the thread
     * alongside other internal systems.
     * </p>
     */
    ReplicaRequestCoordinator replicaRequestCoordinator = null;

    public ReplicaRequestCoordinator getReplicaRequestCoordinator() {
        return this.replicaRequestCoordinator;
    }

    public void createReplicaRequestCoordinator() {
        this.replicaRequestCoordinator = this.replicaRequestFactory.get();
    }

    /** Supplier for the coordinator class that handles requests sent by REPLICA processes to the PRIMARY */
    private final Supplier<ReplicaRequestCoordinator> replicaRequestFactory;

    /**
     * Terminate the {@link ReplicaRequestCoordinator} background thread and cleans up its reference.
     * <p>
     * This method signals the coordinator to stop, interrupts its current sleep or network operation,
     * and attempts to join the thread for up to 2 seconds to ensure a graceful exit. Once the
     * thread has terminated (or the timeout is reached), the local reference is nullified to
     * prevent stale interactions and allow for garbage collection.
     * </p>
     * <p>
     * This is primarily used when a {@code REPLICA} is promoted to {@code PRIMARY}, as a primary
     * server should not attempt to synchronize state with itself.
     * </p>
     */
    public void shutdownCoordinatorRequest() {
        if (this.replicaRequestCoordinator != null) {
            this.replicaRequestCoordinator.shutdown();
            try {
                // Wait up to 2 seconds for the thread to die gracefully
                this.replicaRequestCoordinator.join(2000);

                if (this.replicaRequestCoordinator.isAlive()) {
                    // We use an interrupt in repilcaRequestCoordinator.shutdown to wake it up,
                    // so if it is still alive here we must dereference it.
                    System.err.println("WARNING: ReplicaRequestCoordinator did not terminate within 2s." +
                            "Proceeding with nullification to avoid blocking the main thread.");
                }
            } catch (InterruptedException e) {
                System.err.println("Main event loop interrupted while waiting for replica request coordinator thread to die:" + e.getMessage());
                Thread.currentThread().interrupt();
            } finally {
                this.replicaRequestCoordinator = null;
            }
        }
    }

    /**
     * Constructs the network manager for the AddressingServer.
     *
     * @throws IOException If the selector fails to initialize.
     */
    public AddrServerNetworkManager(ConnectionCleanupManager cleanupManager, AddrServerConfig config,
            ConcurrentMap<Long, PendingEvent> pendingSyncEvents,
            Supplier<ReplicaRequestCoordinator> replicaRequestFactory) throws IOException {
        this.cleanupManager = cleanupManager;
        this.selector = Selector.open();
        this.config = config;
        this.pendingSyncEvents = pendingSyncEvents;
        this.eventWatchdog = new PendingEventWatchdog(pendingSyncEvents, this.cleanupManager, 500);
        this.replicaRequestFactory = replicaRequestFactory;
    }

    public void closeAllConnections() {
        System.out.println("Closing all listener channels and persistent connections...");
        // Close each listener channel
        tryCloseChannel(chatServerListenerChannel);
        tryCloseChannel(peerListenerChannel);
        tryCloseChannel(healthCheckListenerChannel);

        // Close all persistent channels that may exist for peer and chat server
        // channels
        cleanupManager.getPeerManager().getChannels().keySet().forEach(this::tryCloseChannel);
        cleanupManager.getChatServerManager().getChannels().keySet().forEach(this::tryCloseChannel);

        // Cancel selection keys and close all remaining channels registered with
        // selector
        for (SelectionKey key : selector.keys()) {
            try {
                key.cancel();
                key.channel().close();
            } catch (IOException e) {
                System.err.println("Error closing the selector-registered channel: " + e.getMessage());
            }
        }

        // Shutdown the thread pool in AddrServerReadDispatcher
        readDispatcher.shutdownExecutorService();
        try {
            eventWatchdog.shutdown();
            eventWatchdog.join(); // wait for it to finish
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore the interrupt status
            System.err.println("Interrupted while waiting for watchdog to finish.");
        }
        // Stop the request coordinator thread
        shutdownCoordinatorRequest();

        // Close the selector
        try {
            selector.close();
        } catch (IOException e) {
            System.err.println("Failed to close selector: " + e.getMessage());
        }

        System.out.println("All network resources have been closed for the Addressing Server Object.");
    }

    // Helper method to close channels quietly
    private void tryCloseChannel(Channel channel) {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException e) {
                System.err.println("Error closing channel: " + e.getMessage());
            }
        }
    }

    /**
     * Opens and binds a {@code ServerSocketChannel} to the specified port for a specific role.
     * <p>
     * This method initializes the channel in non-blocking mode and registers it with the
     * internal {@code Selector} for {@code OP_ACCEPT} events. The provided {@code connectionRole}
     * is attached to the {@code SelectionKey} for later identification during the event loop.
     * </p>
     *
     * @param port           the port number to bind the channel to
     * @param connectionRole a descriptive string (e.g., Roles.CHATSERVER) identifying the channel's purpose
     * @return the opened and registered {@code ServerSocketChannel}
     * @throws IOException if an error occurs during opening, binding, or registration
     */
    public ServerSocketChannel openListenerChannel(int port, String connectionRole) throws IOException {
        try {
            ServerSocketChannel channel = ServerSocketChannel.open();

            // Ensure the selector is not blocked during registration
            selector.wakeup();

            channel.configureBlocking(false);
            channel.socket().bind(new InetSocketAddress(port));

            // Register the channel and ATTACH the role string to the SelectionKey
            // NOTE: this will be replaced by an NIOChannel in the main event loop on the server channels
            channel.register(selector, SelectionKey.OP_ACCEPT, connectionRole);

            DebugLogger.debug(DEBUG_BASIC, connectionRole + " listener channel opened on port " + port);

            return channel;
        } catch (IOException e) {
            DebugLogger.debug(DEBUG_BASIC, "Failed to open " + connectionRole + " listener channel on port " + port + ": " + e.getMessage());
            throw e;
        }
    }



    /**
     * Opens all listener channels required for normal operation and health
     * monitoring.
     * <p>
     * This method binds and registers non-blocking {@code ServerSocketChannel}s
     * for:
     * <ul>
     * <li>Client connections (used to assign ChatServers)</li>
     * <li>Replica (peer) connections</li>
     * <li>ChatServer registration</li>
     * <li>Health check requests (used for Docker healthchecks) on port 5050</li>
     * </ul>
     * Each channel is registered with the internal {@code Selector} for accept
     * events.
     * </p>
     *
     * @param clientPort     The port used for incoming client connections.
     * @param peerPort       The port used for replica-to-primary communication.
     * @param chatServerPort The port used for ChatServer registration.
     */
    public void openListenerChannels(int clientPort, int peerPort, int chatServerPort) throws IOException {
        try {
            openListenerChannel(clientPort, Roles.CLIENT);
            this.peerListenerChannel = openListenerChannel(peerPort, Roles.REPLICA);
            this.chatServerListenerChannel = openListenerChannel(chatServerPort, Roles.CHATSERVER);
            this.healthCheckListenerChannel = openListenerChannel(5050, "HEALTH_CHECK"); // static port for health checks
        } catch (IOException e) {
            System.err.println("Failed to open a listener channel. Exiting main event loop.");
            throw e; // Throw the error so we can catch it in the main method of AddressingServer and
                     // reinitialize.
        }
    }

    /**
     * Registers a {@link SocketChannel} with the internal {@code Selector} to
     * listen for read events.
     * <p>
     * This method is used for persistent peer-to-peer or server-to-server
     * connections, allowing
     * the selector to monitor the channel for non-blocking reads via
     * {@code SelectionKey.OP_READ}.
     * The channel is also set to non-blocking mode.
     * </p>
     *
     * @param channel The {@code SocketChannel} to register for persistent read
     *                monitoring.
     * @throws IOException If an error occurs during configuration or registration.
     */
    public void openPersistentChannel(SocketChannel channel) throws IOException {
        try {
            channel.configureBlocking(false);
            channel.register(selector, SelectionKey.OP_READ);
            System.out.println("Registered persistent channel: " + channel.getRemoteAddress());
        } catch (IOException e) {
            System.err.println("Failed to register persistent channel: " + e.getMessage());
            throw e;
        }
    }

    public Selector getSelector() {
        return selector;
    }

    /**
     *
     * @param message
     * @return
     */
    public boolean isSupportedMessageType(BaseAddrServerMessage<?> message) {
        return switch (message.getMsgType()) {
            case MessageTypes.REGISTER,
                    MessageTypes.ELECTION,
                    MessageTypes.SYNCHRONIZE -> true;
            default -> false;
        };
    }

    /**
     * Begins the main event loop for the {@code AddressingServer} process by
     * listening for incoming connections on the ports defined in its instance of
     * the
     * {@link AddrServerConfig} class:
     * <ul>
     * <li>{@code config.clientPort} – used for client connection requests</li>
     * <li>{@code config.replicaPort} – used for communication with peer
     * replicas</li>
     * <li>{@code config.chatServerPort} – used for chat server registration</li>
     * </ul>
     *
     * <p>
     * This method uses a {@link Selector} to multiplex I/O across all registered
     * channels,
     * blocking until at least one event becomes available. When a connection or
     * read event
     * is detected, it dispatches the appropriate handler logic and removes the
     * processed
     * key to prevent duplicate handling.
     * </p>
     *
     * <p>
     * The event loop continues to run until a shutdown is explicitly requested via
     * {@link #requestShutdown()}. Once the shutdown flag is set, the loop exits
     * cleanly,
     * calling {@link #closeAllConnections()} to release all associated network
     * resources.
     * </p>
     *
     * @param readDispatcher the object responsible for handling all incoming
     *                       requests on persistent connections.
     *
     * @throws IOException if an I/O error occurs while selecting or processing
     *                     channel events
     */
    public void startEventLoop(AddrServerReadDispatcher readDispatcher) throws IOException {
        this.readDispatcher = readDispatcher;

        // Only start the ReplicaRequestCoordinator if this server is a REPLICA
        if (config.getRole().equals(ServerRole.REPLICA)) {
            this.createReplicaRequestCoordinator();
            if (replicaRequestCoordinator != null) {
                replicaRequestCoordinator.start();
            }
        }

        eventWatchdog.start();

        while (true) {

            
            if (shutdownRequested) {
                System.out.println("Shutdown signal received. Exiting the AddrServerNetworkManager main event loop.");
                this.closeAllConnections();
                return; // exit to calling code
            }

            if (!eventWatchdog.isAlive()) {
                System.err.println("Watchdog thread is not alive. Restarting...");
                eventWatchdog = new PendingEventWatchdog(this.pendingSyncEvents, cleanupManager, 500);
                eventWatchdog.start();
            }

            if (config.getRole().equals(ServerRole.REPLICA) && !this.midElection) {
                if (replicaRequestCoordinator != null && !replicaRequestCoordinator.isAlive()) {
                    System.err.println("ReplicaRequestCoordinator is missing or dead. Starting new instance...");
                    createReplicaRequestCoordinator();
                    replicaRequestCoordinator.start();
                }
            }

            try {

                // Any thread calling this method blocks until an event occurs on a channel
                // registered with the `selector`.
                selector.select(5000);

                // Iterate through all keys (channels) that are "ready" for an I/O operation.
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();

                    if (!key.isValid())
                        continue;

                    if (key.isAcceptable()) {
                        ServerSocketChannel listenerSC = (ServerSocketChannel) key.channel();
                        SocketChannel channel = listenerSC.accept();

                        if (channel == null)
                            continue;

                        channel.configureBlocking(false);

                        // Use the attachment to identify the "HEALTH_CHECK" role
                        if ("HEALTH_CHECK".equals(key.attachment())) {
                            try {
                                String response = this.config.getPID() + "\n";
                                ByteBuffer buffer = ByteBuffer.wrap(response.getBytes());
                                while (buffer.hasRemaining()) {
                                    channel.write(buffer);
                                }
                                channel.close();
                                DebugLogger.debug(DEBUG_EXTREME, "PONG - responded to Health Check...");
                            } catch (IOException e) {
                                DebugLogger.debug(DEBUG_BASIC, "Failed to respond to ping: " + e.getMessage());
                            }
                            continue;
                        }

                        NIOMessageChannel nioChannel = new NIOMessageChannel(channel);
                        // Attach nioChannel to the new SocketChannel's key for the handshake phase
                        channel.register(selector, SelectionKey.OP_READ, nioChannel);
                        selector.wakeup();
                    }

                    else if (key.isReadable()) {
                        SocketChannel channel = (SocketChannel) key.channel();

                        Object attachment = key.attachment();
                        NIOMessageChannel nioChannel;

                        // Case 1: this is a new connection in handshake phase (not persistent yet)
                        if (attachment instanceof NIOMessageChannel) {
                            nioChannel = (NIOMessageChannel) attachment;
                            try {
                                String firstMsg = nioChannel.receiveMessage();

                                if (firstMsg == null) {
                                    System.err.println("Connection dropped: no initial message received.");
                                    channel.close();
                                    key.cancel();
                                    continue;
                                }

                                BaseAddrServerMessage<?> message = deserializeMessage(firstMsg);
                                if (message == null || !isSupportedMessageType(message)) {
                                    System.err.println("Rejected: initial message must be REGISTER, SYNCHRONIZE, or ELECTION.");
                                    channel.close();
                                    key.cancel();
                                    continue;
                                }

                                if (message.getMsgType().equals(MessageTypes.ELECTION)) {
                                    readDispatcher.dispatchMsgType(channel, nioChannel, message);
                                    channel.close();
                                    key.cancel();
                                    continue;
                                }

                                String senderRole = message.getSenderRole();
                                // Open persistent connection and hand it to the manager class.
                                if (senderRole.equals(Roles.CHATSERVER)) {
                                    openPersistentChannel(channel); // will register with selector
                                    cleanupManager.getChatServerManager().getChannels().put(channel, nioChannel);
                                } else if (senderRole.equals(Roles.REPLICA)) {
                                    openPersistentChannel(channel);
                                    cleanupManager.getPeerManager().getChannels().put(channel, nioChannel);
                                }

                                try {
                                    // If synchronization fails, the persistent connection that was just opened is closed.
                                    // The persistent connection (NIO socket) does not contain a unique PID until
                                    // it is assigned during registration or synchronization.
                                    if (message.getMsgType().equals(MessageTypes.SYNCHRONIZE)) {
                                        if (senderRole.equals(Roles.CHATSERVER)) {
                                            readDispatcher.handleSynchronizationRequest(channel, nioChannel, message, Roles.CHATSERVER);
                                        } else if (senderRole.equals(Roles.REPLICA)) {
                                            readDispatcher.handleSynchronizationRequest(channel, nioChannel, message, Roles.REPLICA);
                                        }
                                        else { System.err.println("Received SYNCHRONIZE request from a process with an unrecognized role."); }
                                    }
                                    else {
                                        readDispatcher.handleRegistration(channel, nioChannel, message);
                                    }
                                    if (!cleanupManager.isPersistentConnection(channel)) {
                                        channel.close();
                                        key.cancel();
                                    }
                                } catch (ConnectionClosedException cce) {
                                    if (cleanupManager.isPersistentConnection(channel)) {
                                        cleanupManager.cleanupPersistentConnection(channel, true);
                                    } else {
                                        channel.close();
                                        key.cancel();
                                    }
                                } catch (Exception e) {
                                    if (cleanupManager.isPersistentConnection(channel)) {
                                        cleanupManager.cleanupPersistentConnection(channel, false);
                                    } else {
                                        channel.close();
                                        key.cancel();
                                    }
                                }

                            } catch (IOException ioe) {
                                System.err.println("IOException during handshake: " + ioe.getMessage());
                                try {
                                    channel.close();
                                } catch (IOException ignored) {
                                }
                                key.cancel();
                            }

                            continue; // skip rest — this wasn't a persistent read
                        }

                        // Case 2: persistent connection
                        nioChannel = cleanupManager.getKnownPersistentChannel(channel);
                        if (nioChannel == null) {
                            try {
                                System.err.printf(
                                        "No persistent NIO channel found for SocketChannel %s. Closing channel.\n",
                                        channel.getRemoteAddress());
                            } catch (IOException e) {
                                System.err.println("No persistent NIO channel found (remote address unavailable).");
                            }
                            key.cancel();
                            try {
                                channel.close();
                            } catch (IOException ignored) {
                            }
                            continue;
                        }

                        // Temporarily disable OP_READ to avoid re-triggering
                        key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);

                        try {
                            readDispatcher.dispatch(channel, nioChannel);

                            if (key.isValid()) {
                                key.interestOps(key.interestOps() | SelectionKey.OP_READ);
                                selector.wakeup(); // ensure changes are picked up
                            }

                        } catch (ConnectionClosedException cce) {
                            try {
                                System.err.printf("ConnectionClosedException on channel %s (interestOps=%d): %s\n",
                                        channel.getRemoteAddress(), key.interestOps(), cce.getMessage());
                            } catch (IOException e) {
                                System.err.printf("ConnectionClosedException on unknown channel: %s\n", cce.getMessage());
                            }
                            cleanupManager.cleanupPersistentConnection(channel, true);
                        } catch (IOException ioe) {
                            try {
                                System.err.printf("IOException on channel %s (interestOps=%d): %s\n",
                                        channel.getRemoteAddress(), key.interestOps(), ioe.getMessage());
                            } catch (IOException e) {
                                System.err.printf("IOException on unknown channel: %s\n", ioe.getMessage());
                            }
                            cleanupManager.cleanupPersistentConnection(channel, false);
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Exception in main event loop: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}