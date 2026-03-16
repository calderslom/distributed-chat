package io.github.cpsc559.team16.chatserver;

import io.github.cpsc559.team16.common.dto.AddrServerRecord;
import io.github.cpsc559.team16.common.dto.ChatServerRecord;
import io.github.cpsc559.team16.common.dto.ConnectionType;
import io.github.cpsc559.team16.common.dto.PrimaryAddress;
import io.github.cpsc559.team16.common.messaging.BaseAddrServerMessage;
import io.github.cpsc559.team16.common.messaging.RegisterMessage;
import io.github.cpsc559.team16.common.messaging.Roles;
import io.github.cpsc559.team16.common.utilities.NetworkManager;
import io.github.cpsc559.team16.common.utilities.NetworkUtils;
import io.github.cpsc559.team16.common.utilities.PrimaryDiscoveryReader;

import static io.github.cpsc559.team16.common.utilities.DebugLogger.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link NetworkManager} for the Chat Server.
 * This handles incoming client connections and communication with other chat servers.
 */
public class ChatServerNetworkManager implements NetworkManager {
    private final Selector selector;

    public Map<ConnectionType, ConnectionHandler> getHandlerMap() {
        return handlerMap;
    }

    /**
     * Maps each {@link ConnectionType} to its respective handler implementation.
     * <p>
     * For example, {@code CLIENT} maps to {@link ClientHandler}.
     * </p>
     */
    private final Map<ConnectionType, ConnectionHandler> handlerMap = new HashMap<>();



    public String getPrimaryHostAddress() {
        return primaryHostAddress;
    }

    public void setPrimaryHostAddress(String primaryHostAddress) {
        this.primaryHostAddress = primaryHostAddress;
    }

    /**
     * Host address of the Addressing Server.
     * Retrieved dynamically using the {@link PrimaryDiscoveryReader}
     */
    private volatile String primaryHostAddress;


    /**
     * Port on which the Addressing Server is listening for ChatServer connections.
     * Retrieved dynamically using the {@link PrimaryDiscoveryReader}
     */
    private volatile int asPort;

    public ChatServerNetworkManager() throws IOException {
        this.selector = Selector.open();
    }

    @Override
    public Selector getSelector() {
        return selector;
    }

    @Override
    public ServerSocketChannel openListenerChannel(int port) throws IOException {
        ServerSocketChannel channel = ServerSocketChannel.open();
        channel.configureBlocking(false);
        channel.socket().bind(new InetSocketAddress(port));
        channel.register(selector, SelectionKey.OP_ACCEPT);
        return channel;
    }

    @Override
    public void openPersistentChannel(SocketChannel channel) throws IOException {
        channel.register(selector, SelectionKey.OP_READ);
    }

    @Override
    public void startEventLoop(ReadDispatcher dispatcher2) throws IOException {

    }

    /**
     * Triggers a state synchronization handshake with a newly promoted PRIMARY.
     * <p>
     * This is the critical recovery entry point used during leader failover. Unlike a
     * standard registration, this method informs the new PRIMARY of this process's
     * existing state to maintain cluster consistency without losing local history.
     * </p>
     *
     * @param record The {@link AddrServerRecord} containing the network coordinates
     *               of the new PRIMARY.
     * @return {@code true} if the synchronization message was successfully queued
     * and the channel registered with the NIO {@code Selector}.
     * @see #initiatePrimaryHandshake(BaseAddrServerMessage, AddrServerRecord)
     */
    public boolean synchronizeWithPrimary(AddrServerRecord record) {
        // Logic to fetch local PID and wrap it in a SyncRegisterMessage
        // ...
        return false;
    }

    /**
     * Executes a resilient network handshake with the PRIMARY Addressing Server.
     * <p>
     * This method unifies connection logic for both initial boots and mid-lifecycle
     * failovers. It implements an exponential backoff retry strategy to handle
     * Docker network stabilization delays and uses the discovery filesystem as a
     * secondary source of truth if the provided {@code knownHost} fails.
     * </p>
     *
     * @param handshakeMsg The specific message (Register or Sync) to transmit.
     * @param knownHost    The targeted PRIMARY host. If null, discovery is performed.
     * @return {@code true} if a persistent NIO channel is successfully established.
     */
    public boolean initiatePrimaryHandshake(BaseAddrServerMessage<AddrServerRecord> handshakeMsg,
                                            AddrServerRecord knownHost) {
        // Retry loop, transmitDiscoveryMessage, and Selector registration
        // ...
        return false;
    }


    /**
     * Checks if there are any existing connections to the Addressing Server.
     * <p>
     * This method prevents creating duplicate connections to the Addressing Server,
     * which could cause conflicting PIDs and state inconsistencies.
     * </p>
     *
     * @return true if an existing addressing server connection is found, false
     *         otherwise
     */
    private boolean hasExistingAddressingServerConnection() {
        for (SelectionKey key : selector.keys()) {
            if (!key.isValid())
                continue;

            Object attachment = key.attachment();
            if (attachment instanceof ConnectionContext) {
                ConnectionContext ctx = (ConnectionContext) attachment;

                if (ctx.type == ConnectionType.ADDRESSING_SERVER) {
                    debug(DEBUG_DETAILED, "Found existing Addressing Server connection.");
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Attempts to reconnect to the Addressing Server after a connection failure.
     * <p>
     * This method is triggered when a connection to the Addressing Server is lost
     * due to
     * network failure, shutdown, or other I/O issues. It tries to re-establish
     * a connection to the Addressing Server and register this chat server again.
     * </p>
     *
     * <h3>Reconnection Strategy:</h3>
     * <ul>
     * <li>Performs up to 10 reconnection attempts, spaced 3 seconds apart.</li>
     * <li>For each attempt:
     * <ul>
     * <li>Opens a new {@link SocketChannel} in non-blocking mode</li>
     * <li>Connects to the Addressing Server's known address</li>
     * <li>Creates a new {@link ConnectionContext} and registers it for
     * {@code OP_CONNECT}</li>
     * <li>Sends a REGISTER message to re-establish this chat server's presence</li>
     * </ul>
     * </li>
     * <li>If all attempts fail, a message is logged and the server continues
     * operating
     * with existing peer connections, but new client connections will not be
     * possible.</li>
     * </ul>
     */
    private static void attemptReconnectingToAddressingServer() {
        debug(DEBUG_BASIC, "Attempting reconnection to Addressing Server...");

        // First, check if we already have any active addressing server connections
        if (hasExistingAddressingServerConnection()) {
            debug(DEBUG_BASIC, "An existing Addressing Server connection was found. Aborting reconnection attempt.");
            return;
        }

        final int MAX_RETRIES = 10;
        final int RETRY_DELAY_MS = 3000;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                debug(DEBUG_NORMAL, "Addressing Server reconnection attempt " + attempt);

                // Create a new connection to the addressing server
                SocketChannel channel = SocketChannel.open();
                channel.configureBlocking(false);
                channel.connect(new InetSocketAddress(primaryHostAddress, asPort));

                ConnectionContext ctx = new ConnectionContext(channel);
                ctx.type = ChatServer.ConnectionType.ADDRESSING_SERVER;

                // Create a new registration message
                String publicAddress = System.getenv("PUBLIC_ADDRESS");
                // Add a fallback if the environment variable isn't set
                if (publicAddress == null || publicAddress.isEmpty()) {
                    try {
                        InetAddress localHost = InetAddress.getLocalHost();
                        publicAddress = localHost.getHostAddress();
                        System.out.println(
                                "WARNING: PUBLIC_ADDRESS not set in environment, using detected address: "
                                        + publicAddress);
                    } catch (IOException ioe) {
                        publicAddress = "localhost";
                        System.out.println("Failed to get local host address, using localhost");
                    }
                } else {
                    System.out.println("Using PUBLIC_ADDRESS from environment: " + publicAddress);
                }

                RegisterMessage<ChatServerRecord> registrationMsg = RegisterMessage.fromChatServer(
                        publicAddress,
                        CLIENT_PORT,
                        PEER_LISTEN_PORT,
                        asPort,
                        MAX_CLIENTS);

                String json = registrationMsg.toJson() + "\n";
                ctx.writeQueue.add(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));

                channel.register(selector, SelectionKey.OP_CONNECT, ctx);
                debug(DEBUG_BASIC, "Reconnection attempt to Addressing Server initiated");

                // Wake up the selector to process this connection immediately
                selector.wakeup();
                return;
            } catch (IOException e) {
                debug(DEBUG_NORMAL, "Reconnection attempt " + attempt + " failed: " + e.getMessage());
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    debug(DEBUG_NORMAL, "Reconnection retry sleep interrupted");
                    break;
                }
            }
        }

        debug(DEBUG_BASIC,
                "All reconnection attempts to Addressing Server failed. The Chat Server will continue to operate but may have limited functionality.");
    }

    /**
     * Initiates a non-blocking registration flow to the central Addressing Server.
     * <p>
     * This server must register with the Addressing Server to receive a unique PID
     * (Process ID)
     * and a list of currently active peer servers. This registration is performed
     * by sending
     * a {@link BaseAddrServerMessage} of type {@code REGISTER}, which includes
     * metadata
     * such as IP address, port numbers, and client capacity.
     * </p>
     * <p>
     * The connection is configured in non-blocking mode and registered with the
     * selector
     * using {@code OP_CONNECT}, so that it can be completed during the selector's
     * event loop.
     * The registration message is queued in the
     * {@link ConnectionContext#writeQueue} and will be
     * transmitted upon a writable event.
     * </p>
     *
     * @param selector the selector managing the main event loop; used to register
     *                 the channel
     */
    private void registerWithAddressingServer(Selector selector) {
        // Stage 1: Discovery Phase
        int discoveryAttempts = 0;
        while (!this.retrievePrimaryDetails()) {
            discoveryAttempts++;
            debug(DEBUG_BASIC, "Waiting for Addressing Server network details (Attempt " + discoveryAttempts + ")...");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        debug(DEBUG_BASIC, "Found PRIMARY Addressing Server at " + primaryHostAddress + ":" + asPort);

        // Stage 2: Connection Phase
        try {
            SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(primaryHostAddress, asPort));

            ConnectionContext ctx = new ConnectionContext(channel);
            ctx.type = ChatServer.ConnectionType.ADDRESSING_SERVER;

            // NOTE: Use the factory methods in RegisterMessage for registering a process.
            // These ensure all syntax is correct and help with bug tracking.

            // Save registration payload in ctx (optional, in case needed later)
            // ChatServerRecord record = new ChatServerRecord(
            // 0L,
            // InetAddress.getLocalHost().getHostAddress(),
            // CLIENT_PORT,
            // PEER_LISTEN_PORT,
            // ADDRESSING_SERVER_PORT,
            // MAX_CLIENTS);

            // BaseAddrServerMessage<ChatServerRecord> registrationMsg = new
            // BaseAddrServerMessage<>(
            // "REGISTER", "ChatServerRecord", 0L, "CHATSERVER", "PRIMARY", record);
            // Retrieve hostname dynamically.
            String publicAddress = NetworkUtils.getSerializedIdentity(Roles.CHATSERVER);
            // This creates a registration message and a properly formed chat server record
            // all in one.
            RegisterMessage<ChatServerRecord> registrationMsg = RegisterMessage.fromChatServer(publicAddress,
                    CLIENT_PORT,
                    PEER_LISTEN_PORT,
                    asPort,
                    MAX_CLIENTS);

            String json = registrationMsg.toJson() + "\n";
            ctx.writeQueue.add(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));

            channel.register(selector, SelectionKey.OP_CONNECT, ctx);
            debug(DEBUG_BASIC, "Initiated non-blocking registration to Addressing Server");
        } catch (IOException e) {
            debug(DEBUG_BASIC, "Failed to connect to Addressing Server: " + e.getMessage());
        }
    }

    private static String getThisDockerAddress() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            System.out.println("[Replica] Could not retrieve internal Docker address - defaulting to 'localhost'");
            return "localhost";
        }
    }

    /**
     * Re-reads the shared discovery file to update the current known Primary.
     * Sets static variables {@code primaryHostAddress} and {@code asPort}
     * PRIMARY host address and port for client connections for the ChatServer.
     *
     * @return A {@link PrimaryAddress} if the shared file was found and its details were loaded; null otherwise.
     */
    private boolean retrievePrimaryDetails() {
        try {
            PrimaryAddress details = PrimaryDiscoveryReader.readPrimaryDetails();
            if (details != null) {
                this.setPrimaryHostAddress(details.hostAddress());
                asPort = details.clientPort();
                return true;
            }
        } catch (IOException e) {
            System.err.println("Config: Error reading primary addressing server discovery file: " + e.getMessage());
        }

        return false;
    }
}
