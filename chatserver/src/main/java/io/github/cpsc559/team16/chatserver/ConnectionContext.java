package io.github.cpsc559.team16.chatserver;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;

import io.github.cpsc559.team16.common.dto.ConnectionType;

/**
 * Holds the connection context for each client or peer server connection.
 * <p>
 * This class stores various attributes and state information for a specific
 * connection,
 * such as the {@link SocketChannel}, message buffers, connection type, and
 * client-specific details like
 * username and peer ID. It is used to track the state of the connection and
 * manage message I/O.
 * </p>
 *
 * <h3>Attributes:</h3>
 * <ul>
 * <li><strong>socketChannel:</strong> The {@link SocketChannel} associated with
 * the connection.</li>
 * <li><strong>type:</strong> The type of the connection (client or peer
 * server). This is determined by the
 * connection context type in {@link ConnectionType}.</li>
 * <li><strong>readBuffer:</strong> A buffer used for reading incoming data from
 * the socket channel.</li>
 * <li><strong>writeQueue:</strong> A queue of {@link ByteBuffer} objects that
 * hold outgoing messages to be written
 * to the socket channel.</li>
 * <li><strong>partialData:</strong> A {@link StringBuilder} that stores
 * incoming data that may be incomplete,
 * such as JSON messages being received in chunks.</li>
 * <li><strong>peerID:</strong> The unique process ID of the connected peer
 * (used in the peer-to-peer communication).
 * A default value of {@code -1} indicates no peer ID has been assigned
 * yet.</li>
 * <li><strong>username:</strong> The username associated with the client
 * connection, if available.</li>
 * <li><strong>host:</strong> The IP address or hostname of the connected
 * peer.</li>
 * <li><strong>port:</strong> The port used to establish the connection with the
 * peer.</li>
 * <li><strong>lastActivityTime:</strong> The timestamp of the last activity on
 * the connection. This helps track
 * when the connection was last used for heartbeat checks or timeouts.</li>
 * <li><strong>awaitingPong:</strong> A flag indicating whether the server is
 * waiting for a {@code PONG} message from the peer
 * as part of the heartbeat protocol.</li>
 * <li><strong>missedPongs:</strong> A counter that tracks the number of missed
 * {@code PONG} messages received from the peer.</li>
 * </ul>
 *
 * <h3>Constructor:</h3>
 * <p>
 * The constructor takes a {@link SocketChannel} and initializes the connection
 * context, setting the initial values
 * for attributes like {@code lastActivityTime}.
 * </p>
 *
 */

public class ConnectionContext {

    public final SocketChannel socketChannel;
    public ConnectionType type;
    public ByteBuffer readBuffer = ByteBuffer.allocate(4096);
    public Queue<ByteBuffer> writeQueue = new LinkedList<>();
    public StringBuilder partialData = new StringBuilder();

    public long peerPID = -1;
    public String username;

    public String host; // IP or hostname
    public int port; // Port used to connect

    public long lastActivityTime = System.currentTimeMillis();
    public boolean awaitingPong = false;
    public int missedPongs = 0;

    // Connections are attempted up to 3 times before a failure message is generated
    public int retryCount = 0;
    public static final int MAX_RETRIES = 3;

    public volatile boolean needsClosing = false;

    public ConnectionContext(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
        this.lastActivityTime = System.currentTimeMillis();

    }
}