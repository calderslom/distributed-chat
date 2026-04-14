package io.github.cpsc559.team16.common.utilities;

import io.github.cpsc559.team16.common.exceptions.ConnectionClosedException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A utility class that simplifies reading and writing messages over a SocketChannel.
 * It mimics linking a {@code PrintWriter} to a traditional socket for output streams,
 * and using a {@code BufferedReader} for input streams.
 * <p>It also tracks the unique ID (PID) of the network process associated with this channel</p><p>
 * This class ensures that messages are read and written correctly, even when fragmented across multiple
 * network transmissions by using structured message boundaries and persistent buffering.
 * </p><p>
 * This class automatically handles:
 * <ul>
 *     <li>Encoding and decoding messages using UTF-8</li>
 *     <li>Message framing using a newline (`\n`) delimiter</li>
 *     <li>Buffer allocation</li>
 *     <li>Flipping a {@code ByteBuffer}'s state between writing (to the network) and reading (from the network)</li>
 *     <li>Accumulating incomplete messages in a {@code StringBuilder} until they are fully received</li>
 *     <li>Persisting unprocessed data across multiple reads when messages arrive in fragments</li>
 * </ul>
 * <p><strong>NOTE:</strong> To reiterate, this class assumes that the client process adheres to the
 * messaging protocol of ending each message with a newline delimiter (`\n`). Messages that do not
 * follow this protocol may not be correctly framed or processed.</p>
 *
 */
public class NIOMessageChannel {
    /**
     * The underlying {@link SocketChannel} used for network communication.
     * <p>
     * This channel represents an active connection between two network processes and is used
     * for sending and receiving data in a non-blocking manner. The {@link NIOMessageChannel}
     * interacts with this channel to facilitate efficient data transfer.
     * </p>
     */
    private final SocketChannel channel;



    /**
     * The unique identifier of the server associated with this channel.
     * 0L represents "not yet registered" or "uninitialized" in the system —
     * i.e. it is a placeholder PID that no valid process will ever use.
     * <p>
     *     Using 0L as a sentinel value avoids the case of null-pointer exceptions,
     *     while providing the same function of denoting an unregistered process/channel.
     * </p>
     */
    private Long serverPID = 0L;


    /**
     * A {@link ByteBuffer} used as an intermediate storage buffer for network data.
     * <p>
     * This buffer temporarily holds incoming raw bytes read from the {@code SocketChannel}.
     * It ensures efficient reading of network packets by accumulating data until a
     * full message is available for processing.
     * </p>
     */
    private final ByteBuffer streamBuffer;

    /**
     * A {@link StringBuilder} used for accumulating and reconstructing messages.
     * <p>
     * Since TCP is a stream-based protocol, messages may arrive fragmented across multiple
     * read events. This buffer ensures that incomplete messages are stored and correctly
     * concatenated until a complete message (delimited by {@code \n}) is received.
     * </p>
     */
    private final StringBuilder messageBuffer = new StringBuilder();

    /**
     * Creates a new NIOMessageChannel for a given SocketChannel.
     *
     * @param channel The SocketChannel to wrap.
     */
    public NIOMessageChannel(SocketChannel channel) {
        this.channel = channel;
        this.streamBuffer = ByteBuffer.allocate(1024);  // Adjustable buffer size
    }



    /**
     * Retrieves the {@code SocketChannel} being managed by this instance of
     * {@link NIOMessageChannel}. This allows a developer to
     * store a single NIOMessageChannel in a data structure, while
     * retaining access to both.
     * @return channel The {@code SocketChannel} assigned to this instance of {@code NIOMessageChannel}
     */
    public SocketChannel getSocketChannel() {
        return this.channel;
    }

    /**
     * Sends a message over the network.
     * <p>
     * This method automatically appends a newline (`\n`) to the message to act as a delimiter.
     * </p>
     *
     * @param message The string message to send.
     * @throws IOException If an I/O error occurs.
     */
    public void sendMessage(String message) throws IOException {
        String framedMessage = message + "\n";  // Append newline as a delimiter
        ByteBuffer sendBuffer = ByteBuffer.wrap(framedMessage.getBytes(StandardCharsets.UTF_8));
        while (sendBuffer.hasRemaining()) {
            this.channel.write(sendBuffer);
        }
    }

    /**
     * Reads available data from the {@code SocketChannel} into the stream buffer and appends it to the message buffer.
     * <p>
     * This method ensures that incoming data is read from the network and stored for later processing.
     * It does not process or extract messages but rather fills the buffer with any new data received.
     * </p>
     *
     * <p><strong>Data Persistence:</strong></p>
     * <ul>
     *     <li>If a message is incomplete, the remaining data stays in {@code messageBuffer} until a full message is received.</li>
     *     <li>If no data is available, the method avoids unnecessary processing.</li>
     * </ul>
     *
     * @return {@code true} if new data was read and appended to the buffer, {@code false} if no data was read
     *         (or if the connection was closed).
     * @throws IOException If an I/O error occurs while reading from the channel.
     */
    public boolean fillMessageBuffer() throws IOException {
        int bytesRead = this.channel.read(streamBuffer);
        // NOTE - with NIO sockets, a channel can be closed, and the key will remain. We must remove
        // the closed channel from any data structure it is stored in, and remove the key from the selector.
        if (bytesRead == -1) {
            System.out.println("WARNING: Connection closed by remote host. Throwing exception...");
            throw new ConnectionClosedException("Remote peer closed the connection.");
        } else if (bytesRead == 0) {
            return false;  // No new data available, avoid unnecessary processing
        }
        streamBuffer.flip();
        String receivedData = StandardCharsets.UTF_8.decode(streamBuffer).toString();
        streamBuffer.clear();  // Clear the ByteBuffer (not the accumulated message buffer)
        // Append new data to our message buffer
        messageBuffer.append(receivedData);
        return true;
    }

    public String checkMessageBuffer() throws IOException {
        int newlineIndex = messageBuffer.indexOf("\n");
        if (newlineIndex != -1) {
            String completeMessage = messageBuffer.substring(0, newlineIndex).trim();
            messageBuffer.delete(0, newlineIndex + 1);
            return completeMessage;
        }
        return null;
    }

    /**
     * Reads a message from the network.
     * <p>
     * This method accumulates data until a full message (ending in `\n`) is received.
     * </p>
     * <p>If data exists on the channel, it will continue looping until
     * it exhausts that data, or retrieves a message whichever comes first.
     * A message here meaning: a sequence of characters ending with a newline `\n` character.
     * <strong>NOTE:</strong>If you read from an empty channel (before the message arrives) it will just return null.
     * This does not operate in a "blocking" manner like regular Sockets</p>
     *
     * @return The received message as a string, or {@code null} if no complete message is available.
     * @throws IOException If an I/O error occurs.
     */
    public String receiveMessage() throws IOException {
        while (true) {
            /* First, we must check if the accumulated data already contains a complete message.
               This will occur if the previous call to receiveMessage retrieved MORE than one message -
               because receiveMessage returns the first complete message it finds, even if messageBuffer
               contains more than one.
             */
            int newlineIndex = messageBuffer.indexOf("\n");
            if (newlineIndex != -1) {
                String completeMessage = messageBuffer.substring(0, newlineIndex).trim();
                messageBuffer.delete(0, newlineIndex + 1);
                return completeMessage;
            }
            // Attempt to read more data into the buffer.
            if (!fillMessageBuffer()) {
                // No new data available; exit the loop, even if there's a partial message.
                return null;
            }
        }
    }


    /**
     * Closes the {@code SocketChannel} being managed by this instance of
     * {@link NIOMessageChannel}, terminating the persistent I/O connection
     * between networked processes.
     * <p>
     * Once closed, this instance can no longer send or receive messages,
     * and any attempts to interact with either channel will result in an error.
     * </p>
     * <p>
     * <strong>NOTE:</strong> After calling this method, the reference to this
     * {@code NIOMessageChannel} instance should be discarded to avoid unintended use
     * of a closed channel.
     * </p>
     */
    public void closeSocketChannel() {
        try {
            channel.close();
        } catch (IOException ignore) { } // Our goal is to close the channel. If there is an error -> it's closed.
    }

    /**
     * Returns the process ID (PID) associated with the remote server connected through this channel.
     * <p>
     * A PID of {@code 0L} indicates that the remote server has not yet been assigned
     * a unique identifier by the primary {@code AddressingServer}.
     * </p>
     *
     * @return the PID of the remote server; {@code 0L} if unregistered.
     */
    public Long getServerPID() {
        return serverPID;
    }

    /**
     * Assigns a unique process ID (PID) to the remote server connected through this channel.
     * <p>
     * This is typically called during the registration process, once the primary
     * {@code AddressingServer} has issued a valid PID to the remote server.
     * </p>
     *
     * @param pid the PID to associate with this channel's remote server.
     */
    public void setServerPID(Long pid) {
        this.serverPID = pid;
    }

    /**
     * Checks if the underlying SocketChannel is open and connected.
     * * @return {@code true} if the channel is open, {@code false} otherwise.
     */
    public boolean isOpen() {
        return this.channel != null && this.channel.isOpen() && this.channel.isConnected();
    }

}
