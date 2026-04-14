package io.github.cpsc559.team16.chatserver;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.github.cpsc559.team16.common.dto.ConnectionType;
import io.github.cpsc559.team16.common.utilities.BaseMessage;
import io.github.cpsc559.team16.common.utilities.ChatLog;
import io.github.cpsc559.team16.common.utilities.ClientServerMessage;
import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

/**
 * Handles all incoming messages from client connections.
 * <p>
 * Responsibilities include:
 * <ul>
 * <li>Username registration and validation</li>
 * <li>Duplicate message detection</li>
 * <li>Appending valid messages to the chat log</li>
 * <li>Broadcasting messages to all active clients</li>
 * <li>Gossiping messages to the closest peer servers for replication</li>
 * <li>Handling error responses and sending protocol-compliant error messages to
 * clients</li>
 * </ul>
 * Implements a caching mechanism to minimize recalculation of closest peers for
 * gossip.
 */
@SuppressWarnings("unused")
class ClientHandler implements ConnectionHandler {

    /**
     * A thread-safe set of message IDs that have already been processed by this
     * server.
     * <p>
     * Used to detect and prevent re-processing or rebroadcasting of duplicate
     * messages.
     * Ensures idempotency of received client messages and prevents infinite gossip
     * loops.
     * </p>
     */
    private static final Set<String> seenMessageIds = ConcurrentHashMap.newKeySet();

    /**
     * A thread-safe set of all registered usernames on this server.
     * <p>
     * Used to enforce uniqueness of usernames during the client registration phase.
     * Cleared automatically when a client disconnects.
     * </p>
     */
    private static final Set<String> registeredUsernames = ConcurrentHashMap.newKeySet();

    /**
     * A cached list of peer server PIDs considered "closest" for gossiping
     * messages.
     * <p>
     * This list is periodically refreshed to optimize peer-to-peer message
     * spreading
     * while minimizing redundant updates.
     * </p>
     */
    private static List<Long> cachedClosestPeers = new ArrayList<>();

    /**
     * Timestamp (in milliseconds) when the closest peer cache was last updated.
     */
    private static long lastClosestPeersUpdate = 0;

    /**
     * Time-to-live (TTL) for the closest peer cache.
     * <p>
     * If the cache is older than this threshold, it will be recomputed.
     * </p>
     */
    private static final long CACHE_TTL_MS = 10_000; // 10 seconds


    /**
     * Handles incoming client messages, including registration, validation, and
     * message distribution.
     * <p>
     * This method is invoked whenever a message is received from a client
     * connection.
     * It processes commands (like {@code REGISTER}), verifies client identity, logs
     * messages,
     * and broadcasts them to other clients and peer servers.
     * </p>
     *
     * <h3>Behavior:</h3>
     * <ul>
     * <li>Verifies that the message is of type {@link ClientServerMessage};
     * otherwise, it logs and exits.</li>
     * <li>If the command is {@code REGISTER}, checks whether the username is:
     * <ul>
     * <li>Non-empty</li>
     * <li>Not already taken</li>
     * </ul>
     * If valid, assigns the username to the context and confirms registration back
     * to the client.
     * </li>
     * <li>If the client sends a message before registering, responds with an
     * error.</li>
     * <li>If the message ID is already seen (duplicate), the message is skipped to
     * avoid reprocessing.</li>
     * <li>Valid messages are:
     * <ul>
     * <li>Appended to the server's {@link ChatLog}</li>
     * <li>Broadcast to all connected clients</li>
     * <li>Gossiped to one closest peer for propagation</li>
     * <li>Message ID is cached to prevent future duplication</li>
     * </ul>
     * </li>
     * </ul>
     *
     * @param message the incoming message object, expected to be of type
     *                {@link ClientServerMessage}
     * @param ctx     the connection context for the client
     * @param key     the selector key associated with the client's socket channel
     */

    public void handle(BaseMessage message, ConnectionContext ctx, SelectionKey key) {
        if (!(message instanceof ClientServerMessage msg)) {
            System.err.println("Invalid message type for CLIENT");
            return;
        }

        // Handle REGISTER command
        if ("REGISTER".equalsIgnoreCase(msg.getCommand())) {
            String desiredUsername = msg.getSender().trim();
            try {
                if (desiredUsername.isEmpty()) {
                    sendError("server", "Username cannot be empty", ctx, key);
                    return;
                }

                if (registeredUsernames.contains(desiredUsername)) {
                    sendError("server", "Username already taken", ctx, key);
                    return;
                }

                ctx.username = desiredUsername;
                registeredUsernames.add(desiredUsername);

                debug(DEBUG_BASIC, "[REGISTER] User registered: " + desiredUsername);
                WriteUtils.enqueueResponse(ctx, key, msg.toJson() + "\n");
                key.selector().wakeup();

                // Notify addressing server about new client connection
                ChatServer.notifyAddressingServerClientCount();
                return;

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if ("HISTORY".equalsIgnoreCase(msg.getCommand())) {
            int amount = 20; // default value
            try {
                amount = Integer.parseInt(msg.getContent().trim());
            } catch (NumberFormatException ignored) {
                // keep default
            }

            ChatLog chatlog = ChatServer.getChatLog();
            String messageHistory = chatlog.getLastMessagesAsString(amount);

            ClientServerMessage historyResponse = new ClientServerMessage("server", msg.getSender(), -1,
                    messageHistory);
            historyResponse.setCommand("HISTORY_RESPONSE");

            try {
                WriteUtils.enqueueResponse(ctx, key, historyResponse.toJson() + "\n");
                key.selector().wakeup();
                debug(DEBUG_NORMAL, "[HISTORY] Sent " + amount + " messages to " + msg.getSender());
            } catch (IOException e) {
                System.err.println("Failed to send HISTORY_RESPONSE: " + e.getMessage());
            }

            return;
        }

        if (ctx.type == ConnectionType.CLIENT && ctx.username == null) {
            try {
                sendError("server", "ERROR: Must register username first.\n", ctx, key);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return;
        }

        if (seenMessageIds.contains(msg.getMessageId())) {
            debug(DEBUG_NORMAL, "[CLIENT] Duplicate message skipped: " + msg.getMessageId());
            return;
        }

        debug(DEBUG_NORMAL, "[CLIENT] ID=" + msg.getId() + ", Content=" + msg.getContent());

        ChatLog chatlog = ChatServer.getChatLog();
        chatlog.appendMessage(msg);

        try {
            sendToAllClients(msg, key.selector());
            seenMessageIds.add(msg.getMessageId());
            gossipToClosestPeers(msg, 1);
        } catch (Exception e) {
            System.err.println("Failed to serialize response: " + e.getMessage());
        }
    }

    /**
     * Broadcasts a client message to all connected clients registered with the
     * selector.
     * <p>
     * Iterates through all keys registered with the given {@link Selector} and
     * identifies valid client connections.
     * For each client, the message is serialized to JSON and enqueued in their
     * write queue.
     * The selector is then woken up to ensure the message is processed promptly.
     * </p>
     *
     * <h3>Behavior:</h3>
     * <ul>
     * <li>Skips invalid selection keys or keys not associated with a
     * {@link SocketChannel}.</li>
     * <li>Filters for connections whose {@link ConnectionContext#type} is
     * {@code CLIENT}.</li>
     * <li>Uses {@link WriteUtils#enqueueResponse} to prepare the message for
     * sending.</li>
     * <li>Triggers the selector with {@code wakeup()} so the server loop handles
     * the write quickly.</li>
     * <li>Logs each successful send to a client using the {@code DEBUG_LOW_LEVEL}
     * debug level.</li>
     * </ul>
     *
     * @param msg      the message to send to all clients
     * @param selector the server's selector holding all connected channels
     */

    public static void sendToAllClients(ClientServerMessage msg, Selector selector) {
        for (SelectionKey key : selector.keys()) {
            if (!key.isValid() || !(key.channel() instanceof SocketChannel))
                continue;

            ConnectionContext ctx = (ConnectionContext) key.attachment();
            if (ctx.type == ConnectionType.CLIENT) {
                try {
                    WriteUtils.enqueueResponse(ctx, key, msg.toJson() + "\n");
                    selector.wakeup();
                    debug(DEBUG_LOW_LEVEL, "Sent message to client: " + ctx.username);
                } catch (Exception e) {
                    System.err.println("Failed to send to client: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Removes a username from the set of registered clients when they disconnect.
     * <p>
     * This method ensures that the username becomes available for future clients to
     * register.
     * It is typically called during connection teardown to maintain username
     * uniqueness.
     * </p>
     *
     * @param username the username to unregister
     */
    public static void unregisterUsername(String username) {
        registeredUsernames.remove(username);
        debug(DEBUG_BASIC, "[DISCONNECT] User unregistered: " + username);
    }

    /**
     * Returns the current count of registered usernames (active clients).
     * <p>
     * This method is used to monitor server load and report client counts to
     * the Addressing Server for load balancing purposes.
     * </p>
     *
     * @return the number of currently registered clients
     */
    public static int getRegisteredUsernameCount() {
        return registeredUsernames.size();
    }

    /**
     * Sends an error message to the client with the given message content.
     * <p>
     * Constructs a {@link ClientServerMessage} with the sender set to the server,
     * recipient set to the client, a dummy ID of {@code -1}, and the error message
     * prefixed with {@code ERROR:}. The message's command is explicitly set to
     * {@code ERROR}.
     * </p>
     * <p>
     * The message is serialized to JSON, added to the client's write queue,
     * and the selector is woken up to ensure it is sent promptly.
     * </p>
     *
     * @param sender       the identifier of the message sender (usually \"server\")
     * @param errorMessage the error text to send to the client
     * @param ctx          the client connection context
     * @param key          the selector key associated with the client's socket
     *                     channel
     * @throws JsonProcessingException if the error message cannot be serialized to
     *                                 JSON
     */
    private void sendError(String sender, String errorMessage, ConnectionContext ctx, SelectionKey key)
            throws JsonProcessingException {
        ClientServerMessage errorMsg = new ClientServerMessage(sender, "client", -1, "ERROR: " + errorMessage);
        errorMsg.setCommand("ERROR");
        WriteUtils.enqueueResponse(ctx, key, errorMsg.toJson() + "\n");
        key.selector().wakeup();
        debug(DEBUG_DETAILED, "Sent error to client: " + errorMessage);
    }

    /**
     * Gossips a client message to a specified number of closest peer servers for
     * replication.
     * <p>
     * This method is used to propagate client messages across the server network by
     * forwarding
     * them to a subset of peer servers selected as the "closest" based on PID
     * ordering.
     * It supports partial replication to reduce network overhead while maintaining
     * redundancy.
     * </p>
     *
     * <h3>Steps:</h3>
     * <ul>
     * <li>Retrieves the current {@link Selector} and list of connected peer
     * servers.</li>
     * <li>Uses {@link #getValidatedClosestPeers(int)} to select up to {@code n}
     * closest peers.</li>
     * <li>Iterates through the selector keys to find each peer's
     * {@link SelectionKey} and confirms the channel is open.</li>
     * <li>For each valid target, the message is serialized and added to its write
     * queue via {@link WriteUtils#enqueueResponse}.</li>
     * <li>Wakes up the selector to process the write promptly.</li>
     * <li>Logs the number of successful sends versus the requested count.</li>
     * </ul>
     *
     * <p>
     * If a peer's connection context or selector key is missing or invalid, that
     * peer is skipped.
     * This function tolerates transient failures and does not retry failed gossip
     * attempts.
     * </p>
     *
     * @param msg the message to be gossiped to peers
     * @param n   the number of closest peers to send the message to
     */
    public static void gossipToClosestPeers(ClientServerMessage msg, int n) {
        Selector selector = ChatServer.getSelector();
        Map<Long, ConnectionContext> peers = ChatServer.getConnectedPeers();
        List<Long> targets = getValidatedClosestPeers(n);

        debug(DEBUG_NORMAL, "[GOSSIP] Attempting to gossip to " + targets.size() + " closest peers.");
        int sent = 0;

        for (long pid : targets) {
            ConnectionContext ctx = peers.get(pid);
            if (ctx == null || ctx.type != ConnectionType.SERVER)
                continue;

            for (SelectionKey key : selector.keys()) {
                if (key.attachment() == ctx && key.channel().isOpen()) {
                    try {
                        WriteUtils.enqueueResponse(ctx, key, msg.toJson() + "\n");
                        selector.wakeup();
                        debug(DEBUG_LOW_LEVEL, "[GOSSIP] Sent message to peer PID=" + pid);
                        sent++;
                        break;
                    } catch (Exception e) {
                        System.err.println("Gossip failed to peer " + pid + ": " + e.getMessage());
                    }
                }
            }
        }

        debug(DEBUG_BASIC, "[GOSSIP] Sent to " + sent + " peers out of " + n + " requested.");
    }

    /**
     * Retrieves a list of the closest peer servers for gossiping, potentially
     * refreshing the cached list.
     * <p>
     * The method selects {@code n} closest peer servers based on the current
     * server's Process ID (PID).
     * It uses a cached list of closest peers for performance but will refresh the
     * cache if:
     * <ul>
     * <li>The cache is empty or too small</li>
     * <li>The cache has expired based on the configured {@link #CACHE_TTL_MS}
     * threshold</li>
     * <li>The cache contains stale or invalid entries</li>
     * </ul>
     * The peers are ordered by their PIDs in ascending order, and the server itself
     * is excluded from the list.
     * </p>
     *
     * <h3>Steps:</h3>
     * <ul>
     * <li>Checks if the cache needs to be refreshed based on the TTL or changes in
     * the connected peers.</li>
     * <li>If refresh is needed, creates a new list of peers, sorts them by PID, and
     * adds the current server's PID.</li>
     * <li>Excludes the server's own PID from the list and selects up to {@code n}
     * closest peers.</li>
     * <li>Updates the cache and records the timestamp of the last refresh.</li>
     * <li>If the cache is valid, returns the cached list of closest peers.</li>
     * </ul>
     * 
     * <h3>Use Case:</h3>
     * <p>
     * This function is primarily used for selecting a subset of peers to gossip
     * messages to in the network,
     * optimizing for performance and ensuring that the most relevant peers are
     * chosen based on their proximity.
     * </p>
     *
     * @param n the number of closest peers to return
     * @return a list of {@code n} closest peer server PIDs, excluding the server's
     *         own PID
     */

    private static List<Long> getValidatedClosestPeers(int n) {
        long selfPid = ChatServer.getPID();
        Map<Long, ConnectionContext> peers = ChatServer.getConnectedPeers();
        System.out.println("peers");
        System.out.println(peers);

        boolean needsRefresh = cachedClosestPeers.isEmpty()
                || cachedClosestPeers.size() < n
                || System.currentTimeMillis() - lastClosestPeersUpdate > CACHE_TTL_MS
                || !cachedClosestPeers.stream().allMatch(peers::containsKey);

        if (needsRefresh) {
            debug(DEBUG_DETAILED, "[GOSSIP] Refreshing cached closest peers list...");
            List<Long> sortedPids = new ArrayList<>(peers.keySet());
            sortedPids.add(selfPid);
            sortedPids.sort(Long::compareTo);

            int selfIndex = sortedPids.indexOf(selfPid);
            List<Long> updatedList = new ArrayList<>();
            int total = sortedPids.size();

            for (int i = 1; i < total && updatedList.size() < n; i++) {
                int index = (selfIndex + i) % total;
                long pid = sortedPids.get(index);
                if (pid != selfPid && peers.containsKey(pid)) {
                    updatedList.add(pid);
                }
            }

            cachedClosestPeers = updatedList;
            lastClosestPeersUpdate = System.currentTimeMillis();
            debug(DEBUG_LOW_LEVEL, "[GOSSIP] Updated closest peers: " + cachedClosestPeers);
        } else {
            debug(DEBUG_EXTREME, "[GOSSIP] Using cached closest peers: " + cachedClosestPeers);
        }

        return cachedClosestPeers;
    }
}
