package io.github.cpsc559.team16.common.messaging;

/**
 * Constants representing sender and target roles used in structured network messages.
 * <p>
 * These values correspond to the {@code senderRole} and {@code targetRole} fields in
 * {@link BaseAddrServerMessage} and help determine the origin and intended recipient
 * of each message in the distributed system.
 * </p>
 */
public class Roles {
    /**
     * Represents the primary server role in the distributed network.
     * The primary coordinates the network and manages registration.
     */
    public static final String PRIMARY = "PRIMARY";

    /**
     * Represents the replica server role in the distributed network.
     * Replicas receive updates to maintain consistency with the PRIMARY. They are
     * also responsible for heartbeat pings for fault tolerance.
     */
    public static final String REPLICA = "REPLICA";

    /**
     * Represents the chat server role.
     * Chat servers handle client connections and messaging within the chat system.
     */
    public static final String CHATSERVER = "CHATSERVER";

    /**
     * Represents the client role.
     * Clients initiate requests and interact with the chat system through chat servers.
     * They connect to the chat server by sending a {@link MessageTypes#REGISTER} message
     * to the PRIMARY {@code AddressingServer}, and receiving an ACK containing the
     * Host PID, and HostAddress of an ACTIVE ChatServer with room to accept new clients.
     */
    public static final String CLIENT = "CLIENT";

    /**
     * Helper to validate if a string is a recognized role.
     */
    public static boolean isValid(String role) {
        return PRIMARY.equals(role) ||
                REPLICA.equals(role) ||
                CHATSERVER.equals(role) ||
                CLIENT.equals(role);
    }

    // Preventing any possible instantiation of the utility class
    private Roles() {}
}
