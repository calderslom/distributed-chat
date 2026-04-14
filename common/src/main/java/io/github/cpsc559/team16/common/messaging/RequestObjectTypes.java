package io.github.cpsc559.team16.common.messaging;

/**
 * Defines string constants used to identify the type of data being requested
 * in a synchronization message (e.g., from a replica to the primary AddressingServer).
 * <p>
 * These identifiers help the receiving server interpret the request and respond
 * with the appropriate set or individual record.
 */
public class RequestObjectTypes {

    /**
     * Request to retrieve all {@link io.github.cpsc559.team16.common.dto.AddrServerRecord} entries
     * from the primary AddressingServer.
     */
    public static final String ADDR_SERVER_RECORDS = "ADDR_SERVER_RECORDS";

    /**
     * Request to retrieve all {@link io.github.cpsc559.team16.common.dto.ChatServerRecord} entries
     * from the primary AddressingServer.
     */
    public static final String CHAT_SERVER_RECORDS = "CHAT_SERVER_RECORDS";

    /**
     * Request to retrieve all known records of type {@link io.github.cpsc559.team16.common.dto.AddrServerRecord}
     * and {@link io.github.cpsc559.team16.common.dto.ChatServerRecord}.
     */
    public static final String ALL_SERVER_RECORDS = "ALL_SERVER_RECORDS";

    /**
     * Request to retrieve a single {@link io.github.cpsc559.team16.common.dto.AddrServerRecord}
     * by process ID.
     */
    public static final String SINGLE_AS_RECORD = "SINGLE_AS_RECORD";

    /**
     * Request to retrieve a single {@link io.github.cpsc559.team16.common.dto.ChatServerRecord}
     * by process ID.
     */
    public static final String SINGLE_CS_RECORD = "SINGLE_CS_RECORD";

    /**
     * Request for a collection containing the network PIDs of all currently
     * registered Addressing Servers (Peers) in the cluster.
     */
    public static final String ALL_PEER_PIDS = "ALL_PEER_PIDS";

    /**
     * Request for a collection containing the network PIDs of all currently
     * registered Chat Servers in the network.
     */
    public static final String ALL_CS_PIDS = "ALL_CS_PIDS";


    // Preventing any possible instantiation of the utility class
    private RequestObjectTypes() {}
}
