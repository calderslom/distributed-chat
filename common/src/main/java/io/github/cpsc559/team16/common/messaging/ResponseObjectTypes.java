package io.github.cpsc559.team16.common.messaging;

/**
 * Defines string constants used to identify the type of data being returned
 * in a response message.
 * <p>
 * These identifiers help the receiving node (AddrServer or ChatServer) interpret
 * the payload within a {@link PrimaryResponseMessage} and perform the
 * correct cast or registry update.
 */
public class ResponseObjectTypes {

    /**
     * Response containing all {@link io.github.cpsc559.team16.common.dto.AddrServerRecord} entries
     * from the Primary AddressingServer.
     */
    public static final String ADDR_SERVER_RECORDS = "ADDR_SERVER_RECORDS";

    /**
     * Response containing all {@link io.github.cpsc559.team16.common.dto.ChatServerRecord} entries
     * from the Primary AddressingServer.
     */
    public static final String CHAT_SERVER_RECORDS = "CHAT_SERVER_RECORDS";

    /**
     * Response containing both {@link io.github.cpsc559.team16.common.dto.AddrServerRecord}
     * and {@link io.github.cpsc559.team16.common.dto.ChatServerRecord} entries.
     */
    public static final String ALL_SERVER_RECORDS = "ALL_SERVER_RECORDS";

    /**
     * Response containing a single {@link io.github.cpsc559.team16.common.dto.AddrServerRecord}
     * associated with a specific process ID.
     */
    public static final String SINGLE_AS_RECORD = "SINGLE_AS_RECORD";

    /**
     * Response containing a single {@link io.github.cpsc559.team16.common.dto.ChatServerRecord}
     * associated with a specific process ID.
     */
    public static final String SINGLE_CS_RECORD = "SINGLE_CS_RECORD";

    /**
     * Response containing a collection of network PIDs for all currently
     * registered Addressing Servers (Peers) in the cluster.
     */
    public static final String ALL_PEER_PIDS = "ALL_PEER_PIDS";

    /**
     * Response containing a collection of network PIDs for all currently
     * registered Chat Servers in the network.
     */
    public static final String ALL_CS_PIDS = "ALL_CS_PIDS";

    /**
     * A general acknowledgment or status response from the Primary
     * that does not include a specific record payload.
     */
    public static final String PRIMARY_RESPONSE = "PRIMARY_RESPONSE";


    // Preventing any possible instantiation of the utility class
    private ResponseObjectTypes() {}
}