package io.github.cpsc559.team16.common.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

/**
 * Represents a response sent by the Primary Addressing Server to other nodes in the network.
 * <p>
 * This class encapsulates data requested by Chat Servers or Replicas, such as
 * process ID sets used for registry synchronization and stale record purging.
 * </p>
 *
 * @param <T> The type of the payload being returned (e.g., {@code Set<Long>}).
 */
public class PrimaryResponseMessage<T> extends BaseAddrServerMessage<T> {

    // Jackson Constructor
    @JsonCreator
    public PrimaryResponseMessage(
            @JsonProperty("messageID") long messageID,
            @JsonProperty("msgType") String msgType, 
            @JsonProperty("objectType") String objectType,
            @JsonProperty("senderPID") long senderPID,
            @JsonProperty("senderRole") String senderRole,
            @JsonProperty("targetRole") String targetRole, 
            @JsonProperty("payload") T payload) {
        super(messageID, msgType, objectType, senderPID, senderRole, targetRole, payload);
    }

    /**
     * Constructs a new PrimaryResponseMessage with a generated message ID.
     *
     * @param objectType   The type of data contained in the payload (from {@link ObjectTypes}).
     * @param senderPID    The PID of the Primary Addressing Server.
     * @param targetRole The role of the recipient (e.g., Roles.REPLICA, Roles.CHATSERVER).
     * @param payload      The data being transmitted.
     */
    public PrimaryResponseMessage(String objectType, Long senderPID, String targetRole, T payload) {
        super(0, MessageTypes.PRIMARY_RESPONSE, objectType, senderPID, Roles.PRIMARY, targetRole, payload);
    }

    /**
     * Constructs a new PrimaryResponseMessage with a specific message ID.
     *
     * @param messageID    The unique identifier for this message.
     * @param objectType   The type of data contained in the payload.
     * @param senderPID    The PID of the Primary Addressing Server.
     * @param targetRole The role of the recipient.
     * @param payload      The data being transmitted.
     */
    public PrimaryResponseMessage(long messageID, String objectType, Long senderPID, String targetRole, T payload) {
        super(messageID, MessageTypes.PRIMARY_RESPONSE, objectType, senderPID, Roles.PRIMARY, targetRole, payload);
    }

    /**
     * Creates a response containing the current set of active Addressing Server PIDs.
     * * @param messageID    The unique identifier for this message.
     * @param senderPID     The PID of the Primary Addressing Server.
     * @param targetRole  The role of the recipient (REPLICA or CHATSERVER).
     * @param pids          The set of active Peer PIDs.
     * @return A PrimaryResponseMessage configured with the PID_SET object type.
     */
    public static PrimaryResponseMessage<Set<Long>> addrServerPidList(long messageID, Long senderPID, String targetRole, Set<Long> pids) {
        return new PrimaryResponseMessage<>(messageID, ResponseObjectTypes.ALL_PEER_PIDS, senderPID, targetRole, pids);
    }

    /**
     * Creates a response containing the current set of active Chat Server PIDs.
     * * @param messageID    The unique identifier for this message.
     * @param senderPID     The PID of the Primary Addressing Server.
     * @param targetRole  The role of the recipient (REPLICA or CHATSERVER).
     * @param pids          The set of active Chat Server PIDs.
     * @return A PrimaryResponseMessage configured with the PID_SET object type.
     */
    public static PrimaryResponseMessage<Set<Long>> chatServerPidList(long messageID, Long senderPID, String targetRole, Set<Long> pids) {
        return new PrimaryResponseMessage<>(messageID, ResponseObjectTypes.ALL_CS_PIDS, senderPID, targetRole, pids);
    }


}