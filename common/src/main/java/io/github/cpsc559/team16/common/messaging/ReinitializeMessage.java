package io.github.cpsc559.team16.common.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A message instructing a remote process to exit and reinitialize (i.e., restart and re-register).
 * <p>
 * This message uses the msgType "REINITIALIZE" and an objectType of `Long`. The payload
 * is a Long representing the process ID of the failed (or target) process. Recipients should compare
 * the failsafe PID in the payload against their own PID to ensure that they are indeed the intended target.
 * </p>
 */
public class ReinitializeMessage extends BaseAddrServerMessage<Long> {

    // Jackson Constructor
    @JsonCreator
    public ReinitializeMessage(
            @JsonProperty("messageID") long messageID,
            @JsonProperty("msgType") String msgType,
            @JsonProperty("objectType") String objectType,
            @JsonProperty("senderPID") long senderPID,
            @JsonProperty("senderRole") String senderRole,
            @JsonProperty("targetRole") String targetRole,
            @JsonProperty("payload") Long payload) {
        super(messageID, msgType, objectType, senderPID, senderRole, targetRole, payload);
    }

    private static final String OBJECT_TYPE = "Reinitialize";

    /**
     * Private constructor to enforce the use of factory methods.
     *
     * @param senderPID   The PID of the sender.
     * @param senderRole  The role of the sender.
     * @param targetRole  The intended recipient's role.
     * @param failsafePID The process ID of the target process (failsafe check).
     */
    private ReinitializeMessage(long senderPID, String senderRole, String targetRole, long failsafePID) {
        super(0, MessageTypes.REINITIALIZE, OBJECT_TYPE, senderPID, senderRole, targetRole, failsafePID);
    }

    /**
     * Private constructor to enforce the use of factory methods.
     *
     * @param messageID   A globally unique identifier for the message. Use {@link MessageIDGenerator} for generation.
     * @param senderPID   The PID of the sender.
     * @param senderRole  The role of the sender.
     * @param targetRole  The intended recipient's role.
     * @param failsafePID The process ID of the target process (failsafe check).
     */
    private ReinitializeMessage(long messageID, long senderPID, String senderRole, String targetRole, long failsafePID) {
        super(messageID, MessageTypes.REINITIALIZE, OBJECT_TYPE, senderPID, senderRole, targetRole, failsafePID);
    }

    /**
     * Creates a ReinitializeMessage intended for a Replica Addressing Server.
     * Typically, a PRIMARY server sends this message to instruct a REPLICA to reinitialize.
     *
     * @param senderPID  The PID of the process sending this message.
     * @param senderRole The {@link Roles} of the sending process.
     * @param replicaPID The process ID of the REPLICA expected to reinitialize (failsafe).
     * @return A ReinitializeMessage with targetRole set to REPLICA.
     */
    public static ReinitializeMessage toReplica(long senderPID, String senderRole, long replicaPID) {
        return new ReinitializeMessage(senderPID, senderRole, Roles.REPLICA, replicaPID);
    }

    /**
     * Creates a ReinitializeMessage intended for the Primary Addressing Server.
     * Typically, a REPLICA sends this message to the PRIMARY after it has failed,
     * instructing it to shut down, restart, and register as a replica.
     *
     * @param senderPID  The PID of the REPLICA sending this message.
     * @param primaryPID The process ID of the PRIMARY expected to reinitialize (failsafe).
     * @return A ReinitializeMessage with targetRole set to PRIMARY.
     */
    public static ReinitializeMessage toPrimary(long senderPID, long primaryPID) {
        return new ReinitializeMessage(senderPID, Roles.REPLICA, Roles.PRIMARY, primaryPID);
    }

    /**
     * Creates a ReinitializeMessage intended for a Chat Server.
     * Typically, a PRIMARY server sends this message to instruct a CHATSERVER to reinitialize.
     *
     * @param senderPID    The PID of the process sending this message.
     * @param senderRole    The {@link Roles} of the sending process.
     * @param chatServerPID The process ID of the CHATSERVER expected to reinitialize (failsafe).
     * @return A ReinitializeMessage with targetRole set to CHATSERVER.
     */
    public static ReinitializeMessage toChatServer(long senderPID, String senderRole, long chatServerPID) {
        return new ReinitializeMessage(senderPID, senderRole, Roles.CHATSERVER, chatServerPID);
    }

}
