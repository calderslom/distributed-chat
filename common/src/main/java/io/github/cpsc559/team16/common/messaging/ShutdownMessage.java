package io.github.cpsc559.team16.common.messaging;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A message instructing a remote process to exit and shutdown.
 * <p>
 * This message uses the msgType "SHUTDOWN" and an objectType of `Long`. The payload
 * is a Long representing the process ID of the failed (or target) process. Recipients should compare
 * the failsafe PID in the payload against their own PID to ensure that they are indeed the intended target.
 * </p>
 */
public class ShutdownMessage extends BaseAddrServerMessage<Long> {

    // Jackson Constructor
    @JsonCreator
    public ShutdownMessage(
            @JsonProperty("messageID") long messageID,
            @JsonProperty("msgType") String msgType,
            @JsonProperty("objectType") String objectType,
            @JsonProperty("senderPID") long senderPID,
            @JsonProperty("senderRole") String senderRole,
            @JsonProperty("targetRole") String targetRole,
            @JsonProperty("payload") Long payload) {
        super(messageID, msgType, objectType, senderPID, senderRole, targetRole, payload);
    }

    private static final String OBJECT_TYPE = "Shutdown";


    /**
     * Private constructor to prevent direct instantiation and enforce the use of factory methods.
     *
     * @param senderPID  The process ID of the sender.
     * @param senderRole The role of the sender (e.g., CHATSERVER).
     * @param targetRole The intended recipient's role (e.g., PRIMARY).
     * @param failsafePID The process ID of the target process (failsafe check).
     */
    private ShutdownMessage(long senderPID, String senderRole, String targetRole, long failsafePID) {
        super(0, MessageTypes.SHUTDOWN, OBJECT_TYPE, senderPID, senderRole, targetRole, failsafePID);
    }

    /**
     * Private constructor to prevent direct instantiation and enforce the use of factory methods.
     *
     * @param messageID use the {@link MessageIDGenerator} to generate a unique message ID based on the network PID of the calling code.
     * @param senderPID  The process ID of the sender.
     * @param senderRole The role of the sender (e.g. CHATSERVER).
     * @param targetRole The intended recipient's role (e.g. PRIMARY).
     @param failsafePID The process ID of the target process (failsafe check).
     */
    private ShutdownMessage(long messageID, long senderPID, String senderRole, String targetRole, long failsafePID) {
        super(messageID, MessageTypes.SHUTDOWN, OBJECT_TYPE, senderPID, senderRole, targetRole, failsafePID);
    }

    /**
     * Creates a ShutdownMessage intended for a Replica Addressing Server.
     *
     * @param senderPID  The PID of the process sending this message.
     * @param senderRole The {@link Roles} of the sending process.
     * @param replicaPID The process ID of the REPLICA expected to shut down (failsafe).
     * @return A ShutdownMessage with targetRole set to REPLICA.
     */
    public static ShutdownMessage toReplica(long messageID, long senderPID, String senderRole, long replicaPID) {
        return new ShutdownMessage(messageID, senderPID, senderRole, Roles.REPLICA, replicaPID);
    }

    /**
     * Creates a ShutdownMessage intended for the Primary Addressing Server.
     * Typically, a REPLICA sends this message to the PRIMARY after it has failed,
     * instructing it to shut down, restart, and register as a replica.
     *
     * @param senderPID  The PID of the REPLICA sending this message.
     * @param primaryPID The process ID of the PRIMARY expected to shut down (failsafe).
     * @return A ShutdownMessage with targetRole set to PRIMARY.
     */
    public static ShutdownMessage toPrimary(long messageID, long senderPID, long primaryPID) {
        return new ShutdownMessage(messageID, senderPID, Roles.REPLICA, Roles.PRIMARY, primaryPID);
    }

    /**
     * Creates a ShutdownMessage intended for a Chat Server.
     * Typically, a PRIMARY server sends this message to instruct a CHATSERVER to shutdown.
     *
     * @param senderPID    The PID of the process sending this message.
     * @param senderRole    The {@link Roles} of the sending process.
     * @param chatServerPID The process ID of the CHATSERVER expected to shutdown (failsafe).
     * @return A ShutdownMessage with targetRole set to CHATSERVER.
     */
    public static ShutdownMessage toChatServer(long messageID, long senderPID, String senderRole, long chatServerPID) {
        return new ShutdownMessage(messageID, senderPID, senderRole, Roles.CHATSERVER, chatServerPID);
    }

}