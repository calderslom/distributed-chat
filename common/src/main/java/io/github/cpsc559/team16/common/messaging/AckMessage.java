package io.github.cpsc559.team16.common.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A standardized acknowledgment message used for confirming successful communication,
 * such as registration or message receipt.
 * <p>
 * The payload of an {@code AckMessage} is typically a string (e.g., "OK", "Registered", or an error reason).
 * </p>
 *
 * <pre>
 * Example:
 * {
 *   "msgType": "ACK",
 *   "objectType": "Registration",
 *   "senderPID": 101,
 *   "senderRole": "PRIMARY",
 *   "targetRole": "REPLICA",
 *   "payload": "Registered"
 * }
 * </pre>
 */
public class AckMessage<T> extends BaseAddrServerMessage<T> {

    @JsonCreator
    public AckMessage(
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
     * Constructs a new acknowledgment message.
     *
     * @param objectType Describes what action is being acknowledged (e.g., "Registration", "Update").
     * @param senderPID The ID of the process sending the acknowledgment.
     * @param senderRole The role of the sender (e.g., PRIMARY, REPLICA, CHATSERVER).
     * @param targetRole The role of the process being acknowledged (e.g., CHATSERVER, REPLICA).
     * @param payload A short payload string. It can be anything you want, but you'll have to handle it appropriately.
     */
    public AckMessage(String objectType, long senderPID, String senderRole, String targetRole, T payload) {
        super(0, MessageTypes.ACK, objectType, senderPID, senderRole, targetRole, payload);
    }


    /**
     * Constructs a new acknowledgment message.
     *
     * @param messageID use the {@link MessageIDGenerator} to generate a unique message ID based on the network PID of the calling code.
     * @param objectType Describes what action is being acknowledged (e.g., "Registration", "Update").
     * @param senderPID The ID of the process sending the acknowledgment.
     * @param senderRole The role of the sender (e.g. PRIMARY, REPLICA, CHATSERVER).
     * @param targetRole The role of the process being acknowledged (e.g. CHATSERVER, REPLICA).
     * @param payload A short payload string. It can be anything you want, but you'll have to handle it appropriately.
     */
    public AckMessage(long messageID, String objectType, long senderPID, String senderRole, String targetRole, T payload) {
        super(messageID, MessageTypes.ACK, objectType, senderPID, senderRole, targetRole, payload);
    }


    /**
     * Creates a simple "OK" acknowledgment.
     *
     * @param senderPID The process ID of the sender.
     * @param senderRole The role of the sender.
     * @param targetRole The role of the intended recipient.
     * @return A basic {@code AckMessage} with payload "OK".
     */
    public static AckMessage<String> ok(long senderPID, String senderRole, String targetRole) {
        return new AckMessage<>(AckObjectTypes.OK, senderPID, senderRole, targetRole, "OK");
    }

    /**
     * Creates an ACK message from the PRIMARY server directed to a CLIENT.
     * <p>
     * This factory method generates an {@code AckMessage} where the sender is identified as PRIMARY and the recipient as CLIENT.
     * The acknowledgment type is hardcoded as {@code AckObjectTypes.HOSTADDRESS}, and the payload represents the assigned host.
     * </p>
     *
     * @param senderPID             the process ID of the PRIMARY server sending this message
     * @param chatServerHostAddress the host address (and optionally port) of the chat server to be communicated to the client
     * @return an {@code AckMessage} constructed with the specified parameters, ready to be sent from the PRIMARY to the CLIENT
     */
    public static AckMessage<String> chatHostAddress(long senderPID, String chatServerHostAddress) {
        return new AckMessage<>(AckObjectTypes.HOSTADDRESS, senderPID, Roles.PRIMARY, Roles.CLIENT, chatServerHostAddress);
    }

    /**
     * Creates an ACK message from the PRIMARY server directed to a CLIENT.
     * <p>
     * This factory method generates an {@code AckMessage} where the sender is identified as PRIMARY and the recipient as CLIENT.
     * The acknowledgment type is hardcoded as {@code AckObjectTypes.NOHOST}, and the payload is a "womp womp" String.
     * </p>
     *
     * @param senderPID             the process ID of the PRIMARY server sending this message
     * @return an {@code AckMessage} constructed with the specified parameters, ready to be sent from the PRIMARY to the CLIENT
     */
    public static AckMessage<String> noChatHost(long senderPID) {
        return new AckMessage<>(AckObjectTypes.NOHOST, senderPID, Roles.PRIMARY, Roles.CLIENT, "404 ChatServer Not Found — they ghosted you.");
    }



    /**
     * Creates an ACK message from the PRIMARY server directed to a REPLICA.
     * <p>
     * This factory method generates an {@code AckMessage} where the sender is identified as {@link Roles#PRIMARY}
     * and the recipient as {@link Roles#REPLICA}.
     * The acknowledgment type is hardcoded as {@code AckObjectTypes.REGISTERED}, and the payload contains the assigned PID.
     * </p>
     *
     * @param senderPID the process ID of the PRIMARY server sending this message
     * @param payload   the assigned PID to be sent as confirmation
     * @return an {@code AckMessage} constructed with the specified parameters, ready to be sent from the PRIMARY to the REPLICA
     */
    public static AckMessage<Long> replicaRegistered(long senderPID, Long payload) {
        return new AckMessage<>(AckObjectTypes.REGISTERED, senderPID, Roles.PRIMARY, Roles.REPLICA, payload);
    }

    /**
     * Creates an ACK message from the PRIMARY server directed to a REPLICA.
     * <p>
     * This factory method generates an {@code AckMessage} where the sender is identified as {@link Roles#PRIMARY}
     * and the recipient as {@link Roles#REPLICA}.
     * The acknowledgment type is hardcoded as {@code AckObjectTypes.SYNCHRONIZED}, and the payload contains the assigned PID.
     * </p>
     *
     * @param senderPID the process ID of the PRIMARY server sending this message
     * @param payload   the assigned PID to be sent as confirmation
     * @return an {@code AckMessage} constructed with the specified parameters, ready to be sent from the PRIMARY to the REPLICA
     */
    public static AckMessage<Long> replicaSynchronized(long messageID, long senderPID, Long payload) {
        return new AckMessage<>(messageID, AckObjectTypes.SYNCHRONIZED, senderPID, Roles.PRIMARY, Roles.REPLICA, payload);
    }

    /**
     * Creates an ACK message from the PRIMARY {@code AddressingServer} directed to a {@code ChatServer}.
     * <p>
     * This factory method generates an {@code AckMessage} where the sender is identified as {@link Roles#PRIMARY}
     * and the recipient as {@link Roles#CHATSERVER}.
     * The acknowledgment type is hardcoded as {@code AckObjectTypes.REGISTERED}, and the payload contains the assigned PID.
     * </p>
     *
     * @param senderPID the process ID of the PRIMARY server sending this message
     * @param payload   the assigned chat server PID to be sent as confirmation
     * @return an {@code AckMessage} constructed with the specified parameters, ready to be sent from the PRIMARY to the CHATSERVER
     */
    public static AckMessage<Long> chatServerRegistered(long senderPID, Long payload) {
        return new AckMessage<>(AckObjectTypes.REGISTERED, senderPID, Roles.PRIMARY, Roles.CHATSERVER, payload);
    }

    public static AckMessage<Boolean> replicated(long messagedID, long senderPID, Boolean eventReplicated) {
        return new AckMessage<>(messagedID, AckObjectTypes.REPLICATED, senderPID, Roles.REPLICA, Roles.PRIMARY, eventReplicated);
    }


    public static AckMessage<Long> replicaDeregistered(long senderPID, Long payload) {
                return new AckMessage<>(AckObjectTypes.DEREGISTERED, senderPID, Roles.PRIMARY, Roles.REPLICA, payload);
    }

    public static AckMessage<Long> chatServerDeregistered(long senderPID, Long payload) {
        return new AckMessage<>(AckObjectTypes.DEREGISTERED, senderPID, Roles.PRIMARY, Roles.CHATSERVER, payload);
    }

    public static AckMessage<Long> chatServerSynchronized(long messageID, long senderPID, long targetPID) {
        return new AckMessage<>(messageID, AckObjectTypes.SYNCHRONIZED, senderPID, Roles.PRIMARY, Roles.CHATSERVER, targetPID);
    }

}
