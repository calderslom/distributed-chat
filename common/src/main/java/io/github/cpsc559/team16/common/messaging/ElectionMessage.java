package io.github.cpsc559.team16.common.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A standardized election message used to create and respond to leader election events.
 *
 * <p>
 * The payload of an {@code ElectionMessage} is a string ("Election", "Bully", or "Leader").
 * </p>
 * 
 * <pre>
 * Example:
 * {
 *   "msgType": "ELECTION",
 *   "objectType": "String",
 *   "senderPID": 11,
 *   "senderRole": "",
 *   "targetRole": "",
 *   "payload": "Leader"
 * }
 * </pre>
 */
public class ElectionMessage extends BaseAddrServerMessage<String> {

    // Jackson Constructor
    @JsonCreator
    public ElectionMessage(
            @JsonProperty("messageID") long messageID,
            @JsonProperty("msgType") String msgType,
            @JsonProperty("objectType") String objectType,
            @JsonProperty("senderPID") long senderPID,
            @JsonProperty("senderRole") String senderRole,
            @JsonProperty("targetRole") String targetRole,
            @JsonProperty("payload") String payload) {
        super(messageID, msgType, objectType, senderPID, senderRole, targetRole, payload);
    }

    /**
     * Constructs a new election message, used to initiate a leader election.
     *
     * @param senderPID The ID of the process initiating the election.
     */
    private ElectionMessage(long senderPID, String payload) {
        super(0, MessageTypes.ELECTION, ObjectTypes.STRING, senderPID, Roles.REPLICA, Roles.REPLICA, payload);
    }

    /**
     * Constructs a new election message, used to initiate a leader election.
     *
     * @param messageID   A globally unique identifier for the message. Use {@link MessageIDGenerator} for generation.
     * @param senderPID The ID of the process initiating the election.
     */
    private ElectionMessage(long messageID, long senderPID, String payload) {
        super(messageID, MessageTypes.ELECTION, ObjectTypes.STRING, senderPID, Roles.REPLICA, Roles.REPLICA, payload);
    }

    /**
     * Creates a simple "ELECTION" message used to initiate
     * a leader election with the senderPID running.
     *
     * @param senderPID The process ID of the sender.
     * @return A basic {@code ElectionMessage} with payload "Election".
     */
    public static ElectionMessage election(long senderPID){
        return new ElectionMessage(senderPID, "Election");
    }

    /**
     * Creates a simple "BULLY" message used to respond to an
     * "ELECTION" message from a sender with a lower PID.
     *
     * @param senderPID The process ID of the sender.
     * @return A basic {@code ElectionMessage} with payload "Bully".
     */
    public static ElectionMessage bully(long senderPID){
        return new ElectionMessage(senderPID, "Bully");
    }

    /**
     * Creates a simple "LEADER" message used to announce a
     * leader election winner.
     *
     * @param senderPID The process ID of the sender.
     * @return A basic {@code ElectionMessage} with payload "Leader".
     */
    public static ElectionMessage leader(long senderPID){
        return new ElectionMessage(senderPID, "Leader");
    }
}
