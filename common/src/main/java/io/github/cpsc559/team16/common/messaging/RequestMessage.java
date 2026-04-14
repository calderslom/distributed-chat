package io.github.cpsc559.team16.common.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents an "REQUEST" type message used for sending REQUESTs to Addressing Servers,
 * Replicas, and Chat Servers.
 * <p>
 * This class simplifies the creation of REQUEST messages by pre-setting {@code msgType} to "REQUEST."
 * </p>
 *
 * @param <T> The type of data being sent as the payload (e.g., {@code ChatServerRecord}, {@code AddrServerRecord}).
 */
public class RequestMessage<T> extends BaseAddrServerMessage<T> {

    // Jackson Constructor
    @JsonCreator
    public RequestMessage(
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
     * Constructs an "REQUEST" message.
     * <p>
     * An example of a {@code ChatServer} a primary {@code AddressingServer} a request for all the ChatServerRecord
     * records for the distributed system is given below:
     * </p>
     * <pre>
     *   {
     *   "msgType": "REQUEST",
     *   "objectType": "AllChatServerInfo",
     *   "senderPID": 72,
     *   "senderRole": "CHATSERVER",
     *   "targetRole": "PRIMARY",
     *   "payload": { null }
     *   }
     * </pre>
     *
     * @param objectType The type of data contained in the payload.
     * @param senderPID The process ID of the sender.
     * @param senderRole The role of the sender (PRIMARY, REPLICA, CHATSERVER).
     * @param targetRole The intended recipient's role.
     * @param payload The actual data being sent.
     *
     */
    public RequestMessage(String objectType, long senderPID, String senderRole, String targetRole, T payload) {
        super(0, MessageTypes.REQUEST, objectType, senderPID, senderRole, targetRole, payload);
    }


    /**
     * Constructs an "REQUEST" message.
     * <p>
     * An example of a {@code ChatServer} a primary {@code AddressingServer} a request for all the ChatServerRecord
     * records for the distributed system is given below:
     * </p>
     * <pre>
     *   {
     *   "msgType": "REQUEST",
     *   "objectType": "AllChatServerInfo",
     *   "senderPID": 72,
     *   "senderRole": "CHATSERVER",
     *   "targetRole": "PRIMARY",
     *   "payload": { null }
     *   }
     * </pre>
     *
     * @param messageID   A globally unique identifier for the message. Use {@link MessageIDGenerator} for generation.
     * @param objectType The type of data contained in the payload.
     * @param senderPID The process ID of the sender.
     * @param senderRole The role of the sender (PRIMARY, REPLICA, CHATSERVER).
     * @param targetRole The intended recipient's role.
     * @param payload The actual data being sent.
     *
     */
    public RequestMessage(long messageID, String objectType, long senderPID, String senderRole, String targetRole, T payload) {
        super(messageID, MessageTypes.REQUEST, objectType, senderPID, senderRole, targetRole, payload);
    }

    public static RequestMessage<Void> requestAllChatServerRecords(long messageID, long senderPID) {
        return new RequestMessage<>(messageID, RequestObjectTypes.CHAT_SERVER_RECORDS, senderPID, Roles.REPLICA, Roles.PRIMARY, null);
    }

    public static RequestMessage<Void> requestAllAddrServerRecords(long messageID, long senderPID) {
        return new RequestMessage<>(messageID, RequestObjectTypes.ADDR_SERVER_RECORDS, senderPID, Roles.REPLICA, Roles.PRIMARY, null);
    }

    public static RequestMessage<Void> requestAllServerRecords(long messageID, long senderPID) {
        return new RequestMessage<>(messageID, RequestObjectTypes.ALL_SERVER_RECORDS, senderPID, Roles.REPLICA, Roles.PRIMARY, null);
    }

    public static RequestMessage<Void> requestAllPeerPids(long messageID, long senderPID) {
        return new RequestMessage<>(messageID, RequestObjectTypes.ALL_PEER_PIDS, senderPID, Roles.REPLICA, Roles.PRIMARY, null);
    }

    public static RequestMessage<Void> requestAllChatServerPids(long messageID, long senderPID) {
        return new RequestMessage<>(messageID, RequestObjectTypes.ALL_CS_PIDS, senderPID, Roles.REPLICA, Roles.PRIMARY, null);
    }

}
