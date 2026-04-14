package io.github.cpsc559.team16.common.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.cpsc559.team16.common.dto.AddrServerRecord;
import io.github.cpsc559.team16.common.dto.ChatServerRecord;

/**
 * Represents an "UPDATE" type message used for sending updates to Addressing Servers,
 * Replicas, and Chat Servers.
 * <p>
 * This class simplifies the creation of update messages by pre-setting {@code msgType} to "UPDATE."
 * </p>
 *
 * @param <T> The type of data being sent as the payload (e.g., {@code ChatServerRecord}, {@code AddrServerRecord}).
 */
public class UpdateMessage<T> extends BaseAddrServerMessage<T> {

    // Jackson Constructor
    @JsonCreator
    public UpdateMessage(
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
     * Constructs an "UPDATE" message.
     * <p>
     * An example of a {@code ChatServer} sending a notification to a REPLICA that a failure of the primary
     * {@code AddressingServer} has occurred is given below:
     * </p>
     * <pre>
     * {
     *   "msgType": "UPDATE",
     *   "objectType": "Failure",
     *   "senderPID": 72,
     *   "senderRole": "CHATSERVER",
     *   "targetRole": "REPLICA",
     *   "payload": { `Long failedServerPID` }
     * }
     * </pre>
     *
     * @param objectType The type of data contained in the payload.
     * @param senderPID  The process ID of the sender.
     * @param senderRole The role of the sender (PRIMARY, REPLICA, CHATSERVER).
     * @param targetRole The intended recipient's role.
     * @param payload    The actual data being sent.
     */
    public UpdateMessage(String objectType, Long senderPID, String senderRole, String targetRole, T payload) {
        super(0, MessageTypes.UPDATE, objectType, senderPID, senderRole, targetRole, payload);
    }

    /**
     * Constructs an "UPDATE" message.
     * <p>
     * An example of a {@code ChatServer} sending a notification to a REPLICA that a failure of the primary
     * {@code AddressingServer} has occurred is given below:
     * </p>
     * <pre>
     * {
     *   "msgType": "UPDATE",
     *   "objectType": "Failure",
     *   "senderPID": 72,
     *   "senderRole": "CHATSERVER",
     *   "targetRole": "REPLICA",
     *   "payload": { `Long failedServerPID` }
     * }
     * </pre>
     *
     * @param messageID   A globally unique identifier for the message. Use {@link MessageIDGenerator} for generation.
     * @param objectType The type of data contained in the payload.
     * @param senderPID  The process ID of the sender.
     * @param senderRole The role of the sender (PRIMARY, REPLICA, CHATSERVER).
     * @param targetRole The intended recipient's role.
     * @param payload    The actual data being sent.
     */
    public UpdateMessage(long messageID, String objectType, Long senderPID, String senderRole, String targetRole, T payload) {
        super(messageID, MessageTypes.UPDATE, objectType, senderPID, senderRole, targetRole, payload);
    }


    /**
     * Creates an {@code UpdateMessage} to send an {@link AddrServerRecord}
     * from the PRIMARY AddressingServer to a REPLICA.
     */
    public static UpdateMessage<AddrServerRecord> asRecordPrimaryToReplica(long messageID, Long senderPID, AddrServerRecord record) {
        return new UpdateMessage<>(messageID, ObjectTypes.ADDR_SERVER_RECORD, senderPID, Roles.PRIMARY, Roles.REPLICA, record);
    }

    /**
     * Creates an {@code UpdateMessage} to send an {@link AddrServerRecord}
     * from the PRIMARY AddressingServer to a REPLICA.
     */
    public static UpdateMessage<AddrServerRecord> asRecordPrimaryToReplica(Long senderPID, AddrServerRecord record) {
        return new UpdateMessage<>(0, ObjectTypes.ADDR_SERVER_RECORD, senderPID, Roles.PRIMARY, Roles.REPLICA, record);
    }

    /**
     * Creates an {@code UpdateMessage} to send an {@link AddrServerRecord}
     * from the PRIMARY AddressingServer to a ChatServer.
     */
    public static UpdateMessage<AddrServerRecord> asRecordPrimaryToCS(long messageID, Long senderPID, AddrServerRecord record) {
        return new UpdateMessage<>(messageID, ObjectTypes.ADDR_SERVER_RECORD, senderPID, Roles.PRIMARY, Roles.CHATSERVER, record);
    }

    /**
     * Creates an {@code UpdateMessage} to send an {@link AddrServerRecord}
     * from the PRIMARY AddressingServer to a ChatServer.
     */
    public static UpdateMessage<AddrServerRecord> asRecordPrimaryToCS(Long senderPID, AddrServerRecord record) {
        return new UpdateMessage<>(0, ObjectTypes.ADDR_SERVER_RECORD, senderPID, Roles.PRIMARY, Roles.CHATSERVER, record);
    }

    /**
     * For use by {@code AddressingServer} REPLICAs during failover of the Primary AddressingServer.
     * Sends an {@code UpdateMessage} with an {@link AddrServerRecord} from one REPLICA to another.
     */
    public static UpdateMessage<AddrServerRecord> asRecordReplicaToReplica(long messageID, Long senderPID, AddrServerRecord record) {
        return new UpdateMessage<>(messageID, ObjectTypes.ADDR_SERVER_RECORD, senderPID, Roles.REPLICA, Roles.REPLICA, record);
    }

    /**
     * For use by {@code AddressingServer} REPLICAs during failover of the Primary AddressingServer.
     * Sends an {@code UpdateMessage} with an {@link AddrServerRecord} from one REPLICA to another.
     */
    public static UpdateMessage<AddrServerRecord> asRecordReplicaToReplica(Long senderPID, AddrServerRecord record) {
        return new UpdateMessage<>(0, ObjectTypes.ADDR_SERVER_RECORD, senderPID, Roles.REPLICA, Roles.REPLICA, record);
    }

    /**
     * Sends a {@link ChatServerRecord} from the PRIMARY AddressingServer to a ChatServer.
     */
    public static UpdateMessage<ChatServerRecord> csRecordPrimaryToCS(long messageID, Long senderPID, ChatServerRecord record) {
        return new UpdateMessage<>(messageID, ObjectTypes.CHAT_SERVER_RECORD, senderPID, Roles.PRIMARY, Roles.CHATSERVER, record);
    }

    /**
     * Sends a {@link ChatServerRecord} from the PRIMARY AddressingServer to a ChatServer.
     */
    public static UpdateMessage<ChatServerRecord> csRecordPrimaryToCS(Long senderPID, ChatServerRecord record) {
        return new UpdateMessage<>(0, ObjectTypes.CHAT_SERVER_RECORD, senderPID, Roles.PRIMARY, Roles.CHATSERVER, record);
    }

    /**
     * Sends a {@link ChatServerRecord} from the PRIMARY AddressingServer to a REPLICA.
     */
    public static UpdateMessage<ChatServerRecord> csRecordPrimaryToReplica(long messageID, Long senderPID, ChatServerRecord record) {
        return new UpdateMessage<>(messageID, ObjectTypes.CHAT_SERVER_RECORD, senderPID, Roles.PRIMARY, Roles.REPLICA, record);
    }

    /**
     * Sends a {@link ChatServerRecord} from the PRIMARY AddressingServer to a REPLICA.
     */
    public static UpdateMessage<ChatServerRecord> csRecordPrimaryToReplica(Long senderPID, ChatServerRecord record) {
        return new UpdateMessage<>(0, ObjectTypes.CHAT_SERVER_RECORD, senderPID, Roles.PRIMARY, Roles.REPLICA, record);
    }

    /**
     * Sends a {@link ChatServerRecord} from a ChatServer to the PRIMARY AddressingServer.
     */
    public static UpdateMessage<ChatServerRecord> csRecordChatServerToPrimary(long messageID, Long senderPID, ChatServerRecord record) {
        return new UpdateMessage<>(messageID, ObjectTypes.CHAT_SERVER_RECORD, senderPID, Roles.CHATSERVER, Roles.PRIMARY, record);
    }

}