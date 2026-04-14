package io.github.cpsc559.team16.common.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.cpsc559.team16.common.dto.ChatServerRecord;
import io.github.cpsc559.team16.common.dto.AddrServerRecord;


/**
 * A specialized version of {@code BaseAddrServerMessage} for synchronization requests.
 * <p>
 * Used when an existing process (ChatServer or Replica) that already has a PID
 * needs to re-establish its record with a newly elected Primary.
 * </p>
 *
 * @param <T> The type of the payload, such as {@code ChatServerRecord} or {@code AddrServerRecord}.
 */
public class SyncRegisterMessage<T> extends BaseAddrServerMessage<T> {

    // Jackson Constructor
    @JsonCreator
    public SyncRegisterMessage(
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
     * Constructs a "SYNCHRONIZE" message.
     */
    private SyncRegisterMessage(long messageID, String objectType, long senderPID, String senderRole, String targetRole, T payload) {
        super(messageID, MessageTypes.SYNCHRONIZE, objectType, senderPID, senderRole, targetRole, payload);
    }

    /**
     * Constructs a "SYNCHRONIZE" message.
     */
    private SyncRegisterMessage(String objectType, long senderPID, String senderRole, String targetRole, T payload) {
        super(0, MessageTypes.SYNCHRONIZE, objectType, senderPID, senderRole, targetRole, payload);
    }

    /**
     * Factory method for an existing ChatServer to sync its state with the Primary.
     */
    public static SyncRegisterMessage<ChatServerRecord> fromChatServer(long messageID, ChatServerRecord record) {
        return new SyncRegisterMessage<>(messageID,
                ObjectTypes.CHAT_SERVER_RECORD,
                record.getPID(), // Uses the already assigned PID
                Roles.CHATSERVER,
                Roles.PRIMARY,
                record
        );
    }

    /**
     * Factory method for an existing AddressingServer replica to sync its state.
     */
    public static SyncRegisterMessage<AddrServerRecord> fromReplica(long messageID, AddrServerRecord record) {
        return new SyncRegisterMessage<>(messageID,
                ObjectTypes.ADDR_SERVER_RECORD,
                record.getPID(), // Uses the already assigned PID
                Roles.REPLICA,
                Roles.PRIMARY,
                record
        );
    }
}