package io.github.cpsc559.team16.common.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.cpsc559.team16.common.dto.ChatServerRecord;
import io.github.cpsc559.team16.common.dto.AddrServerRecord;
import io.github.cpsc559.team16.common.dto.ServerRole;

/**
 * A specialized version of {@code BaseAddrServerMessage} for sending REGISTER messages.
 * <p>
 * This message type is used when a ChatServer or AddressingServer process registers
 * with the Primary AddressingServer.
 * </p>
 *
 * @param <T> The type of the payload, such as {@code ChatServerRecord} or {@code AddrServerRecord}.
 */
public class RegisterMessage<T> extends BaseAddrServerMessage<T> {

    // Processes receive a PID upon registration with the PRIMARY {@code AddressingServer}
    private static final long DEFAULT_PID = 0L;

    // Jackson Constructor
    @JsonCreator
    public RegisterMessage(
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
     * Constructs an "REGISTER" message.
     * <p>
     * An example of a {@code ChatServer} a primary {@code AddressingServer} a REGISTER for all the ChatServerRecord
     * records for the distributed system is given below:
     * </p>
     * <pre>
     *   {
     *   "msgType": "REGISTER",
     *   "objectType": "",
     *   "senderPID":
     *   "senderRole": "CHATSERVER",
     *   "targetRole": "PRIMARY",
     *   "payload": { null }
     *   }
     * </pre>
     *
     * @param objectType The subclass of {@code ServerRecord} being sent - {@code ChatServerRecord} or {@code AddrServerRecord}
     * @param senderRole The role of the sender (PRIMARY, REPLICA, CHATSERVER).
     * @param targetRole The role of the receiver - The PRIMARY {@code AddressingServer}.
     * @param payload    The ServerRecord record to be sent for registering on the network.
     *
     * <p>
     * <strong>NOTE:</strong> All {@code RegisterMessage}'s are sent with a process ID (PID) set to zero, as
     * all network processes are assigned a pid UPON registration, not before.
     * </p>
     */
    private RegisterMessage(String objectType, String senderRole, String targetRole, T payload) {
        super(0, MessageTypes.REGISTER, objectType, DEFAULT_PID, senderRole, targetRole, payload);
    }

    /**
     * Constructs an "REGISTER" message.
     * <p>
     * An example of a {@code ChatServer} a primary {@code AddressingServer} a REGISTER for all the ChatServerRecord
     * records for the distributed system is given below:
     * </p>
     * <pre>
     *   {
     *   "msgType": "REGISTER",
     *   "objectType": "",
     *   "senderPID":
     *   "senderRole": "CHATSERVER",
     *   "targetRole": "PRIMARY",
     *   "payload": { null }
     *   }
     * </pre>
     *
     * @param messageID   A globally unique identifier for the message. Use {@link MessageIDGenerator} for generation.
     * @param objectType The subclass of {@code ServerRecord} being sent - {@code ChatServerRecord} or {@code AddrServerRecord}
     * @param senderRole The role of the sender (PRIMARY, REPLICA, CHATSERVER).
     * @param targetRole The role of the receiver - The PRIMARY {@code AddressingServer}.
     * @param payload    The ServerRecord record to be sent for registering on the network.
     *
     * <p>
     * <strong>NOTE:</strong> All {@code RegisterMessage}'s are sent with a process ID (PID) set to zero, as
     * all network processes are assigned a pid UPON registration, not before.
     * </p>
     */
    private RegisterMessage(long messageID, String objectType, String senderRole, String targetRole, T payload) {
        super(messageID, MessageTypes.REGISTER, objectType, DEFAULT_PID, senderRole, targetRole, payload);
    }

    /**
     * Factory method for registering a ChatServer.
     *
     * @param clientPort       The port used for client communication.
     * @param peerPort         The port used for peer-to-peer chat server communication.
     * @param maxClientCount   The server's maximum client capacity.
     * @return A {@code RegisterMessage} containing a {@code ChatServerRecord} payload.
     */
    public static RegisterMessage<ChatServerRecord> fromChatServer(String hostAddress, int clientPort, int peerPort,
                                                                   int maxClientCount) {
        ChatServerRecord record = new ChatServerRecord(
                DEFAULT_PID, hostAddress, clientPort, peerPort, maxClientCount);
        return new RegisterMessage<>(ObjectTypes.CHAT_SERVER_RECORD, Roles.CHATSERVER, Roles.PRIMARY, record);
    }

    /**
     * Factory method for registering an AddressingServer replica.
     *
     * @param clientPort       Port used for client communication.
     * @param peerPort         Port used for replica communication.
     * @param chatServerPort   Port used to receive chat server registrations.
     * @return A {@code RegisterMessage} containing an {@code AddrServerRecord} payload.
     */
    public static RegisterMessage<AddrServerRecord> fromReplica(long messageID, String hostAddress, int clientPort, int peerPort, int chatServerPort) {
        AddrServerRecord record = new AddrServerRecord(
                DEFAULT_PID, hostAddress, clientPort, peerPort, chatServerPort, ServerRole.REPLICA);
        return new RegisterMessage<>(messageID, ObjectTypes.ADDR_SERVER_RECORD, Roles.REPLICA, Roles.PRIMARY, record);
    }

    /**
     * Factory method for registering a ChatServer with the HostAddress declared.
     *
     * @param hostAddress      The network (IP) address of the address server.
     * @param clientPort       The port used for client communication.
     * @param peerPort         The port used for peer-to-peer chat server communication.
     * @param maxClientCount   The server's maximum client capacity.
     * @return A {@code RegisterMessage} containing a {@code ChatServerRecord} payload.
     */
    public static RegisterMessage<ChatServerRecord> fromChatServerWithAddress(String hostAddress, int clientPort, int peerPort,
                                                                   int maxClientCount) {
        ChatServerRecord record = new ChatServerRecord(
                DEFAULT_PID, hostAddress, clientPort, peerPort, maxClientCount);
        return new RegisterMessage<>(ObjectTypes.CHAT_SERVER_RECORD, Roles.CHATSERVER, Roles.PRIMARY, record);
    }


    /**
     * Factory method for registering an AddressingServer replica with the HostAddress declared.
     *
     * @param hostAddress      The network (IP) address of the address server.
     * @param clientPort       Port used for client communication.
     * @param peerPort         Port used for replica communication.
     * @param chatServerPort   Port used to receive chat server registrations.
     * @return A {@code RegisterMessage} containing an {@code AddrServerRecord} payload.
     */
    public static RegisterMessage<AddrServerRecord> fromReplicaWithAddress(String hostAddress, int clientPort, int peerPort, int chatServerPort) {
        AddrServerRecord record = new AddrServerRecord(
                DEFAULT_PID, hostAddress, clientPort, peerPort, chatServerPort, ServerRole.REPLICA);
        return new RegisterMessage<>(ObjectTypes.ADDR_SERVER_RECORD, Roles.REPLICA, Roles.PRIMARY, record);
    }


    /**
     * Factory method for registering a Client with the Primary AddressingServer.
     *
     * @return A {@code RegisterMessage} containing a {@code String} payload for registering the client.
     */
    public static RegisterMessage<String> fromClient() {
        return new RegisterMessage<>(null, Roles.CLIENT, Roles.PRIMARY, null);
    }



}
