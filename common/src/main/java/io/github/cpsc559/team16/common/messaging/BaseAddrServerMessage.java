package io.github.cpsc559.team16.common.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Set;


/**
 * Represents a standardized message format used for communication between Addressing Servers,
 * Chat Servers, and Clients within the distributed network.
 * <p>
 * This class ensures structured messaging across all networked processes, allowing them to
 * exchange data, send updates, request information, and participate in leader elections.
 * </p>
 * <h3>Example Messages:</h3>
 * <pre>
 * // Requesting all Addressing Server info
 * {
 *   "msgType": "REQUEST",
 *   "objectType": "AllAddrServerInfo",
 *   "senderPID": 102,
 *   "senderRole": "REPLICA",
 *   "targetRole": "PRIMARY",
 *   "payload": {}
 * }
 *
 * // Sending a PING for failure detection
 * {
 *   "msgType": "PING",
 *   "objectType": "NONE",
 *   "senderPID": 101,
 *   "senderRole": "REPLICA",
 *   "targetRole": "PRIMARY",
 *   "payload": {}
 * }
 *
 * // Updating a Chat Server's client count
 * {
 *   "msgType": "UPDATE",
 *   "objectType": "ClientCount",
 *   "senderPID": 203,
 *   "senderRole": "CHATSERVER",
 *   "targetRole": "PRIMARY",
 *   "payload": {
 *     "clientCount": 45
 *   }
 * }
 *
 * // Updating Chat Server Info
 * {
 *   "msgType": "UPDATE",
 *   "objectType": "ChatServerRecord",
 *   "senderPID": 7,
 *   "senderRole": "PRIMARY",
 *   "targetRole": "REPLICA",
 *   "payload": { <serialized ChatServerRecord object> }
 * }
 * </pre>
 */
public class BaseAddrServerMessage<T> {

    /**
     * This field does not have to be set, but if you do -
     * use the {@link MessageIDGenerator} to generate a unique message ID
     * based on the network PID of the calling code.
     *<p>
     * A message ID of 0L represents a message that was not created with a message ID.
     * </p>
     */
    private final long messageID;

    /**
     * Defines the type of action required for the message.
     * <p>
     * This field determines how the recipient should process the message.
     * The possible values include:
     * </p>
     * <ul>
     *     <li><strong>REGISTER</strong> - Requesting the Primary {@code AddressingServer} to register this server.</li>
     *     <li><strong>UPDATE</strong> - A data update, such as a change in chat server status or client count.</li>
     *     <li><strong>REQUEST</strong> - A request for information, such as retrieving updated records.</li>
     *     <li><strong>PING</strong> - Used for failure detection, ensuring peers are still responsive.</li>
     *     <li><strong>NOTIFICATION</strong> - A general event notification, such as a server joining or leaving the network.</li>
     *     <li><strong>SERVERFAILURE</strong> - Used by any process to notify others in the network that a process has failed and needs to be expunged from all records and channels.</li>
     *     <li><strong>ELECTION</strong> - Used for Addressing Server leadership election messages.</li>
     *     <li><strong>ACK</strong> - Used for custom one-time responses.</li>
     * </ul>
     * <p>
     * This field works in conjunction with {@code objectType} to provide additional context
     * about the data being transmitted.
     * </p>
     */
    private final String msgType;

    /**
     * Defines the type (Class) of data contained within the message payload.
     * <p>
     * This field specifies the nature of the data being transmitted, allowing
     * recipients to properly process and interpret the message. The possible values include:
     * </p>
     * <ul>
     *     <li><strong>ChatServerRecord</strong> - Contains details about a registered chat server, such as host address and active client count.</li>
     *     <li><strong>AddrServerRecord</strong> - Provides information about an Addressing Server, including its role (PRIMARY or REPLICA).</li>
     *     <li><strong>ClientCount</strong> - Indicates an update to the number of active clients connected to a chat server.</li>
     *     <li><strong>ChatServerFailure</strong> - A notification that a chat server has failed. The payload is a Long containing the failed PID.</li>
     *     <li><strong>AddrServerFailure</strong> - A notification that an addressing server has failed. The payload is a Long containing the failed PID.</li>
     *     <li><strong>ElectionMessage</strong> - Used during an election process to determine a new PRIMARY server.</li>
     * </ul>
     * <p>
     * This field ensures that recipients can process incoming messages appropriately,
     * distinguishing between different types of data even within the same {@code msgType} category.
     * </p>
     */
    private final String objectType;

    /** The process ID (PID) of the sender of the message. */
    private final long senderPID;

    /** The role of the sender in the distributed system (PRIMARY, REPLICA, CHATSERVER, CLIENT). */
    private final String senderRole;

    /** The intended recipient role of the message (PRIMARY, REPLICA, CHATSERVER, CLIENT). */
    private final String targetRole;

    /** The actual data being transmitted in the message. */
    private final T payload;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Constructs a new message with the provided parameters.
     *
     * @param messageID  The unique message ID - created by the MessageIDGenerator class
     * @param msgType    The type of action required (UPDATE, REQUEST, etc.).
     * @param objectType The type of data contained in the payload.
     * @param senderPID  The process ID of the sender.
     * @param senderRole The role of the sender (PRIMARY, REPLICA, CHATSERVER).
     * @param targetRole The intended recipient's role.
     * @param payload    The actual data being sent.
     *
     */
    @JsonCreator
    public BaseAddrServerMessage(
            @JsonProperty("messageID") long messageID,
            @JsonProperty("msgType") String msgType,
            @JsonProperty("objectType") String objectType,
            @JsonProperty("senderPID") long senderPID,
            @JsonProperty("senderRole") String senderRole,
            @JsonProperty("targetRole") String targetRole,
            @JsonProperty("payload") T payload) {
        this.messageID = messageID;
        this.msgType = msgType;
        this.objectType = objectType;
        this.senderPID = senderPID;
        this.senderRole = senderRole;
        this.targetRole = targetRole;
        this.payload = payload;
    }


    // Getters
    public long getMessageID() { return messageID; }
    public String getMsgType() { return msgType; }
    public String getObjectType() { return objectType; }
    public long getSenderPID() { return senderPID; }
    public String getSenderRole() { return senderRole; }
    public String getTargetRole() { return targetRole; }
    public T getPayload() { return payload; }

    /**
     * Serializes the message into a JSON string for network transmission.
     *
     * @return The JSON representation of the message.
     * @throws JsonProcessingException If an error occurs during serialization.
     */
    public String toJson() throws JsonProcessingException {
        return objectMapper.writeValueAsString(this);
    }


    /**
     * Deserializes a JSON string into a {@code BaseAddrServerMessage} object with a specified payload type.
     *
     * @param json The JSON string representing the message.
     * @param payloadClass The class type of the payload (e.g., ChatServerRecord.class).
     * @return A deserialized {@code BaseAddrServerMessage<T>} instance with a properly typed payload.
     * @throws JsonProcessingException If an error occurs during deserialization.
     */
    public static <T> BaseAddrServerMessage<T> fromJson(String json, Class<T> payloadClass) throws JsonProcessingException {
        return objectMapper.readValue(json, objectMapper.getTypeFactory().constructParametricType(BaseAddrServerMessage.class, payloadClass));
    }


    /**
     * Casts the payload to the type declared in the {@code objectType} field.
     *
     * <p>This method enforces that the declared {@code objectType} matches the requested class type.</p>
     *
     * @param expectedClass The class you expect the payload to be.
     * @return The payload cast to the expected type.
     * @param <U> The expected payload type.
     * @throws IllegalStateException if the declared objectType does not match the expected type.
     */
    @SuppressWarnings("unchecked")
    public <U> U safeCastPayload(Class<U> expectedClass) {
        // Resolve the class from the objectType string
        Class<?> resolvedType = ObjectTypes.getPayloadClass(this.objectType);

        if (resolvedType == null) {
            System.err.println("Unrecognized objectType");
            throw new IllegalStateException("Unrecognized objectType: " + objectType);
        }

        if (!expectedClass.equals(resolvedType)) {
            throw new IllegalStateException("Expected type " + expectedClass.getSimpleName()
                    + " but objectType indicates " + resolvedType.getSimpleName());
        }

        return (U) payload;
    }


    /**
     * Safely casts and converts a payload into a Set of Longs.
     * <p>
     * This is a specialized helper for object types that return Sets (like ALL_PEER_PIDS).
     * It ensures that any numeric types deserialized by Jackson as Integers are
     * correctly converted to Longs to match the Registry's key (PID) types.
     * </p>
     *
     * @return A Set of Longs, or an empty set if the payload is null.
     */
    @SuppressWarnings("unchecked")
    public Set<Long> safeCastPayloadToLongSet() {
        Object rawPayload = safeCastPayload(Set.class);
        if (rawPayload instanceof Set<?> rawSet) {
            Set<Long> longSet = new java.util.HashSet<>();
            for (Object obj : rawSet) {
                // Handles Integer, Long, or String representations of PIDs
                longSet.add(Long.valueOf(obj.toString()));
            }
            return longSet;
        }
        return new java.util.HashSet<>();
    }

}
