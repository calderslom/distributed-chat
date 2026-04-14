package io.github.cpsc559.team16.common.messaging;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A specialized message used to send notifications about events or updates within the distributed system.
 * <p>
 * This class extends {@link BaseAddrServerMessage} and provides a specific implementation for
 * messages of type {@code MessageTypes.NOTIFICATION}. It is used to notify other processes about
 * events such as client count changes, server failures, etc.
 * </p>
 *
 * <h3>Example:</h3>
 * <pre>
 * {
 *   "msgType": "NOTIFICATION",
 *   "objectType": "CLIENT_COUNT",
 *   "senderPID": 101,
 *   "senderRole": "CHATSERVER",
 *   "targetRole": "PRIMARY",
 *   "payload": 45
 * }
 * </pre>
 *
 * @param <T> The type of data being sent in the payload (e.g., Integer for client count).
 */
public class NotificationMessage<T> extends BaseAddrServerMessage<T> {

    // Jackson Constructor
    @JsonCreator
    public NotificationMessage(
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
     * Private constructor to prevent direct instantiation and enforce the use of factory methods.
     *
     * @param objectType The type of data being sent in the payload.
     * @param senderPID The process ID of the sender.
     * @param senderRole The role of the sender (e.g., CHATSERVER).
     * @param targetRole The intended recipient's role (e.g., PRIMARY).
     * @param payload The data being sent in the payload.
     */
    private NotificationMessage(String objectType, long senderPID, String senderRole, String targetRole, T payload) {
        super(0, MessageTypes.NOTIFICATION, objectType, senderPID, senderRole, targetRole, payload);
    }

    /**
     * Private constructor to prevent direct instantiation and enforce the use of factory methods.
     *
     * @param messageID   A globally unique identifier for the message. Use {@link MessageIDGenerator} for generation.
     * @param objectType The type of data being sent in the payload.
     * @param senderPID The process ID of the sender.
     * @param senderRole The role of the sender (e.g., CHATSERVER).
     * @param targetRole The intended recipient's role (e.g., PRIMARY).
     * @param payload The data being sent in the payload.
     */
    private NotificationMessage(long messageID, String objectType, long senderPID, String senderRole, String targetRole, T payload) {
        super(messageID, MessageTypes.NOTIFICATION, objectType, senderPID, senderRole, targetRole, payload);
    }



    /**
     * Factory method for creating a notification message for client count updates.
     * <p>
     * This method creates a notification message with a payload of the current client count.
     * It is typically used when a {@code CHATSERVER} notifies a {@code PRIMARY} server about
     * the current number of active clients.
     * </p>
     *
     * @param senderPID The process ID of the sender (usually a {@code CHATSERVER}).
     * @param currentClientCount The current count of clients connected to the server.
     * @return A {@code NotificationMessage} containing the client count.
     */
    public static NotificationMessage<Integer> clientCountNotification(long senderPID, Integer currentClientCount) {
        return new NotificationMessage<>(ObjectTypes.CLIENT_COUNT, senderPID, Roles.CHATSERVER, Roles.PRIMARY, currentClientCount);
    }


    /**
     * Factory method for creating a failure notification message in response to a failed request.
     * <p>
     * This method creates a {@code NotificationMessage} with an {@code ObjectType} of {@code REQUEST_FAILURE}
     * and a payload containing the message ID of the original request that triggered the failure.
     * It is typically used when the {@code PRIMARY} AddressingServer notifies a process that their
     * previously sent request could not be completed.
     * </p>
     *
     * @param senderPID        The process ID of the sender (usually a {@code CHATSERVER}).
     * @param requestMessageID The message ID from the original request message.
     * @return A {@code NotificationMessage} indicating a failed request.
     */
    public static NotificationMessage<Long> requestFailedNotification(long senderPID, long requestMessageID) {
        return new NotificationMessage<>(ObjectTypes.REQUEST_FAILURE, senderPID, Roles.PRIMARY, Roles.CHATSERVER, requestMessageID);
    }


}
