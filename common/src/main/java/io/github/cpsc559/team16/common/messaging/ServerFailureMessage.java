package io.github.cpsc559.team16.common.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ServerFailureMessage<T> extends BaseAddrServerMessage<T> {

    // Jackson Constructor
    @JsonCreator
    public ServerFailureMessage(
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
     * Constructs a new failed server message.
     *
     * @param objectType Describes what action is being acknowledged (e.g.,
     *                   "Registration", "Update").
     * @param senderPID  The ID of the process sending the acknowledgment.
     * @param senderRole The role of the sender (e.g., PRIMARY, REPLICA,
     *                   CHATSERVER).
     * @param targetRole The role of the process being acknowledged (e.g.,
     *                   CHATSERVER, REPLICA).
     * @param payload    A short payload string. It can be anything you want, but
     *                   you'll have to handle it appropriately.
     */
    public ServerFailureMessage(String objectType, long senderPID, String senderRole, String targetRole, T payload) {
        super(0, MessageTypes.SERVERFAILURE, objectType, senderPID, senderRole, targetRole, payload);
    }

    /**
     * Constructs a new {@code ServerFailureMessage}, typically used within the
     * Addressing Server module
     * to notify peers of a detected failure in the network.
     * <p>
     * This overload accepts a unique {@code messageID}, allowing it to be tracked
     * and acknowledged
     * as part of the sequential consistency mechanism between the PRIMARY and its
     * REPLICA processes.
     * It is strongly recommended to generate this ID using the
     * {@link MessageIDGenerator} to ensure uniqueness
     * across distributed processes.
     * </p>
     *
     * @param messageID  A globally unique identifier for the message. Use
     *                   {@link MessageIDGenerator} for generation.
     * @param objectType A string representing the type of failure event (e.g.,
     *                   "HeartbeatTimeout", "ChannelClosed").
     * @param senderPID  The process ID of the sender issuing the failure
     *                   notification.
     * @param senderRole The role of the sender (e.g., PRIMARY, REPLICA,
     *                   CHATSERVER).
     * @param targetRole The intended recipient's role (e.g., REPLICA, CHATSERVER).
     * @param payload    A descriptive payload (typically a {@code Long}
     *                   representing the failed server's PID or
     *                   other relevant context). Must be interpreted by the
     *                   receiver.
     */
    public ServerFailureMessage(long messageID, String objectType, long senderPID, String senderRole, String targetRole,
            T payload) {
        super(messageID, MessageTypes.SERVERFAILURE, objectType, senderPID, senderRole, targetRole, payload);
    }

    /**
     * Creates a notification message indicating that a ChatServer process has
     * failed.
     * This notification is intended for any server in the network to inform others
     * that a ChatServer
     * is no longer active. Recipients should terminate any connections (channels)
     * with the failed process
     * and remove its record from their local registry.
     *
     * <p>
     * This factory method generates a {@code ServerFailureMessage} with the object
     * type set to
     * {@link ObjectTypes#CHATSERVER_FAILURE}. The sender and receiver roles should
     * be specified using the
     * allowed values defined in {@link Roles} (for example, {@code Roles.PRIMARY},
     * {@code Roles.REPLICA},
     * or {@code Roles.CHATSERVER}).
     * </p>
     *
     * @param senderPID    the process ID of the server sending this notification
     * @param senderRole   the role of the sender (for example,
     *                     {@code Roles.PRIMARY})
     * @param receiverRole the role of the intended recipient (for example,
     *                     {@code Roles.REPLICA})
     * @param failedPID    the process ID of the failed ChatServer process
     * @return a {@code ServerFailureMessage<Long>} encapsulating the failure event
     *         for a ChatServer
     */
    public static ServerFailureMessage<Long> chatServerFailed(long senderPID, String senderRole, String receiverRole,
            Long failedPID) {
        return chatServerFailed(0, senderPID, senderRole, receiverRole, failedPID);
    }

    /**
     * Creates a notification message indicating that a ChatServer process has
     * failed.
     * This notification is intended for any server in the network to inform others
     * that a ChatServer
     * is no longer active. Recipients should terminate any connections (channels)
     * with the failed process
     * and remove its record from their local registry.
     *
     * <p>
     * This factory method generates a {@code ServerFailureMessage} with the object
     * type set to
     * {@link ObjectTypes#CHATSERVER_FAILURE}. The sender and receiver roles should
     * be specified using the
     * allowed values defined in {@link Roles} (for example, {@code Roles.PRIMARY},
     * {@code Roles.REPLICA},
     * or {@code Roles.CHATSERVER}).
     * </p>
     *
     * @param messageID    A globally unique identifier for the message. Use
     *                     {@link MessageIDGenerator} for generation.
     * @param senderPID    the process ID of the server sending this notification
     * @param senderRole   the role of the sender (for example,
     *                     {@code Roles.PRIMARY})
     * @param receiverRole the role of the intended recipient (for example,
     *                     {@code Roles.REPLICA})
     * @param failedPID    the process ID of the failed ChatServer process
     * @return a {@code ServerFailureMessage<Long>} encapsulating the failure event
     *         for a ChatServer
     */
    public static ServerFailureMessage<Long> chatServerFailed(long messageID, long senderPID, String senderRole,
            String receiverRole, Long failedPID) {
        return new ServerFailureMessage<>(messageID, ObjectTypes.CHATSERVER_FAILURE, senderPID, senderRole,
                receiverRole, failedPID);
    }

    /**
     * Creates a notification message indicating that an AddressingServer process
     * has failed.
     * This notification is intended for any server in the network to inform others
     * that an AddressingServer
     * is no longer active. Recipients should terminate any connections (channels)
     * with the failed process
     * and remove its record from their local registry.
     *
     * <p>
     * This factory method generates a {@code NotificationMessage} with the object
     * type set to
     * {@link ObjectTypes#ADDRSERVER_FAILURE}. The sender and receiver roles should
     * be specified using the
     * allowed values defined in {@link Roles} (for example, {@code Roles.PRIMARY},
     * {@code Roles.REPLICA},
     * or {@code Roles.CHATSERVER}).
     * </p>
     *
     * @param senderPID    the process ID of the server sending this notification
     * @param senderRole   the role of the sender (for example,
     *                     {@code Roles.PRIMARY})
     * @param receiverRole the role of the intended recipient (for example,
     *                     {@code Roles.REPLICA})
     * @param failedPID    the process ID of the failed AddressingServer process
     * @return a {@code ServerFailureMessage<Long>} encapsulating the failure event
     *         for an AddressingServer
     */
    public static ServerFailureMessage<Long> addrServerFailed(long senderPID, String senderRole, String receiverRole,
            Long failedPID) {
        return addrServerFailed(0, senderPID, senderRole, receiverRole, failedPID);
    }

    /**
     * Creates a notification message indicating that an AddressingServer process
     * has failed.
     * This notification is intended for any server in the network to inform others
     * that an AddressingServer
     * is no longer active. Recipients should terminate any connections (channels)
     * with the failed process
     * and remove its record from their local registry.
     *
     * <p>
     * This factory method generates a {@code NotificationMessage} with the object
     * type set to
     * {@link ObjectTypes#ADDRSERVER_FAILURE}. The sender and receiver roles should
     * be specified using the
     * allowed values defined in {@link Roles} (for example, {@code Roles.PRIMARY},
     * {@code Roles.REPLICA},
     * or {@code Roles.CHATSERVER}).
     * </p>
     *
     * @param messageID    A globally unique identifier for the message. Use
     *                     {@link MessageIDGenerator} for generation.
     * @param senderPID    the process ID of the server sending this notification
     * @param senderRole   the role of the sender (for example,
     *                     {@code Roles.PRIMARY})
     * @param receiverRole the role of the intended recipient (for example,
     *                     {@code Roles.REPLICA})
     * @param failedPID    the process ID of the failed AddressingServer process
     * @return a {@code ServerFailureMessage<Long>} encapsulating the failure event
     *         for an AddressingServer
     */
    public static ServerFailureMessage<Long> addrServerFailed(long messageID, long senderPID, String senderRole,
            String receiverRole, Long failedPID) {
        return new ServerFailureMessage<>(messageID, ObjectTypes.ADDRSERVER_FAILURE, senderPID, senderRole,
                receiverRole, failedPID);
    }

}
