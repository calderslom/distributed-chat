package io.github.cpsc559.team16.common.messaging;

import io.github.cpsc559.team16.common.dto.AddrServerRecord;
import io.github.cpsc559.team16.common.dto.ChatServerRecord;
import io.github.cpsc559.team16.common.dto.ElectionVote;
import jdk.jshell.execution.Util;

/**
 * Constants representing standardized object types used in message payloads.
 * These should match the values used in {@code BaseAddrServerMessage.objectType}
 * and must align with switch statements in deserialization and dispatcher logic.
 *
 * <p>
 *     <strong>NOTE:</strong> There are many times where the BaseAddrServerMessage class
 *     is used to "safe-cast" the payload into the object referenced in the ObjectType field.
 *     It is of paramount importance this feature is used with care.
 * </p>
 *
 */
public class ObjectTypes {

    /**
     * The object type for an AddressingServer record.
     */
    public static final String ADDR_SERVER_RECORD = "AddrServerRecord";

    /**
     * The object type for a ChatServer record.
     */
    public static final String CHAT_SERVER_RECORD = "ChatServerRecord";


    /**
     * The object type for representing client count values.
     */
    public static final String CLIENT_COUNT = "ClientCount";


    /**
     * The object type for representing a Long value in message payloads.
     */
    public static final String LONG = "Long";

    /**
     * The object type for representing a String value in message payloads.
     */
    public static final String STRING = "String";

    /**
     * The object type used when no specific object type is applicable.
     */
    public static final String NONE = "NONE";

    /**
     * This is used for inter-process communication for failure related messaging. It is used in combination
     * with {@link MessageTypes#UPDATE} to notify all servers (excluding the PRIMARY AddressingServer)
     * of a failed process - so that they can close any connections they have to that process, and remove the record
     * for that process from their registry.
     * <p>
     * <strong>NOTE:</strong> This is not the correct way of notifying the {@code PRIMARY AddressingServer}
     * of a failed server. That responsibility is handled by {@link MessageTypes#SERVERFAILURE}.
     * </p>
     */
    public static final String CHATSERVER_FAILURE = "ChatServerFailure";

    /**
     * This is used for inter-process communication for failure related messaging. It is used in combination
     * with {@link MessageTypes#UPDATE} to notify all servers (excluding the PRIMARY AddressingServer)
     * of a failed process - so that they can close any connections they have to that process, and remove the record
     * for that process from their registry.
     * <p>
     * <strong>NOTE:</strong> This is not the correct way of notifying the {@code PRIMARY AddressingServer}
     * of a failed server. That responsibility is handled by {@link MessageTypes#SERVERFAILURE}.
     * </p>
     */
    public static final String ADDRSERVER_FAILURE = "AddrServerFailure";

    public static final String REQUEST_FAILURE = "RequestFailed";

    /**
     * The object type for representing a vote in a leader election process.
     */
    public static final String ELECTION_VOTE = "ElectionVote";


    // Preventing any possible instantiation of the utility class
    private ObjectTypes() {}

    /**
     * Maps known object types to their respective Java classes.
     *
     * @param objectType The object type field from the message.
     * @return The corresponding class type or {@code Object.class} if no specific type is needed.
     */
    public static Class<?> getPayloadClass(String objectType) {
        return switch (objectType) {
            case ADDR_SERVER_RECORD -> AddrServerRecord.class;
            case CHAT_SERVER_RECORD -> ChatServerRecord.class;
            case CLIENT_COUNT -> Integer.class;         // Client count in {@code ChatServerRecord} is an integer.
            case CHATSERVER_FAILURE -> Long.class;      // Failure objects are tied to the failed PID which is a Long
            case ADDRSERVER_FAILURE -> Long.class;      // Failure objects are tied to the failed PID which is a Long
            case ELECTION_VOTE -> ElectionVote.class;

            // Primitive and base types
            case LONG -> Long.class;
            case STRING -> String.class;
            case NONE -> Object.class;

            // ACK types — Payloads are all strings (unless otherwise noted)
            case    AckObjectTypes.HOSTADDRESS,
                    AckObjectTypes.NOHOST,
                    AckObjectTypes.DEMOTED,
                    AckObjectTypes.OK -> String.class;


            // Request types — Payloads are all strings (unless otherwise noted)
            case    RequestObjectTypes.ADDR_SERVER_RECORDS,
                    RequestObjectTypes.CHAT_SERVER_RECORDS,
                    RequestObjectTypes.ALL_SERVER_RECORDS -> Void.class;

            case    RequestObjectTypes.SINGLE_AS_RECORD,
                    RequestObjectTypes.SINGLE_CS_RECORD-> Long.class;

            // Primary response types
            case    ResponseObjectTypes.ALL_PEER_PIDS,
                    ResponseObjectTypes.ALL_CS_PIDS -> java.util.Set.class;

            /*
             * REGISTERED is the ACK response given by the Primary AddressingServer when a process is successfully registered
             * into the network - a unique process ID is generated, a record is created, stored and broadcasted to all other
             * servers in the network, and finally, a response is sent to the newly registered process with it's PID as the payload.
              */
            case AckObjectTypes.REGISTERED,
                 AckObjectTypes.SYNCHRONIZED-> Long.class;
            

            case AckObjectTypes.REPLICATED -> Boolean.class;

            default -> Object.class; // fallback
        };
    }


}
