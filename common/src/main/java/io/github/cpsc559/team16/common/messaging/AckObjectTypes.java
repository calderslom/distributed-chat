package io.github.cpsc559.team16.common.messaging;

/**
 * All ACK responses are MessageTypes.ACK. However - they can also declare <strong>what</strong> type of event
 * they are responding to by placing a string in the {@code objectType} field.
 * The constants in this class represent the <strong>types of responses</strong> Ack messages can contain.
 * <p>
 * All ACK responses have a String payload - any other type of payload should use a different
 * {@code msgType} declaration.
 *</p>
 {@link BaseAddrServerMessage}.
 * </p>
 */
public class AckObjectTypes {

    /**
     * Used to send an acknowledgement from the new PRIMARY AddressingServer to a REPLICA
     * process that it has been synchronized following a leadership election and a persistent
     * connection has been established.
     */
    public static final String SYNCHRONIZED = "Synchronized";

    /**
     * Used to send an acknowledgement to a ChatServer or AddressingServer process that
     * it has been registered in the distributed network.
     */
    public static final String REGISTERED = "Registered";

    /**
     * Used to send an acknowledgement to a ChatServer or AddressingServer process that
     * it has been deregistered in the distributed network.
     */
    public static final String DEREGISTERED = "Deregistered";

    /**
     * Used to send an acknowledgment from a Replica to the Primary AddressingServer process
     * that it has received a message and updated its state.
     */
    public static final String REPLICATED = "Replicated";


    /**
     * Used to send an acknowledgement to a Client containing the host address of a chat-server
     * that is {@code ACTIVE} and accepting new clients - i.e. activeClients < maxClientCount.
     * <p>
     * This acknowledgment is sent to a Client following their initial connection request to the AddressingServer.
     * </p>
     */
    public static final String HOSTADDRESS = "HostAddress";
    /**
     * Used to send an acknowledgement to a Client that there are no ChatServer's currently
     * accepting new clients.
     * <p>
     * This acknowledgment is sent to a Client following their initial connection request to the AddressingServer.
     * </p>
     */
    public static final String NOHOST = "NoHost";

    public static final String DEMOTED = "Demoted";

    public static final String OK = "Ok";

    // Preventing any possible instantiation of the utility class
    private AckObjectTypes() {}

}
