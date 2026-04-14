package io.github.cpsc559.team16.common.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.cpsc559.team16.common.exceptions.ChatServerFullException;

/**
 * Used by the AddressingServer class to represent a chat server in the
 * distributed network.
 * <p>
 * An instance of {@code ChatServerRecord} is created for each chat server when
 * it registers with the Primary Addressing Server.
 */
public class ChatServerRecord extends ServerRecord {


    /**
     * The maximum number of client connections allowed for this server instance.
     * <p>
     * This value is dynamic and specifically chosen for each server, reflecting
     * real-world
     * scenarios where different servers may have different resource constraints.
     * </p>
     */
    private final int maxClientCount;
    /**
     * The number of active client connections currently held by the chat server.
     */
    private int clientCount;

    /**
     * Represents the operational status of a chat server.
     * <ul>
     * <li>{@code ACTIVE} - Accepting new clients</li>
     * <li>{@code FULL} - Reached maximum client limit</li>
     * <li>{@code INACTIVE} - Marked as failed or disconnected</li>
     * </ul>
     */
    public enum ServerStatus {
        ACTIVE, FULL, INACTIVE
    }

    /**
     * The current availability status of the server.
     */
    private ServerStatus status;


    public int getClientCount() {
        return this.clientCount;
    }

    public ServerStatus getStatus() {
        return this.status;
    }

    // TODO Decide if we are assigning a subset of chat servers to each chat server
    // as its peers -> create instance record.
    /**
     * Constructs a new {@code ChatServerRecord} instance with the specified
     * parameters, a default
     * {@code ACTIVE status} and a starting {@code clientCount} of zero.
     *
     * @param serverID       The unique identifier (key) of the chat server. Needed
     *                       for the HashMap of ChatServerRecord records kept by
     *                       Addressing Servers.
     * @param hostAddress    The network (IP) address of the chat server.
     * @param clientPort     The port used for communication with client processes.
     * @param peerPort       The port used for peer-to-peer communication with other
     *                       chat servers.
     * @param maxClientCount The maximum amount of persistent client connections
     *                       this server should be assigned.
     */
    @JsonCreator
    public ChatServerRecord(
            @JsonProperty("pid") Long serverID,
            @JsonProperty("hostAddress") String hostAddress,
            @JsonProperty("clientPort") int clientPort,
            @JsonProperty("peerPort") int peerPort,
            @JsonProperty("maxClientCount") int maxClientCount) {
        /*
         * `serverID` is used as a key for the key:value pairs that make up the unified
         * (consistent)
         * ChatServerRecord HashMap of records kept by Addressing Servers.
         */
        super(serverID, hostAddress, peerPort, clientPort);
        this.clientCount = 0;
        this.maxClientCount = maxClientCount;
        this.status = ServerStatus.ACTIVE;
    }

    /**
     * Attempts to add a new client to the server.
     * Any calling code should perform its own capacity check prior to invocation of
     * this method.
     * It exists as a secondary guard only.
     * <ul>
     * <li>If the server is full, this method throws a
     * {@link ChatServerFullException}.</li>
     * <li>Otherwise, the client is added successfully.</li>
     * </ul>
     * 
     * @throws ChatServerFullException if the server has reached maximum capacity.
     */
    public void addClient() throws ChatServerFullException {
        if (isFull()) {
            System.err.println("WARNING - Server FULL: " + hostAddress + " reporting to Addressing Server.");
            throw new ChatServerFullException("Server FULL: " + hostAddress + ":" + clientPort);
        }
        clientCount++;
        try {
            updateStatus();
        } catch (IllegalStateException e) {
            System.err.println("Server status update failed: " + e.getMessage());
        }
    }

    /**
     * Removes a specified number of clients from the server.
     * <p>
     * Ensures {@code clientCount} never goes below zero and updates the chat
     * server's status.
     *
     * @param numClients The number of clients to remove.
     * @see #updateStatus() Possible status changes.
     */
    public void removeClients(int numClients) {
        clientCount = Math.max(0, clientCount - numClients);
        try {
            updateStatus();
        } catch (IllegalStateException e) {
            System.err.println("Server status update failed: " + e.getMessage());
        }
    }

    /**
     * Checks if the server has reached its maximum client capacity.
     * <p>
     * This method returns {@code true} if the number of active clients
     * ({@code clientCount}) is equal to or greater than {@code maxClientCount}.
     *
     * @return {@code true} if the server is full, {@code false} otherwise.
     */
    @JsonIgnore
    public boolean isFull() {
        return clientCount >= maxClientCount;
    }

    /**
     * Updates the server's status based on the current number of active client
     * connections.
     * <ul>
     * <li>If {@code clientCount >= maxClientCount}, the server is marked as
     * "FULL".</li>
     * <li>Otherwise, the server remains "ACTIVE".</li>
     * </ul>
     * <p>
     * Throws an exception if the transition is invalid:
     * <ul>
     * <li>Cannot transition to {@code FULL} unless the server was previously
     * {@code ACTIVE}.</li>
     * <li>Cannot transition to {@code ACTIVE} from {@code INACTIVE} without
     * reactivation.</li>
     * </ul>
     * </p>
     *
     * @throws IllegalStateException If an invalid status transition is attempted.
     * @see #markAsActive() For reactivating server status.
     * @see #markAsInactive() For deactivating server during disruptions.
     */
    private void updateStatus() throws IllegalStateException {
        ServerStatus newStatus = isFull() ? ServerStatus.FULL : ServerStatus.ACTIVE;
        if (newStatus == ServerStatus.FULL && status != ServerStatus.ACTIVE) {
            throw new IllegalStateException("Cannot transition to FULL from " + status);
        }
        if (status == ServerStatus.INACTIVE && newStatus == ServerStatus.ACTIVE) {
            throw new IllegalStateException("Must use the reactivateServer method directly.");
        }
        this.status = newStatus;
        System.out.println("Status updated to: " + status);
    }

    /**
     * Marks the server as "INACTIVE", indicating that it is no longer available for
     * client connections.
     * <p>
     * This method should be called by the Addressing Server when it detects that
     * this server has failed.
     * </p>
     * An inactive server will not be assigned new clients.
     * 
     * @see #markAsActive() For re-activating a registered server.
     */
    public void markAsInactive() {
        this.status = ServerStatus.INACTIVE;
    }

    /**
     * Marks the server as "ACTIVE", indicating that a server is available for
     * client connections.
     * <p>
     * This method should be called by the Addressing Server when it is contacted by
     * a registered server that has
     * recovered from disconnection and subsequent INACTIVE designation from the
     * system.
     * </p>
     * A server should only be labeled ACTIVE if it is capable of accepting new
     * client connections.
     * This method assumes the proper checks have made to ensure this is true.
     * Default behaviour sets the {@code clientCount} to zero as a previously
     * {@code INACTIVE} server should have no connections.
     * 
     * @throws IllegalStateException If an invalid status transition is attempted.
     */
    public void markAsActive() {
        if (status == ServerStatus.INACTIVE) {
            clientCount = 0;
            status = ServerStatus.ACTIVE;
            System.out.printf("Server with address: %s - is reactivated. Client count reset.%n", hostAddress);
        } else {
            throw new IllegalStateException("Cannot mark server as ACTIVE unless previously INACTIVE.");
        }
    }

    public void setPID(Long serverPID) {
        super.setPID(serverPID);
    }

    public void setClientCount(int newClientCount) {
        this.clientCount = newClientCount;
    }

    public int getMaxClientCount() {
        return maxClientCount;
    }

    public void setHostAddress(String hostAddress) {
        super.setHostAddress(hostAddress);
    }

}
