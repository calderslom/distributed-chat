package io.github.cpsc559.team16.addressingserver;

import io.github.cpsc559.team16.common.dto.ServerRole;
import io.github.cpsc559.team16.common.dto.PrimaryAddress;
import io.github.cpsc559.team16.common.messaging.Roles;
import io.github.cpsc559.team16.common.utilities.NetworkUtils;
import io.github.cpsc559.team16.common.utilities.PrimaryDiscoveryReader;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class AddrServerConfig {

    /**
     * Each AddressingServer process has a distinct id amongst its peers.
     * {@code pid} is used as a 'tie-breaker' during leader elections.
     */
    private volatile long pid;

    /**
     * The network address of this Addressing Server.
     * <par>
     * The Primary Addressing Server used to this address to the
     * A-record in the static DNS.
     * Now it writes it to the shared volume in a text file.
     * </par>
     */
    private final String hostAddress;

    /**
     * The port used for client connections.
     * Clients use this port to connect and send messages.
     */
    private final int clientPort;

    /**
     * The port reserved for peer-to-peer communication amongst the
     * Primary Addressing Server and it's backups.
     */
    private final int replicaPort;

    /**
     * The port used for communicating with Chat Servers.
     * This should be the port the chat server used to register itself with the Addressing Server.
     */
    private final int chatServerPort;


    /**
     * AddressingServer processes are either:
     * <ul>
     *     <li>PRIMARY - the leader process in charge of coordinating connections in the network.</li>
     *     <li>REPLICA - a `Passive Replica` receiving and retrieving updates.</li>
     * </ul>
     */
    private ServerRole role;

    /**
     * The network address of the PRIMARY Addressing Server.
     * REPLICA instances use this to identify the target for registration and synchronization.
     */
    private String primaryHostAddress;

    /**
     * The port on the PRIMARY Addressing Server reserved for replica connections.
     * Used by REPLICAs during the initial handshake.
     */
    private int primaryReplicaPort;


    /**
     * The configuration object for the Addressing Server.
     * <p>
     * This instance of {@link AddrServerConfig} is responsible for reading and storing
     * environment variables and other runtime settings required for initializing the server.
     * It provides access to essential network parameters such as:
     * <ul>
     *     <li>Host address</li>
     *     <li>Client port</li>
     *     <li>Replica port</li>
     *     <li>Chat server port</li>
     *     <li>Server role (PRIMARY or BACKUP)</li>
     *     <li>Server pid (Process ID)</li>
     * </ul></p>
     */
    public AddrServerConfig() {
        // Role is required at startup to identify the type of AddressingServer process.
        String roleEnv = System.getenv().getOrDefault("AS_ROLE", "REPLICA").toUpperCase();
        // Validation Guard
        if (!Roles.isValid(roleEnv)) {
            String errorMsg = String.format(
                    "FATAL: Invalid AS_ROLE '%s' provided. Must be one of: %s, %s",
                    roleEnv, Roles.PRIMARY, Roles.REPLICA
            );
            System.err.println(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }
        this.role = roleEnv.equals("PRIMARY") ? ServerRole.PRIMARY : ServerRole.REPLICA;
        // Retrieve hostname dynamically.
        this.hostAddress = NetworkUtils.getSerializedIdentity(roleEnv);

        // Use standard defaults for ports unless specified
        this.clientPort = Integer.parseInt(System.getenv().getOrDefault("AS_CLIENT_PORT", "49800"));
        this.replicaPort = Integer.parseInt(System.getenv().getOrDefault("AS_REPLICA_PORT", "49801"));
        this.chatServerPort = Integer.parseInt(System.getenv().getOrDefault("AS_CHATSERVER_PORT", "49802"));

        // Primary Details: Initialized as null/empty; will be filled by DiscoveryReader for all REPLICA processes
        this.primaryHostAddress = null;
        this.primaryReplicaPort = -1;
    }

    /**
     * Changes the role of an AddressingServer in the DS.
     * <p>During failover, a new PRIMARY addressing server must
     * be elected and change its role to reflect its new status.</p>
     *
     * @param role The type of ServerRole for this AddressingServer.
     */
    public void setRole(ServerRole role) {
        this.role = role;
    }

    /**
     * AddressingServer processes are either:
     * <ul>
     *     <li>PRIMARY - the leader process in charge of coordinating connections in the network.</li>
     *     <li>REPLICA - a `Passive Replica` receiving and retrieving updates.</li>
     * </ul>
     */
    public ServerRole getRole() {
        return role;
    }


    /**
     * Sets the unique process identifier (PID) for this server.
     * <p>
     * This is typically called by the Primary during the registration handshake
     * or initialized during the startup of a Primary process.
     * </p>
     * * @param pid the unique network-wide ID to assign to this server.
     */
    public void setPID(Long pid) {
        this.pid = pid;
    }

    /**
     * Retrieves the unique process identifier (PID) for this server.
     * * @return the assigned {@code Long} PID, or {@code 0L} if the process is not yet registered.
     */
    public Long getPID() {
        return this.pid;
    }

    /**
     * Returns the network host address (IP or hostname) where this server is reachable.
     * * @return the host address string used for incoming network connections.
     */
    public String getHostAddress() {
        return hostAddress;
    }

    /**
     * Returns the port number dedicated to handling Client (end-user) connections.
     * * @return the integer port used for client-to-server communication.
     */
    public int getClientPort() {
        return clientPort;
    }

    /**
     * Returns the port number dedicated to Peer-to-Peer communication.
     * <p>
     * This port is used for replication, consensus protocols, and heartbeats
     * between AddressingServer instances.
     * </p>
     * * @return the integer port used for replica-to-primary communication.
     */
    public int getReplicaPort() {
        return replicaPort;
    }

    /**
     * Returns the port number dedicated to ChatServer interactions.
     * <p>
     * ChatServers connect to this port to register themselves and receive
     * topology updates from the AddressingServer.
     * </p>
     * * @return the integer port used for chat-server-to-addressing-server communication.
     */
    public int getChatServerPort() {
        return chatServerPort;
    }


    /**
     * Returns the host address of the current Primary Addressing Server.
     *
     * @return the IP or hostname of the Primary.
     */
    public String getPrimaryHostAddress() {
        return primaryHostAddress;
    }

    /**
     * Updates the recorded address of the Primary Addressing Server.
     *
     * @param address the new Primary host address.
     */
    public void setPrimaryHostAddress(String address) {
        this.primaryHostAddress = address;
    }

    /**
     * Returns the port used by the Primary for peer/replica handshakes.
     *
     * @return the peer port of the Primary.
     */
    public int getPrimaryReplicaPort() {
        return primaryReplicaPort;
    }

    /**
     * Updates the recorded peer port of the Primary Addressing Server.
     *
     * @param port the new Primary peer port.
     */
    public void setPrimaryReplicaPort(int port) {
        this.primaryReplicaPort = port;
    }

    /**
     * Re-reads the shared discovery file to update the current known Primary.
     * This is called during initialization and after a detected failover.
     *
     * @return true if a PRIMARY was found and its details were loaded, false otherwise.
     */
    public boolean refreshPrimaryDetails() {
        try {
            PrimaryAddress details = PrimaryDiscoveryReader.readPrimaryDetails();
            if (details != null) {
                this.primaryHostAddress = details.hostAddress();
                this.primaryReplicaPort = details.replicaPort();
                return true;
            }
        } catch (IOException e) {
            System.err.println("Config: Error reading primary addressing server discovery file: " + e.getMessage());
        }

        // If we reach here, no primary was found. Clear old stale data.
        this.primaryHostAddress = null;
        this.primaryReplicaPort = -1;
        return false;
    }

}
