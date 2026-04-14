package io.github.cpsc559.team16.addressingserver;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.github.cpsc559.team16.common.dto.AddrServerRecord;
import io.github.cpsc559.team16.common.dto.ServerRole;
import io.github.cpsc559.team16.common.logging.ServerDebugLogger;
import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

public class AddrServerRegistry {

    /** The AddressingServer instance that owns this registry */
    private final AddressingServer server;


    /**
     * Constructor for AddrServerRegistry.
     * <p>
     * This constructor initializes the registry with a reference to the AddressingServer instance
     * that owns it. This allows the registry to interact with the server for various operations.
     * </p>
     *
     * @param server The AddressingServer instance that owns this registry.
     */
    public AddrServerRegistry(AddressingServer server) {
        this.server = server;
    }

    /**
     * This Hashmap is used by each AddressingServer to keep {@code AddrServerRecord}
     * records of all other addressing servers in the network.
     */
    private Map<Long, AddrServerRecord> addrServerRecords = new ConcurrentHashMap<>();

    public Map<Long, AddrServerRecord> getRecords() {
        return addrServerRecords;
    }

    public void setAddrServerRecords(Map<Long, AddrServerRecord> newAddrServerRecords) {
        this.addrServerRecords = newAddrServerRecords;
    }

    /**
     * Registers a new Addressing Server record by storing its {@link AddrServerRecord} object.
     * <p>
     * This method adds the provided server record to the {@code addrServerRecords} map,
     * using its unique process ID as the key.
     * </p>
     *
     * @param id     The unique process ID of the Addressing Server being registered.
     * @param record The {@link AddrServerRecord} object containing the server's details
     *               (e.g., host address, ports, role).
     */
    public void putAddrServerRecord(Long id, AddrServerRecord record) {
        addrServerRecords.put(id, record);
        ServerDebugLogger.printAddrServerAction("Inserting", record);
    }

    /**
     * Updates an existing AddrServerRecord if one with the same ID exists,
     * otherwise inserts the new record.
     *
     * @param record The AddrServerRecord to insert or update.
     */
    public void updateOrInsertRecord(AddrServerRecord record) {
        Long id = record.getPID();
        if (id == null) {
            System.err.println("AddrServerRecord has a null PID. Cannot update or insert record.");
            return;
        }
        AddrServerRecord existing = addrServerRecords.get(id);
        if (existing != null) {
            ServerDebugLogger.printAddrServerAction("Updating", record);
        } else {
            ServerDebugLogger.printAddrServerAction("Inserting", record);
        }
        addrServerRecords.put(id, record);
    }

    /**
     * Validates an incoming {@link AddrServerRecord} against the local registry to ensure consistency.
     * <p>
     * This method is a critical security and integrity check used during the
     * {@code SYNCHRONIZE} handshake. It verifies that the process claiming a specific PID
     * matches a record stored in this registry across all functional fields:
     * <ul>
     * <li><b>Identity:</b> The Process ID (PID).</li>
     * <li><b>Role:</b> The {@link ServerRole} (PRIMARY/REPLICA).</li>
     * <li><b>Topology:</b> The Host Address and all three communication ports (Client, Peer, Chat).</li>
     * </ul>
     * </p>
     * <p>
     * This record is the single source of truth - If any field mismatches,
     * it indicates a state conflict (e.g. a process attempting to
     * spoof an identity, a split-brain scenario, or a stale registry record).
     * </p>
     *
     * @param externalRecord the record provided by the connecting AddressingServer process.
     * @return {@code true} if the external record perfectly matches the registry's state;
     * {@code false} if no record exists for that PID or if a field mismatch is detected.
     */
    public boolean validateReplicaIdentity(AddrServerRecord externalRecord) {
        AddrServerRecord registryRecord = this.getRecords().get(externalRecord.getPID());

        if (registryRecord == null) {
            return false;
        }

        return externalRecord.getPID().equals(registryRecord.getPID()) &&
                externalRecord.getHostAddress().equals(registryRecord.getHostAddress()) &&
                externalRecord.getClientPort() == registryRecord.getClientPort() &&
                externalRecord.getPeerPort() == registryRecord.getPeerPort() &&
                externalRecord.getChatServerPort() == registryRecord.getChatServerPort() &&
                externalRecord.getRole().equals(registryRecord.getRole());
    }

    /**
     * Removes an {@link AddrServerRecord} from the registry using the provided process ID (PID).
     * <p>
     * This method attempts to remove the record associated with the given {@code pid} from the internal
     * {@code addrServerRecords} map. If a record is found and removed, a success message is printed.
     * Otherwise, a warning is issued to indicate that no record existed for the specified PID.
     * </p>
     *
     * @param pid the unique process ID of the AddressingServer to remove from the registry.
     * @return true if a record corresponding to the PID was actually found and removed; false otherwise.
     *
     */
    public boolean removeRecordByKey(Long pid) {
        AddrServerRecord record = addrServerRecords.remove(pid);
        if (record != null) {
            System.out.printf("Successfully removed *AddrServerRecord* for Network Process with PID: %d%n", pid);
            if (record.getRole() == ServerRole.PRIMARY) {
                LeaderElectionManager mgr = server.getLeaderElectionManager();
                System.out.print("WARNING: The removed server was the PRIMARY AddressingServer; ");
                if (mgr.isMidElection()) {
                    System.out.println("an election is underway.");
                } else {
                    mgr.initiateElection();
                    System.out.println("initiating an election.");
                }

            } else {
                System.out.println("The removed server was a REPLICA AddressingServer.");
            }
            return true;
        } else {
            System.out.println("No AddrServerRecord found for PID: " + pid + " — nothing to remove.");
            return false;
        }
    }

    /**
     * Compares the local registry keys against a set of active PIDs and
     * removes any records that are no longer present in the network.
     *
     * @param activePids The set of PIDs currently recognized by the Primary.
     */
    public void purgeStaleRecords(Set<Long> activePids) {
        int initialSize = addrServerRecords.size();
        addrServerRecords.keySet().removeIf(pid -> !activePids.contains(pid));
        int removedRecordCount = initialSize - addrServerRecords.size();
        if (removedRecordCount  > 0) {
            debug(DEBUG_NORMAL, "Purged " + removedRecordCount  + " stale Addressing Server records from the peer registry.");
        }
    }


    /**
     * Creates a record for an addressing server by inserting its AddrServerRecord record into the registry.
     *
     * @param serverPID the unique process ID generated for this addressing server.
     * @param hostAddress the host address (IP or hostname) of the addressing server.
     * @param clientPort the port used for client communication.
     * @param peerPort the port used for peer-to-peer communication.
     * @param chatServerPort the port used for communication with chat servers.
     * @param role the role of the addressing server being registered (PRIMARY || BACKUP)
     *
     * @return serverPID The unique process ID generated for the newly registered {@code AddrServer}.
     */
    public void registerAddrServer(Long serverPID, String hostAddress, int clientPort, int peerPort, int chatServerPort, ServerRole role) {
        try {
            AddrServerRecord newServer = new AddrServerRecord(serverPID, hostAddress, clientPort, peerPort, chatServerPort, role);
            AddrServerRecord previous = this.addrServerRecords.put(serverPID, newServer);

            if (previous != null) {
                System.err.println("WARNING: A server with ID " + serverPID + " already existed. Overwriting existing entry.");
            } else {
                System.out.println("\n**AddressingServer** successfully registered with ID: " + serverPID + "\n");
            }
        } catch (Exception e) {
            System.err.println("Error registering addressing server: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Returns the greatest process ID (PID) held by any of the active
     * {@code AddressingServer} by iterating through the current set
     * of {@link AddrServerRecord}'s. These records represent the current
     * {@link AddressingServer} network topology.
     * <p>
     *     This method is typically used during failover of an addressing server in conjunction with
     *     a sister method in {@link ChatServerRegistry}, allowing the new leader process to set it's
     *     {@code AddressingServer.pidCounter} to the highest active PID - ensuring no active PIDs are re-assigned
     *     in future registration events.
     * </p>
     *
     * @return a Long representing the highest PID held by any active {@code AddressingServer}
     */
    public Long getMaxPID() {
        Long currentMax = 0L;
        for (AddrServerRecord record: this.addrServerRecords.values())
        {
            if (record.getPID() > currentMax) currentMax = record.getPID();
        }
        return currentMax;
    }



    /**
     * Returns a set of all connected peer process IDs, excluding the given calling process ID.
     * <p>
     * This method is useful when the caller wants to get all *other* peer PIDs in the network,
     * for example when broadcasting to all replicas except itself.
     * </p>
     *
     * @return a {@code Set<Long>} containing the PIDs of all connected peers except the caller.
     */
    public Set<Long> getAllReplicaPIDs() {
        Set<Long> registeredReplicaPIDs = new HashSet<>();
        for (AddrServerRecord record : this.addrServerRecords.values()) {
            if (record.getRole().equals(ServerRole.REPLICA)) {
                registeredReplicaPIDs.add(record.getPID());
            }
        }
        return registeredReplicaPIDs;
    }

    public AddrServerRecord getPrimaryPID() {
        for (AddrServerRecord record : this.addrServerRecords.values()) {
            if (record.getRole().equals(ServerRole.PRIMARY)) {
                return record;
            }
        }
        return null;
    }

//    /**
//     * Logs the details of this AddrServerRecord object to the console for debugging purposes.
//     */
//    public void debugPrintServer(AddrServerRecord s) {
//        System.out.println("---------- AddrServerRecord Record -----------");
//        System.out.printf("Network PID : %s%n", s.getPID());
//        System.out.printf("Host Address : %s%n", s.getHostAddress());
//        System.out.printf("Client Port  : %d%n", s.getClientPort());
//        System.out.printf("Peer Port    : %d%n", s.getPeerPort());
//        System.out.printf("ChatServer Port : %d%n", s.getChatServerPort());
//        System.out.printf("Role : %s%n", s.getRole());
//        System.out.println("---------------------------------------------------");
//    }

//    public void debugPrintAllServers() {
//        System.out.println("|------------- Currently Registered AddressingServer's -------------|");
//        for (AddrServerRecord s : this.addrServerRecords.values()) {
//            debugPrintServer(s);
//        }
//        System.out.println("|--------------------------------------------------------------------|");
//    }


    /**
     * Triggers a detailed diagnostic print of all currently registered Addressing Servers.
     * <p>
     * This method extracts the underlying {@link AddrServerRecord} collection and
     * delegates the formatting and I/O operations to the {@link ServerDebugLogger}.
     * Use this primarily for verifying network topology and role assignments (Primary/Backup)
     * during failover events.
     * </p>
     */
    public void debugPrintAllServers() {
        ServerDebugLogger.printAllAddrServers(this.getRecords().values());
    }

//    public void debugPrintInsertServerPID(AddrServerRecord s) {
//        System.out.println("\t------ Inserting AddrServer Record -------");
//        System.out.printf("\tProcess ID : %s%n", s.getPID());
//        System.out.printf("\tHost Address : %s%n", s.getHostAddress());
//        System.out.println("\t------------------------------------------");
//    }
//
//    public void debugPrintUpdateServerPID(AddrServerRecord s) {
//        System.out.println("\t------ Updating AddrServer Record -------");
//        System.out.printf("\tProcess ID : %s%n", s.getPID());
//        System.out.printf("\tHost Address : %s%n", s.getHostAddress());
//        System.out.println("\t------------------------------------------");
//    }

}
