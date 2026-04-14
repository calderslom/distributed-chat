package io.github.cpsc559.team16.addressingserver;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.github.cpsc559.team16.common.dto.ChatServerRecord;
import io.github.cpsc559.team16.common.logging.ServerDebugLogger;

import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

public class ChatServerRegistry {

    /**
     * A mapping of unique chat server IDs to their corresponding {@link ChatServerRecord} records.
     * <p>
     * A {@code ChatServerRecord} record is created for each {@code ChatServer},
     * and subsequently updated anytime the {@code ChatServer} reports any changes to its state
     * to the Primary {@code AddressingServer} for each registered chat server in the network.
     * </p>
     */
    private Map<Long, ChatServerRecord> chatServerRecords = new ConcurrentHashMap<>();


    public Map<Long, ChatServerRecord> getRecords() {
        return chatServerRecords;
    }

    /**
     * Replaces the current set of chat server records with the new set of records.
     * <p>
     * This method updates the AddressingServer's registry of chat servers by replacing its internal
     * address log with the new map passed as a parameter. This allows for dynamic reconfiguration or recovery
     * by resetting the chat server records.
     * </p>
     *
     * @param newChatServerRecords a {@code Map} of chat server IDs to {@link ChatServerRecord} objects representing the new address log.
     */
    public void setChatServerRecords(Map<Long, ChatServerRecord> newChatServerRecords) {
        this.chatServerRecords = newChatServerRecords;
    }


    public void putChatServerRecord(Long chatServerPID, ChatServerRecord record) {
        this.chatServerRecords.put(chatServerPID, record);
    }

    /**
     * Updates an existing ChatServerRecord if the record being passed in contains
     * a process ID (PID) of a process that has already been registered -
     * otherwise a new record is inserted.
     *
     * @param record The ChatServerRecord to insert or update.
     */
    public void updateOrInsertRecord(ChatServerRecord record) {
        Long id = record.getPID();
        if (id == null) {
            System.err.println("ChatServerRecord has a null PID. Cannot update or insert record.");
            return;
        }
        ChatServerRecord existing = chatServerRecords.get(id);
        if (existing != null) {
            ServerDebugLogger.printChatServerAction("Updating", record);
        } else {
            ServerDebugLogger.printChatServerAction("Inserting", record);
        }
        chatServerRecords.put(id, record);
    }


    /**
     * Removes a {@link ChatServerRecord} from the internal registry using the provided process ID (PID) as the key.
     * <p>
     * This method attempts to remove the record associated with the given {@code pid} from the internal
     * {@code chatServerRecords} map. If the record exists and is successfully removed, a confirmation message
     * is printed to the console. If no record is found for the given {@code pid}, a warning message is printed
     * instead.
     * </p>
     *
     * @param pid the unique process ID of the chat server to remove from the registry.
     */
    public boolean removeRecordByKey(Long pid) {
        ChatServerRecord record = chatServerRecords.remove(pid);
        if (record != null) {
            System.out.printf("Successfully removed *ChatServerRecord* for Network Process with PID: %d%n", pid);
            return true;
        } else {
            System.out.printf("No ChatServerRecord found for PID %d — nothing to remove.%n", pid);
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
        int initialSize = chatServerRecords.size();
        chatServerRecords.keySet().removeIf(pid -> !activePids.contains(pid));
        int removedRecordCount = initialSize - chatServerRecords.size();
        if (removedRecordCount  > 0) {
            debug(DEBUG_NORMAL, "Purged " + removedRecordCount  + " stale ChatServer records from the internal registry.");
        }
    }

    /**
     * Validates an incoming {@link ChatServerRecord} against the local registry to ensure consistency.
     * <p>
     * This method is a critical security and integrity check used during the
     * {@code SYNCHRONIZE} handshake. It verifies that the process claiming a specific PID
     * matches a record stored in this registry across all functional fields:
     * <ul>
     * <li><b>Identity:</b> The Process ID (PID).</li>
     * <li><b>Capacity:</b> The Maximum Client limit.</li>
     * <li><b>Topology:</b> The Host Address and all relevant communication ports (Client, Peer, Addressing).</li>
     * </ul>
     * </p>
     * <p>
     * This record is the single source of truth - If any field mismatches,
     * it indicates a state conflict (e.g. a process attempting to
     * spoof an identity, a split-brain scenario, or a stale registry record).
     * </p>
     *
     * @param externalRecord the record provided by the connecting ChatServer process.
     * @return {@code true} if the external record perfectly matches the registry's state;
     * {@code false} if no record exists for that PID or if a field mismatch is detected.
     */
    public boolean validateChatServerIdentity(ChatServerRecord externalRecord) {
        ChatServerRecord registryRecord = this.getRecords().get(externalRecord.getPID());

        // Check if the PID exists in our registry at all
        if (registryRecord == null) {
            return false;
        }

        // Perform a deep equality check on all critical identity and topology fields
        return externalRecord.getPID().equals(registryRecord.getPID()) &&
                externalRecord.getHostAddress().equals(registryRecord.getHostAddress()) &&
                externalRecord.getClientPort() == registryRecord.getClientPort() &&
                externalRecord.getPeerPort() == registryRecord.getPeerPort() &&
                externalRecord.getMaxClientCount() == registryRecord.getMaxClientCount();
    }


    /**
     * Updates the client count in the {@link io.github.cpsc559.team16.common.dto.ChatServerRecord}
     * associated with the specified ChatServer PID.
     * <p>
     * This method retrieves the {@code ChatServerRecord} corresponding to the provided {@code chatServerPid}
     * from the internal registry map, updates its client count to the value specified by {@code newClientCount},
     * and then returns the updated record.
     * </p>
     *
     * @param newClientCount the new client count to set in the ChatServerRecord.
     * @param chatServerPid  the process ID (PID) of the ChatServer whose record is to be updated.
     * @return the updated {@link io.github.cpsc559.team16.common.dto.ChatServerRecord}.
     * @throws NullPointerException if no ChatServerRecord exists for the given {@code chatServerPid}.
     */
    public ChatServerRecord updateClientCount(int newClientCount, Long chatServerPid) {
        ChatServerRecord record = chatServerRecords.get(chatServerPid);
        if (record == null) {
            System.err.println("Error: ChatServerRecord not found for PID: " + chatServerPid);
            throw new NullPointerException("No ChatServerRecord exists for ChatServer PID: " + chatServerPid);
        }
        record.setClientCount(newClientCount);
        return record;
    }

    /**
     * Returns the greatest process ID (PID) held by any of the active
     * {@code ChatServer}s by iterating through the current set
     * of {@link ChatServerRecord}s.
     * <p>
     *     This method is typically used during failover handling or recovery
     *     to ensure that no newly registered {@code ChatServer} is assigned a
     *     PID that is already in use, preserving PID uniqueness across the system.
     * </p>
     *
     * @return a Long representing the highest PID currently assigned to a {@code ChatServer}
     */
    public Long getMaxPID() {
        Long currentMax = 0L;
        for (ChatServerRecord record : this.chatServerRecords.values()) {
            if (record.getPID() > currentMax) {
                currentMax = record.getPID();
            }
        }
        return currentMax;
    }


    /**
     * Creates a record for a chat server by generating a unique ID and inserting its
     * ChatServerRecord record into the global addressLog.
     * <p>
     * This method generates a unique ID using {@code generatePID()}, creates a new
     * {@link ChatServerRecord} object with the given parameters, and then inserts it into the addressLog. If a record
     * with the same ID already exists, it logs a warning message (and overwrites it).
     * </p>
     *
     * @param serverPID       the unique process ID generated by the Primary {@code AddressingServer} for this chat server.
     * @param chatHostAddress the host address (IP or hostname) of the chat server.
     * @param chatClientPort  the port used for client connections.
     * @param chatPeerPort    the port used for peer-to-peer (gossip) communication.
     * @param maxClientCount  the maximum number of client connections allowed for this server.
     */
    public void createChatServerRecord(Long serverPID, String chatHostAddress, int chatClientPort,
                                       int chatPeerPort, int maxClientCount) {
        try {
            ChatServerRecord newServer = new ChatServerRecord(serverPID, chatHostAddress,  chatClientPort, chatPeerPort,
                    maxClientCount);

            // Check if the key already existed (should be null for a new key)
            ChatServerRecord previous = this.chatServerRecords.put(serverPID, newServer);

            if (previous != null) {
                System.err.println("WARNING: A server with ID " + serverPID + " already existed. Overwriting existing entry.");
            } else {
                System.out.println("\n**ChatServer** successfully registered with ID: " + serverPID + "\n");
                debugPrintAllServers();
            }
        } catch (Exception e) {
            System.err.println("Error registering chat server: " + e.getMessage());
            throw e;
        }
    }


    /**
     * Triggers a detailed diagnostic print of all currently registered Chat Servers.
     * <p>
     * This method retrieves the live {@link ChatServerRecord} collection to display
     * real-time server metrics, including active client counts and current operational
     * status (e.g., ACTIVE, PIVOTING). Useful for monitoring load distribution
     * across the cluster.
     * </p>
     */
    public void debugPrintAllServers() {
        ServerDebugLogger.printAllChatServers(this.getRecords().values());
    }

}
