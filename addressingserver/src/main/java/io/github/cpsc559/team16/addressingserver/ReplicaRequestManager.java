package io.github.cpsc559.team16.addressingserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.sun.net.httpserver.Request;
import io.github.cpsc559.team16.common.messaging.BaseAddrServerMessage;
import io.github.cpsc559.team16.common.messaging.MessageIDGenerator;
import io.github.cpsc559.team16.common.messaging.RequestMessage;
import io.github.cpsc559.team16.common.utilities.NIOMessageChannel;

import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.util.function.Supplier;

/**
 * The {@code ReplicaRequestManager} is responsible for initiating outbound requests
 * from a {@code REPLICA} process to the {@code PRIMARY} {@code AddressingServer}.
 * <p>
 * This class acts as a communication interface for the replica, enabling it to:
 * <ul>
 *   <li>Request full synchronization of server state or records</li>
 *   <li>Request specific updates (e.g., delta syncs, targeted state queries)</li>
 *   <li>Report conditions or errors to the primary (optional, future functionality)</li>
 * </ul>
 * <p>
 * The {@code ReplicaRequestManager} may be extended in the future to support additional
 * synchronization types, health checks, or telemetry reporting.
 * </p>
 */
public class ReplicaRequestManager {

    private static final int FULL_SYNC_THRESHOLD = 6;


    /**
     * Supplier for the outbound channel for communicating with the primary AddressingServer.
     * Assumes that this connection is already established and persistent.
     */
    Supplier<NIOMessageChannel> primaryChannelSupplier;

    /**
     * Tracks how many sync requests this replica has made to the primary.
     * <p>
     * Used in combination with {@link #incrementSyncCounter()} to control the frequency
     * of different types of synchronization operations. For example, a full
     * {@code AddrServerRecord} sync might be triggered every 6th request, while
     * {@code ChatServerRecord} syncs happen more frequently.
     * </p>
     */
    private int syncRequestCounter = 0;

    /**
     * The {@code MessageIDGenerator} instance used by this server to produce globally unique message IDs.
     * <p>
     * This generator ensures that every message requiring acknowledgment or ordering has a distinct identifier,
     * which is critical for maintaining consistency guarantees (e.g., in replication or event tracking).
     * </p>
     * <p>
     * The generator is initialized once per process and typically updated with the server's assigned PID
     * after registration, ensuring message IDs are globally unique across all AddressingServer processes.
     * </p>
     */
    private final MessageIDGenerator genMID;


    /**
     * Constructs a {@code ReplicaRequestManager}.
     */
    public ReplicaRequestManager(MessageIDGenerator messageIDGenerator, Supplier<NIOMessageChannel> nioChannelSupplier) {
        this.genMID = messageIDGenerator;
        this.primaryChannelSupplier = nioChannelSupplier;
    }

    /**
     * Executes a reliable transmission of a {@link RequestMessage} to the Primary AddressingServer.
     * <p>
     * This method implements a short-term retry loop with a 1-second backoff to handle transient
     * network instability or DNS propagation delays during leader elections. It distinguishes
     * between fatal errors (serialization) and recoverable errors (connection drops).
     * </p>
     * <p>
     * Because this method utilizes a {@code primaryChannelSupplier}, it is self-healing; if a
     * new Primary is elected during the retry window, the next iteration will automatically
     * resolve the new {@link NIOMessageChannel} and attempt transmission there.
     * </p>
     *
     * @param message The {@code RequestMessage} (typically containing a {@code Void} payload)
     * to be sent to the Primary.
     */
    private void send(RequestMessage<Void> message) {
        int attempts = 0;
        int maxAttempts = 3;

        while (attempts < maxAttempts) {
            NIOMessageChannel primaryChannel = this.primaryChannelSupplier.get();

            if (primaryChannel != null && primaryChannel.getSocketChannel().isOpen()) {
                try {
                    // Stage 1: Serialization (FATAL if fails)
                    String jsonPayload = message.toJson();

                    // Stage 2: Transmission (RETRYABLE if fails)
                    primaryChannel.sendMessage(jsonPayload);
                    return;
                } catch (JsonProcessingException e) {
                    // No point in retrying - return to caller
                    System.err.println("Failed to serialize RequestMessage<Void>: " + e.getMessage());
                    return;
                } catch (IOException e) {
                    attempts++;
                    System.err.printf("[RQST MGR] Send failed (Attempt %d/%d): %s%n",
                            attempts, maxAttempts, e.getMessage());
                }
            } else {
                attempts++;
                System.err.printf("[RQST MGR] Request skipped: No active connection to Primary (Attempt %d/%d).%n",
                        attempts, maxAttempts);
            }

            // Short backoff to let the Primary/DNS settle
            if (attempts < maxAttempts) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        System.err.println("[RQST MGR] Sync request aborted after max retries.");
    }

    /**
     * Initiates a full synchronization request for all registered server records
     * (both AddressingServers and ChatServers).
     * <p>
     * This is typically called every Nth cycle by the {@code ReplicaRequestCoordinator}
     * to ensure absolute state consistency across the cluster.
     * </p>
     */
    public void requestAllServerRecords() {
        RequestMessage<Void> message = RequestMessage.requestAllServerRecords(genMID.nextID(), genMID.getPID());
        send(message);
    }

    /**
     * Initiates a synchronization request specifically for {@code ChatServerRecord}s.
     * <p>
     * This is the high-frequency sync path used to keep the Replica updated on
     * active chat instances without the overhead of a full system state transfer.
     * </p>
     */
    public void requestAllChatServerRecords() {
        RequestMessage<Void> message = RequestMessage.requestAllChatServerRecords(genMID.nextID(), genMID.getPID());
        send(message);
    }

    /**
     * Initiates a synchronization request specifically for {@code AddrServerRecord}s.
     * <p>
     * Used primarily to update the Replica's internal view of the AddressingServer
     * cluster topology (e.g. discovering new Replicas or PIDs).
     * </p>
     */
    public void requestAllAddrServerRecords() {
        RequestMessage<Void> message = RequestMessage.requestAllAddrServerRecords(genMID.nextID(), genMID.getPID());
        send(message);
    }

    /**
     * Broadcasts a request to the Primary Addressing Server to retrieve a complete
     * collection of PIDs for all currently registered Addressing Servers (Peers).
     * <p>
     * This is typically used to synchronize local state and identify stale
     * Addressing Server records that are no longer active in the network.
     * </p>
     */
    public void requestAllAddrServerPids() {
        RequestMessage<Void> message = RequestMessage.requestAllPeerPids(genMID.nextID(), genMID.getPID());
        send(message);
    }

    /**
     * Broadcasts a request to the Primary Addressing Server to retrieve a complete
     * collection of PIDs for all currently registered Chat Servers.
     * <p>
     * This allows the local server to verify its peer registry against the Primary's
     * "source of truth" and purge any records for Chat Servers that have
     * disconnected without a proper shutdown.
     * </p>
     */
    public void requestAllChatServerPids() {
        RequestMessage<Void> message = RequestMessage.requestAllChatServerPids(genMID.nextID(), genMID.getPID());
        send(message);
    }


    /**
     * Increments the sync request counter and determines whether to trigger a full sync.
     * <p>
     * This method returns {@code true} every {@link #FULL_SYNC_THRESHOLD} times it is called,
     * using modulo arithmetic to automatically wrap the counter. This helps balance the frequency
     * of full {@code AddrServerRecord} syncs relative to more frequent {@code ChatServerRecord} syncs.
     * </p>
     *
     * @return {@code true} if a full AddressingServer sync should be performed, {@code false} otherwise.
     */
    public boolean incrementSyncCounter() {
        this.syncRequestCounter = ++this.syncRequestCounter % FULL_SYNC_THRESHOLD;
        return this.syncRequestCounter == 0;
    }

}
