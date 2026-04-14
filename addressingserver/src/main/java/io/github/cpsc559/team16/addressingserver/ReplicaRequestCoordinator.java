package io.github.cpsc559.team16.addressingserver;

import io.github.cpsc559.team16.common.messaging.MessageIDGenerator;
import io.github.cpsc559.team16.common.utilities.NIOMessageChannel;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The {@code ReplicaRequestCoordinator} is a background thread responsible for periodically sending
 * synchronization requests from a {@code REPLICA} AddressingServer to the {@code PRIMARY}.
 * <p>
 * It operates independently from the main event loop, and handles retrying until a valid connection
 * to the primary is established. Once connected, it uses a {@link ReplicaRequestManager} to issue
 * structured requests for system state updates (e.g. {@link io.github.cpsc559.team16.common.dto.ChatServerRecord}s,
 * {@link io.github.cpsc559.team16.common.dto.AddrServerRecord}s).
 * </p>
 * <p>
 * The thread is monitored in the event loop via {@code isAlive()}, and may be restarted
 * if it fails. It is also cleanly shut down during server termination.
 */
public class ReplicaRequestCoordinator extends Thread {

    /** Manager used to initiate sync requests to the primary AddressingServer. */
    private ReplicaRequestManager requestManager = null;

    /** A callback used to register the ReplicaRequestManager externally (e.g. in the AddressingServer). */
    private final Consumer<ReplicaRequestManager> onReady;

    /** This is set externally once registration is complete. */
    private volatile NIOMessageChannel primaryChannel;

    /** Generator used to produce unique message IDs for sync requests. */
    private final MessageIDGenerator messageIDGenerator;

    /** Flag to indicate if this thread should continue running. */
    private volatile boolean running = true;

    /** A functional supplier that retrieves the current channel to the PRIMARY addressing server. */
    private final Supplier<NIOMessageChannel> primaryChannelSupplier;


    /**
     * Constructs a new {@code ReplicaRequestCoordinator} thread responsible for managing
     * periodic synchronization requests from a {@code REPLICA} to the {@code PRIMARY} AddressingServer.
     *
     * <p>This thread will periodically issue requests for {@link io.github.cpsc559.team16.common.dto.ChatServerRecord}s,
     * and every Nth cycle, it will also request {@link io.github.cpsc559.team16.common.dto.AddrServerRecord}s,
     * allowing for more frequent chat server syncing while avoiding redundant address server requests.</p>
     *
     * <p>The {@code ReplicaRequestManager} that handles the actual messaging logic is created during this thread's
     * execution. The provided {@code onReady} callback is invoked when the manager has been successfully instantiated,
     * allowing the outer system to reference it.</p>
     *
     * @param messageIDGenerator       the generator used to create globally unique message IDs for each request
     * @param onReady                  a {@code Consumer} callback used to expose the instantiated {@link ReplicaRequestManager}
     * @param primaryChannelSupplier   a {@code Supplier} that provides the active {@link NIOMessageChannel} connected to the PRIMARY
     */
    public ReplicaRequestCoordinator(MessageIDGenerator messageIDGenerator,
                                     Consumer<ReplicaRequestManager> onReady,
                                     Supplier<NIOMessageChannel> primaryChannelSupplier) {
        super("ReplicaRequestCoordinator");
        this.messageIDGenerator = messageIDGenerator;
        this.primaryChannelSupplier = primaryChannelSupplier;
        this.onReady = onReady;
        this.setDaemon(true);
    }


    /**
     * Signals the coordinator to stop running. Should be followed by {@code join()} for a clean shutdown.
     */
    public void shutdown() {
        this.running = false;
        this.interrupt(); // Wake up a sleeping thread so it can shutdown immediately.
    }


    /**
     * The main loop for the {@code ReplicaRequestCoordinator}.
     * <p>
     * This method initializes a {@code ReplicaRequestManager}, publishes it via the consumer callback,
     * and enters a loop where sync requests are periodically sent to the primary. It attempts full sync
     * (AddrServerRecord) requests every 6 cycles, and ChatServer syncs on each loop.
     * </p>
     */
    @Override
    public void run() {
        this.requestManager = new ReplicaRequestManager(messageIDGenerator, primaryChannelSupplier);
        this.onReady.accept(requestManager);

        while (running) {
            try {
                if (requestManager.incrementSyncCounter()) {
                    requestManager.requestAllAddrServerPids();
                    requestManager.requestAllServerRecords();
                } else {
                    requestManager.requestAllChatServerPids();
                    requestManager.requestAllChatServerRecords();
                }
                Thread.sleep(20000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
