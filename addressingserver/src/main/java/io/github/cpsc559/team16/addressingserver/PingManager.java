package io.github.cpsc559.team16.addressingserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import io.github.cpsc559.team16.common.dto.AddrServerRecord;
import io.github.cpsc559.team16.common.dto.ServerRole;
import io.github.cpsc559.team16.common.messaging.Roles;

import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

/**
 * <h1>PingManager</h1>
 * <p>
 * Responsible for managing heartbeat messages (pings) between the primary and replica servers.
 * Replicas monitor these pings to detect whether the primary is still alive. A missed ping (i.e.,
 * if the time elapsed since a successful connection exceeds a per‐peer timeout threshold) indicates
 * that a failure may have occurred, triggering a leader election via {@link LeaderElectionManager}.
 * </p>
 *
 * <h2>Key Features:</h2>
 * <ul>
 * <li>Periodically pings all peers (excluding self) to verify connectivity.</li>
 * <li>Uses an exponential moving average (EMA) of the round‐trip time (RTT) per peer.</li>
 * <li>Computes a per-peer timeout threshold as: max(estimatedRTT, safeDefaultRTT) * marginFactor.</li>
 * <li>The safe default RTT prevents false positives when no measurements exist or during network spikes.</li>
 * <li>If a peer’s last successful ping is older than its timeout threshold, it is flagged (and may be removed).</li>
 * </ul>
 *
 * <h2>Threading Model:</h2>
 * <p>
 * This class runs as its own thread (via {@code Runnable}) and can be scheduled using a
 * {@link java.util.concurrent.ScheduledExecutorService} to perform periodic checks.
 * </p>
 */
public class PingManager implements Runnable {

    /** Reference to the AddressingServer instance managing this process. */
    private final AddressingServer server;

    /** Configuration object containing server settings and runtime state. */
    private final AddrServerConfig config;

    /** Leader election manager used to initiate elections upon failure detection. */
    private final LeaderElectionManager leaderElectionManager;

    /** Flag to indicate when the ping manager should terminate its loop. */
    private boolean terminate;

    // --- Parameters for RTT estimation and connection timeout ---
    /**
     * Safe default RTT (in milliseconds) used as the lower bound for computing timeout thresholds.
     * This helps prevent false positives even when a measured RTT is very low.
     */
    private final int safeDefaultRTT = 10000; // 10 seconds

    /**
     * Smoothing factor (alpha) for the exponential moving average. Must be in (0,1].
     * A higher value gives more weight to recent samples.
     */
    private final double alpha = 0.2;

    /**
     * Margin multiplier applied to the estimated RTT when computing a timeout threshold.
     * For instance, a marginFactor of 1.5 means that the timeout threshold is 50% higher than the base value.
     */
    private final double marginFactor = 1.5;

    /**
     * Constructs a new {@link PingManager} instance.
     *
     * @param server The AddressingServer instance managing this process.
     */
    public PingManager(AddressingServer server) {
        this.server = server;
        this.config = server.getConfig();
        this.leaderElectionManager = server.getLeaderElectionManager();
    }

    /**
     * Shuts down the ping manager gracefully.
     */
    public void shutdown() {
        terminate = true;
    }

    /**
     * Determines if this server is currently playing the primary role.
     *
     * @return {@code true} if the server's role is PRIMARY, {@code false} otherwise.
     */
    private boolean isPrimary() {
        return config.getRole() == ServerRole.PRIMARY;
    }

    /**
     * Runs the periodic ping process. For each round, the manager:
     * <ol>
     * <li>Opens new non-blocking connections to all peers (excluding self).</li>
     * <li>Records the time each connection attempt starts.</li>
     * <li>Waits for connection completion (using a Selector with safeDefaultRTT as the maximum wait).</li>
     * <li>When a connection is finished, computes a sample RTT and updates the EMA estimate for that peer.</li>
     * <li>Updates the last successful ping time for each peer.</li>
     * <li>Checks, for each peer, whether the elapsed time since the last ping exceeds its computed timeout threshold.
     * If so, the peer is flagged as having missed its ping (and may be removed).</li>
     * </ol>
     */
    @Override
    public void run() {
        debug(DEBUG_BASIC, "PingManager: Started!");

        // Map of SocketChannel to the associated peer PID for each ping attempt.
        HashMap<SocketChannel, Long> channelPIDs;
        // Map from peer PID to the last time a successful ping (pong) was recorded.
        HashMap<Long, Date> lastPingFromPID = new HashMap<>();
        // Map of SocketChannel to the timestamp (in ms) when the connection attempt started.
        HashMap<SocketChannel, Long> pingStartTimes = new HashMap<>();
        // Persistent map from peer PID to the estimated round-trip time (RTT) in milliseconds.
        HashMap<Long, Long> estimatedRTTPerPeer = new HashMap<>();

        terminate = false;

        // Main loop: run until shutdown is requested.
        while (!terminate) {
            // Wait for 5 seconds between each round of pings.
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                debug(DEBUG_BASIC, "PingManager sleep interrupted: " + e.getMessage());
            }

            // Only proceed with pinging if the server is in replica mode.
            if (!isPrimary()) {
                debug(DEBUG_LOW_LEVEL, "ROLE: REPLICA");
                try (Selector selector = Selector.open()) {
                    // Reinitialize per-round maps (for new connection attempts).
                    channelPIDs = new HashMap<>();
                    pingStartTimes = new HashMap<>();

                    // Collect all peers except self.
                    ArrayList<AddrServerRecord> peers = new ArrayList<>();
                    for (AddrServerRecord peer : server.getAddrServerRegistry().getRecords().values()) {
                        if (!peer.getPID().equals(config.getPID())) {
                            peers.add(peer);
                        }
                    }

                    // For each peer, open a non-blocking connection and record the start time.
                    for (AddrServerRecord peer : peers) {
                        SocketChannel channel = SocketChannel.open();
                        channel.configureBlocking(false);
                        channel.connect(new InetSocketAddress(peer.getHostAddress(), 5050));
                        // Register only for connection events.
                        channel.register(selector, SelectionKey.OP_CONNECT);
                        // Record the start time of this connection attempt.
                        pingStartTimes.put(channel, System.currentTimeMillis());
                        // Initialize last ping time for the peer if not already set.
                        lastPingFromPID.putIfAbsent(peer.getPID(), new Date());
                        // Remember which channel belongs to which peer.
                        channelPIDs.put(channel, peer.getPID());
                    }

                    // Wait for connection events, with a maximum wait time set to safeDefaultRTT.
                    selector.select(safeDefaultRTT);
                    Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();

                    while (keyIterator.hasNext()) {
                        SelectionKey key = keyIterator.next();
                        keyIterator.remove();

                        if (key.isConnectable()) {
                            SocketChannel ch = (SocketChannel) key.channel();
                            if (ch.isConnectionPending()) {
                                try {
                                    ch.finishConnect();
                                    long finishTime = System.currentTimeMillis();
                                    // Compute sample RTT as the difference between finish and start times.
                                    Long startTime = pingStartTimes.get(ch);
                                    if (startTime != null) {
                                        long sampleRTT = finishTime - startTime;
                                        Long peerPID = channelPIDs.get(ch);
                                        // Retrieve any previous RTT estimate.
                                        Long previousEstimated = estimatedRTTPerPeer.get(peerPID);
                                        long newEstimated;
                                        if (previousEstimated == null) {
                                            // For the first sample, take the raw measurement.
                                            newEstimated = sampleRTT;
                                        } else {
                                            // Update the EMA estimate.
                                            newEstimated = (long)(alpha * sampleRTT + (1 - alpha) * previousEstimated);
                                        }
                                        // Store the updated RTT estimate.
                                        estimatedRTTPerPeer.put(peerPID, newEstimated);
                                    }
                                    // Update the last successful ping (pong) time for this peer.
                                    lastPingFromPID.put(channelPIDs.get(ch), new Date());
                                } catch (IOException e) {
                                    debug(DEBUG_BASIC, "PingManager: Failed to finish connection for a peer: " + e.getMessage());
                                    key.cancel();
                                }
                            }
                        }
                    }

                    // For each peer, determine if the elapsed time since the last ping exceeds the threshold.
                    Date now = new Date();
                    for (AddrServerRecord peer : peers) {
                        Date lastPing = lastPingFromPID.get(peer.getPID());
                        if (lastPing != null) {
                            long diff = now.getTime() - lastPing.getTime();
                            // Get the current estimated RTT; if none exists, use the safe default.
                            long estimatedRTT = estimatedRTTPerPeer.getOrDefault(peer.getPID(), (long) safeDefaultRTT);
                            // Compute the timeout threshold using the safe default as a lower bound.
                            long timeoutThreshold = Math.max(estimatedRTT, safeDefaultRTT);
                            timeoutThreshold = (long) (timeoutThreshold * marginFactor);
                            debug(DEBUG_LOW_LEVEL, "PingManager: Pinging peer: " + peer.getPID());
                            if (diff > timeoutThreshold) {
                                // server.getPeerManager().removeFailedServer(peer.getPID());
                            }
                        }
                    }
                } catch (IOException e) {
                    debug(DEBUG_BASIC, "PingManager: Error while pinging peers: " + e.getMessage());
                }
            } else {
                debug(DEBUG_EXTREME, "ROLE: PRIMARY");
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }
}