package io.github.cpsc559.team16.chatserver;

import java.nio.channels.SelectionKey;
import java.util.Map;

import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

/**
 * Monitors the heartbeat of connected peer servers and clients, ensuring that
 * inactive peers
 * are detected and disconnected after a specified number of missed {@code PONG}
 * responses.
 * <p>
 * This class implements the {@link Runnable} interface and runs in a separate
 * thread to periodically check
 * the activity of connected peers. It performs the following tasks:
 * </p>
 * <ul>
 * <li>Checks the time elapsed since the last activity of each peer.</li>
 * <li>If a peer is inactive beyond the configured {@link #IDLE_TIMEOUT}, it
 * sends a {@code PING} to check the peer's status.</li>
 * <li>If the peer is already awaiting a {@code PONG} response and the number of
 * missed pongs exceeds the {@link #MAX_MISSED} threshold,
 * the peer is considered unresponsive and is disconnected.</li>
 * </ul>
 *
 * <h3>Parameters:</h3>
 * <ul>
 * <li>{@code peerMap}: A map of peer IDs to their corresponding
 * {@link ConnectionContext}, which holds connection details.</li>
 * </ul>
 *
 * <h3>Debugging:</h3>
 * <ul>
 * <li>Various debug levels are used for monitoring the heartbeat process,
 * including details about peer inactivity and ping/pong exchanges.</li>
 * </ul>
 *
 * <h3>Key Methods:</h3>
 * <ul>
 * <li>{@link #run()} - The main loop that checks for peer activity, sends
 * heartbeats, and handles disconnections.</li>
 * </ul>
 * 
 * @see ConnectionContext for details on connection activity tracking
 * @see ChatServer#sendPing(ConnectionContext, SelectionKey) for sending PING
 *      messages
 */

public class HeartbeatMonitor implements Runnable {
    private static final long IDLE_TIMEOUT = 15000; // 15 seconds
    private static final int MAX_MISSED = 3;
    private final long monitorStartTime = System.currentTimeMillis();
    private static final long STARTUP_GRACE_PERIOD = 20_000; // 20 seconds

    private final Map<Long, ConnectionContext> peerMap;

    public HeartbeatMonitor(Map<Long, ConnectionContext> peerMap) {
        this.peerMap = peerMap;
    }


    @Override
    public void run() {
        debug(DEBUG_BASIC, "Starting heartbeat thread...");
        while (true) {

            // Servers cannot begin aggressive heartbeat monitoring right out of the gate. They need a grace period.
            if (System.currentTimeMillis() - monitorStartTime < STARTUP_GRACE_PERIOD) {
                debug(DEBUG_BASIC, "In startup grace period, skipping health checks...");
                try { Thread.sleep(5000); } catch (InterruptedException e) {}
                continue;
            }

            if (this.peerMap.isEmpty()) {
                debug(DEBUG_BASIC, "No active peer connections exist.");
            } else {
                debug(DEBUG_DETAILED, "Checking " + this.peerMap.size() + " peers for heartbeat...");
            }


            long now = System.currentTimeMillis();

            for (Map.Entry<Long, ConnectionContext> entry : peerMap.entrySet()) {
                long peerID = entry.getKey();
                ConnectionContext ctx = entry.getValue();

                if (peerID == -1 || ctx.peerPID == -1) {
                    debug(3, "Skipping unidentified peer connection...");
                    continue;
                }

                SelectionKey key = ctx.socketChannel.keyFor(ChatServer.getSelector());

                if (key == null || !key.isValid()) {
                    debug(3, "Skipping peer " + peerID + " — no valid SelectionKey.");
                    continue;
                }

                long timeSinceLastActivity = now - ctx.lastActivityTime;
                debug(4, "Peer " + peerID + " inactive for " + timeSinceLastActivity + "ms");

                if (timeSinceLastActivity > IDLE_TIMEOUT) {
                    if (ctx.awaitingPong) {
                        ctx.missedPongs++;
                        debug(2, "Peer " + peerID + " missed pong #" + ctx.missedPongs);

                        if (ctx.missedPongs >= MAX_MISSED) {
                            try {
                                debug(1, "Tagging unresponsive peer " + peerID + " for closure.");

                                ctx.needsClosing = true; // Set the flag
                                ChatServer.getSelector().wakeup(); // Interrupt the main thread's selector.select()

                                // We remove it from the map here so heartbeats stop immediately
                                peerMap.remove(peerID);
                            } catch (Exception e) {
                                debug(1, "Error closing peer " + peerID + ": " + e.getMessage());
                                e.printStackTrace();
                            }
                            continue;
                        }
                    } else {
                        debug(2, "Sending PING to peer " + peerID);
                        ChatServer.sendPing(ctx, key);
                    }
                }
            }

            try {
                Thread.sleep(5_000); // check every 5 seconds
            } catch (InterruptedException e) {
                debug(1, "HeartbeatMonitor interrupted.");
                return;
            }
        }
    }
}
