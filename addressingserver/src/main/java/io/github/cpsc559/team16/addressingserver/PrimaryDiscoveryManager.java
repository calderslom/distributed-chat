package io.github.cpsc559.team16.addressingserver;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * <h1>PrimaryDiscoveryManager</h1>
 * <p>
 * Facilitates service discovery in a distributed cluster by publishing the network
 * identity of the current PRIMARY Addressing Server to a shared filesystem.
 * </p>
 * <p><b>Design Pattern: Atomic Write-and-Rename</b></p>
 * <p>
 * To prevent "dirty reads" where a ChatServer or Replica might read a partially
 * written file, this manager writes to a temporary file first and then performs
 * an atomic move (rename) to the target destination. This ensures that the
 * discovery file always contains a valid, complete connection string.
 * </p>
 */
public class PrimaryDiscoveryManager {

    /** The stable path where all network entities access the PRIMARY addressing servers contact information. */
    private static final String DISCOVERY_PATH = "/shared/primary_address.txt";

    /** Temporary path used for staging writes to ensure atomicity. */
    private static final String TEMP_PATH = "/shared/primary_address.tmp";

    private final AddrServerConfig config;

    /**
     * Constructs a discovery manager with the provided addressing server configuration.
     * @param config The object containing the configuration information of the addressing server instantiating the class.
     */
    public PrimaryDiscoveryManager(AddrServerConfig config) {
        this.config = config;
    }

    /**
     * <p>Atomically publishes the PRIMARY server's details to the shared discovery file.</p>
     * <p>The published format is: {@code host:replicaPort:chatServerPort:clientPort}</p>
     * <p><b>Concurrency Note:</b> Uses {@code StandardCopyOption.ATOMIC_MOVE}. On POSIX
     * systems (like Docker/Linux), this guarantees that other processes will see
     * either the old file or the new file, but never a corrupted or empty file
     * during the write process. Failsafes have been put in place to account for when
     * processes access an old (stale) file, such as server failure notices, and retry logic for
     * failed connections.</p>
     *
     * @throws IOException If the file cannot be written or the move operation fails.
     */
    public void publish() throws IOException {
        // Format: host:replica:chat:client
        String content = String.format("%s:%d:%d:%d",
                config.getHostAddress(),
                config.getReplicaPort(),
                config.getChatServerPort(),
                config.getClientPort());
        System.out.printf("[DISCOVERY] PID %d is publishing identity: %s%n", config.getPID(), content);
        Path finalPath = Paths.get(DISCOVERY_PATH);
        Path tempPath = Paths.get(TEMP_PATH);

        // 1. Write to a temporary file first (non-blocking for readers)
        Files.write(tempPath, content.getBytes(StandardCharsets.UTF_8));

        // 2. Atomically move the temp file to the final destination.
        // This effectively "swaps" the file pointers instantly at the OS level.
        Files.move(tempPath, finalPath,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
    }
}