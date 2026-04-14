package io.github.cpsc559.team16.common.utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import io.github.cpsc559.team16.common.dto.PrimaryAddress;

/**
 * <h1>PrimaryDiscoveryReader</h1>
 * <p>
 * A utility class used by addressing server REPLICAs, Chat Servers, and Client processes to
 * discover the current PRIMARY Addressing Server's identity via the Docker networks shared filesystem.
 * </p>
 * <p>
 * This reader complements the {@code PrimaryDiscoveryManager} by parsing the
 * published connection string and providing a structured way to access the
 * Primary's host address within the network and it's port assignments.
 * </p>
 */
public class PrimaryDiscoveryReader {

    private static final String DISCOVERY_PATH = "/shared/primary_address.txt";


    /**
     * Reads the discovery file from the shared volume and parses its content.
     *
     * @return A {@link PrimaryAddress} object containing the Primary's details,
     * or {@code null} if the file does not exist or is empty.
     * @throws IOException If there is an error reading from the filesystem.
     * @throws IllegalArgumentException If the file content is malformed
     * (i.e. it does not match the expected format).
     */
    public static PrimaryAddress readPrimaryDetails() throws IOException {
        Path path = Paths.get(DISCOVERY_PATH);

        if (!Files.exists(path)) {
            return null; // Primary hasn't published itself yet
        }

        List<String> lines = Files.readAllLines(path);
        if (lines.isEmpty()) {
            return null;
        }

        String details = lines.get(0).trim();
        if (details.isEmpty()) {
            return null;
        }

        // Expected string format: host:replicaPort:chatServerPort:clientPort
        String[] substrings = details.split(":");
        if (substrings.length != 4) {
            throw new IllegalArgumentException("Malformed discovery file content: " + details);
        }

        try {
            return new PrimaryAddress(
                    substrings[0],
                    Integer.parseInt(substrings[1]),
                    Integer.parseInt(substrings[2]),
                    Integer.parseInt(substrings[3])
            );
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port number in the discovery file", e);
        }
    }
}