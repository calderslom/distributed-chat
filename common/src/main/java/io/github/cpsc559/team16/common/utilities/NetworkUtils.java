package io.github.cpsc559.team16.common.utilities;

import io.github.cpsc559.team16.common.messaging.Roles;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class NetworkUtils {

    /**
     * Retrieves the unique network identity for this node.
     * In Docker, this returns the Container ID (e.g. 14a7237fc906).
     *
     * @param processRole Use constants from the Roles class (e.g. {@code Roles.#CHATSERVER}).
     * @return A routable hostname string.
     */
    public static String getSerializedIdentity(String processRole) {
        try {
            // Retrieve Docker Container address
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            System.err.println("[" + processRole + "] Identity Lookup Failed: " + e.getMessage());

            // Fallback to environment variables (if available)
            String envHost = System.getenv("HOSTNAME");
            if (envHost != null && !envHost.isEmpty()) {
                return envHost;
            }
            // If we get here, the network stack is likely uninitialized.
            throw new RuntimeException("This " + processRole + " could not establish a network identity.");
        }
    }
}