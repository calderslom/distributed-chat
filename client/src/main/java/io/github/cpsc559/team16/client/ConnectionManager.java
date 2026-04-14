package io.github.cpsc559.team16.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.cpsc559.team16.common.logging.DebugLogger;
import io.github.cpsc559.team16.common.messaging.AckMessage;
import io.github.cpsc559.team16.common.messaging.AckObjectTypes;
import io.github.cpsc559.team16.common.messaging.RegisterMessage;
import io.github.cpsc559.team16.common.utilities.ClientServerMessage;

import static io.github.cpsc559.team16.common.logging.DebugLogger.*;

/**
 * A class responsible for managing the connection between the client and the chat server.
 * <p>
 * This class handles the establishment of connections to the chat server through the addressing server,
 * reconnection attempts in case of network failures, and the registration process with the addressing server.
 * It ensures that the client can communicate with the chat server reliably and provides mechanisms for
 * recovering from disconnections.
 * </p>
 * 
 * @see Client
 * @see RegisterMessage
 * @see AckMessage
 * @see Socket
 */
public class ConnectionManager {

    private final Client client;

    // Connection management
    /**
     * Tracks the number of reconnection attempts made by the client.F
     * It is reset after a successful reconnection.
     */
    private int reconnectTries = 0;
    public int getReconnectTries() {
        return reconnectTries;
    }

    private boolean inChatSession = false;
    /**
     * The maximum number of reconnection attempts allowed.
     * If this limit is reached, the client will stop attempting to reconnect.
     */
    public static final int MAX_RECONNECT_TRIES = 5;



    public ConnectionManager(Client client) {
        this.client = client;
    }


    /**
     * Establishes a connection to the chat server through the addressing server.
     * 
     * This method performs the following steps:
     * <ol>
     * <li>Connects to the addressing server using the provided address and
     * port.</li>
     * <li>Retrieves chat server details (PID, address, and port) from the
     * addressing server.</li>
     * <li>Establishes a direct connection to the retrieved chat server using the
     * provided address and port.</li>
     * <li>Sends a registration message to the chat server to register the
     * client.</li>
     * </ol>
     * 
     * The method will repeatedly attempt to register with the addressing server if
     * the initial attempt fails.
     * It retries once per second until a successful response is received. Upon
     * success, the client will be registered
     * with the chat server.
     * 
     * If the connection to the chat server fails at any point, an
     * {@link IOException} will be thrown.
     * 
     * @throws IOException if there is a failure while connecting to the addressing
     *                     server,
     *                     the chat server, or during the message exchange.
     */
    public void connect() throws IOException {
        if (client.isTerminated()) return;
        try {
            debug(DEBUG_NORMAL, "Attempting to connect to the PRIMARY addressing server...");

            // Establish server connection
            String[] substrings = registerWithAddressingServer();
            int retryCount = 0;
            while (substrings == null&& !client.isTerminated()) {
                retryCount++;
                debug(DEBUG_NORMAL, "PRIMARY connection failed... (Attempt " + retryCount + ")");
                if (DebugLogger.getDebugLevel() < DEBUG_BASIC) {
                    System.out.print("\r[STATUS] Searching for an available chat room... (Attempt " + retryCount + ")");
                }
                Thread.sleep(5000); // pause 5s before retrying
                substrings = registerWithAddressingServer();
            }
            System.out.println();
            if (client.isTerminated()) return;
            Long serverPid = Long.parseLong(substrings[0]);
            String chatServerAddress = substrings[1];
            int chatServerPort = Integer.parseInt(substrings[2]);

            // Step 2: Connect to chat server
            debug(DEBUG_NORMAL, "Attempting connection to chat server at " + chatServerAddress + ":" + chatServerPort);
            client.setChatServer(new Socket(chatServerAddress, chatServerPort));
            client.setOut(new PrintWriter(client.getChatServer().getOutputStream(), true)); // autoFlush = true
            client.setIn(new BufferedReader(new InputStreamReader(client.getChatServer().getInputStream())));
            client.setConnected(true);

            // Send the registration message
            ClientServerMessage registration = new ClientServerMessage(client.getUsername(), "server", -1, "");
            registration.setCommand("REGISTER");
            client.getOut().println(registration.toJson());

            // Display message to client while connecting
            if (DebugLogger.getDebugLevel() < DEBUG_BASIC && !inChatSession) {
                try {
                    System.out.print("Connecting to chatroom...");
                    while (!client.isConnected()) {
                        Thread.sleep(500);
                    }
                    System.out.println(" DONE\n");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                inChatSession = true;
            }
            else {
                debug(DEBUG_BASIC, "Successfully connected to chat server.");
            }
            Thread.sleep(400);
        } catch (IOException e) {
            debug(DEBUG_BASIC, "Connection error: " + e.getMessage());
            throw e;
        } catch (Exception e) {
            System.err.println("Error parsing chat server address: " + e.getMessage());
        }
    }

    /**
     * Attempts to reconnect to the server network after a connection failure.
     * This method ensures the client can recover from network interruptions by
     * trying to
     * reconnect multiple times with an exponential backoff strategy.
     * 
     * <p>
     * The reconnect process involves retrying the connection up to a maximum number
     * of attempts
     * (defined by {@code MAX_RECONNECT_TRIES}). The backoff time between each
     * reconnection attempt
     * doubles, starting with a 1-second wait time. If all reconnection attempts
     * fail, the method will
     * attempt to retrieve a new chat server address from the Addressing Server and
     * reconnect using
     * that information. If the reconnection attempts ultimately fail, an error
     * message is displayed
     * and the client shuts down gracefully.
     * </p>
     *
     * <p>
     * Key operations:
     * </p>
     * <ul>
     * <li>Establishes a new connection with the chat server using exponential
     * backoff after each failure.</li>
     * <li>If the reconnection attempts are exhausted, it tries connecting via the
     * Addressing Server for a new server.</li>
     * <li>Handles retries with an interval that doubles after each failed attempt
     * to reduce server load.</li>
     * <li>In case of failure after all attempts, the method provides feedback to
     * the user and shuts down the client.</li>
     * </ul>
     *
     * @throws IOException If the connection attempts to either the chat server or
     *                     the Addressing Server fail
     *                     after the maximum retry attempts have been reached. This
     *                     ensures the client gracefully
     *                     handles persistent connection issues.
     */
    public void reconnect() {
        debug(DEBUG_BASIC, "Starting reconnection process");
        client.setConnected(false);

        client.getMessageUtils().postSystemMessage("[SYSTEM] Disconnected - please wait while the system attempts to reconnect...");

        while (reconnectTries < MAX_RECONNECT_TRIES) {
            try {
                debug(DEBUG_NORMAL, "Reconnection attempt " + (reconnectTries + 1) + " of " + MAX_RECONNECT_TRIES);
                connect();
                reconnectTries = 0; // Reset counter on successful connection
                debug(DEBUG_BASIC, "Reconnection successful");
                client.getMessageUtils().postSystemMessage("[SYSTEM] Connection re-established.");
                return;
            } catch (Exception e) {
                reconnectTries++;
                debug(DEBUG_NORMAL, "Reconnection attempt failed: " + e.getMessage());
                try {
                    Thread.sleep(5000); // Wait before retrying
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        debug(DEBUG_BASIC, "Max reconnection attempts reached");

        try {
            connect(); // Will re-connect to sys through Addressing Server
            debug(DEBUG_BASIC, "Connected to new chat server after Addressing Server redirect.");
        } catch (IOException e) {
            debug(DEBUG_BASIC, "Unable to connect to any chat server. Client shutdown.");
            // Add user-friendly error message to display log
            client.getMessageUtils().postSystemMessage("[SYSTEM] No chat servers are currently available. Please try again later.");
            // Give the OutputThread time to display the message
            try {
                Thread.sleep(4000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            client.shutdown();
        }
    }

    /**
     * Registers the client with the addressing server by sending a registration
     * message and
     * awaiting a response. This method establishes a connection with the addressing
     * server,
     * sends the client's registration details, and processes the acknowledgment
     * received
     * from the server to retrieve the chat server's information.
     * 
     * The method performs the following steps:
     * <ol>
     * <li>Opens a socket connection to the addressing server</li>
     * <li>Sends a registration message in JSON format</li>
     * <li>Waits for the server's acknowledgment</li>
     * <li>Deserializes the acknowledgment response to extract the chat server's
     * information (PID, host, and port)</li>
     * <li>Returns the extracted information as an array of strings in the format:
     * {pid, host, port}</li>
     * </ol>
     * 
     * @return A string array containing the chat server's PID, host address, and
     *         port
     * @throws IOException If there is a failure in communication with the
     *                     addressing server,
     *                     including connection issues, timeouts, or invalid
     *                     responses.
     */

    public String[] registerWithAddressingServer() throws IOException {

        String hostname = client.getPrimaryHostAddress();
        int port = client.getAsPort();
        debug(DEBUG_NORMAL, "Attempting to resolve and connect to: " + hostname + ":" + port);

        // Open a socket to the addressing server
        try (Socket addressSocket = new Socket(hostname, port)) {
            addressSocket.setSoTimeout(5000); // Timeout after 5 seconds

            debug(DEBUG_NORMAL, "trying to connect to addressing server");

            // Create the streams
            PrintWriter out = new PrintWriter(addressSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(addressSocket.getInputStream()));

            // Create a REGISTER message for the client. Currently it doesn't send any
            // additional information,
            // but I'm guessing this is where we would do the token stuff??? - Aidan
            RegisterMessage<String> regMsg = RegisterMessage.fromClient();
            // Send the registration JSON message
            out.println(regMsg.toJson());

            // Wait for the addressing server's response ()
            String ackJson = in.readLine();
            if (ackJson == null) {
                throw new IOException("No response from Addressing Server.");
            }

            debug(DEBUG_NORMAL, "RECEIVED FROM SERVER: " + ackJson);

            // Deserialize the JSON into an AckMessage
            ObjectMapper mapper = new ObjectMapper();
            AckMessage ackMessage = mapper.readValue(ackJson, AckMessage.class);

            // Extract the payload. It should be a String in the format
            // "pid:hostAddress:clientPort"

            if (ackMessage.getObjectType().equals(AckObjectTypes.HOSTADDRESS)) {
                String payload = (String) ackMessage.getPayload();
                if (payload == null || !payload.contains(":")) {
                    throw new IOException("Invalid payload from Addressing Server: " + payload);
                }
                // Split the payload into 3 parts: pid, host, and port
                String[] substrings = payload.split(":");
                if (substrings.length != 3) {
                    throw new IOException("Expected 3 parts in the payload, but got " + substrings.length);
                }
                // System.out.println("done");

                return substrings;

            } else {
                debug(DEBUG_NORMAL, "Registration ACK from PRIMARY indicated there are no chat servers available (NoHost).");
                return null;
            }
        }
    }
    
}
