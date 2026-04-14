package io.github.cpsc559.team16.common.logging;

import io.github.cpsc559.team16.common.dto.AddrServerRecord;
import io.github.cpsc559.team16.common.dto.ChatServerRecord;

import java.io.PrintStream;
import java.util.Collection;


public class ServerDebugLogger {

    private static final PrintStream out = System.out;

    // --- ADDRESSING SERVER LOGGING ---

    public static synchronized void printAddrServer(AddrServerRecord s) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n---------- AddrServerRecord Record -----------\n");
        sb.append(String.format("Network PID     : %s%n", s.getPID()));
        sb.append(String.format("Host Address    : %s%n", s.getHostAddress()));
        sb.append(String.format("Client Port     : %d%n", s.getClientPort()));
        sb.append(String.format("Peer Port       : %d%n", s.getPeerPort()));
        sb.append(String.format("ChatServer Port : %d%n", s.getChatServerPort()));
        sb.append(String.format("Role            : %s%n", s.getRole()));
        sb.append("---------------------------------------------------\n");
        out.print(sb.toString());
        out.flush();
    }

    public static synchronized void printAddrServerAction(String action, AddrServerRecord s) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("\n\t------ %s AddrServer Record -------%n", action.toUpperCase()));
        sb.append(String.format("\tProcess ID   : %s%n", s.getPID()));
        sb.append(String.format("\tHost Address : %s%n", s.getHostAddress()));
        sb.append("\t------------------------------------------\n");
        out.print(sb.toString());
        out.flush();
    }

    /**
     * Prints all registered Addressing Servers in detail.
     */
    public static synchronized void printAllAddrServers(Collection<AddrServerRecord> addrServers) {
        out.println("\n|------------- Currently Registered AddressingServers -------------|");
        if (addrServers.isEmpty()) {
            out.println("\t(None)");
        } else {
            for (AddrServerRecord s : addrServers) {
                printAddrServer(s);
            }
        }
        out.println("|--------------------------------------------------------------------|");
        out.flush();
    }


    // --- CHAT SERVER LOGGING ---

    public static synchronized void printChatServer(ChatServerRecord s) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\t---------- ChatServerRecord Record ----------\n");
        sb.append(String.format("\tProcess ID   : %s%n", s.getPID()));
        sb.append(String.format("\tHost Address : %s%n", s.getHostAddress()));
        sb.append(String.format("\tClient Port  : %d%n", s.getClientPort()));
        sb.append(String.format("\tPeer Port    : %d%n", s.getPeerPort()));
        sb.append(String.format("\tClient Count : %d%n", s.getClientCount()));
        sb.append(String.format("\tStatus       : %s%n", s.getStatus()));
        sb.append("\t-------------------------------------------\n");
        out.print(sb.toString());
        out.flush();
    }

    public static synchronized void printChatServerAction(String action, ChatServerRecord s) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("\n\t------ %s ChatServer Record -------%n", action.toUpperCase()));
        sb.append(String.format("\tProcess ID   : %s%n", s.getPID()));
        sb.append(String.format("\tHost Address : %s%n", s.getHostAddress()));
        sb.append("\t------------------------------------------\n");
        out.print(sb.toString());
        out.flush();
    }

    /**
     * Prints all registered Chat Servers in detail.
     */
    public static synchronized void printAllChatServers(Collection<ChatServerRecord> chatServers) {
        out.println("\n|--------------- Currently Registered ChatServers ---------------|");
        if (chatServers.isEmpty()) {
            out.println("\t(None)");
        } else {
            for (ChatServerRecord s : chatServers) {
                printChatServer(s);
            }
        }
        out.println("|-----------------------------------------------------------------|");
        out.flush();
    }

    // --- NETWORK TOPOLOGY LOGGING ---

    /**
     * Prints the entire network state - Addressing Servers followed by Chat Servers.
     */
    public static synchronized void printEntireNetwork(Collection<AddrServerRecord> addrServers,
                                                       Collection<ChatServerRecord> chatServers) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n|==================== FULL NETWORK TOPOLOGY ====================|\n");

        sb.append("\n  [ ADDRESSING SERVERS ]\n");
        if (addrServers.isEmpty()) {
            sb.append("\t(None)\n");
        } else {
            for (AddrServerRecord s : addrServers) {
                sb.append(String.format("\tPID: %-5s | Address: %-15s | Role: %-10s |",
                        s.getPID(), s.getHostAddress(), s.getRole()));
            }
        }

        sb.append("\n  [ CHAT SERVERS ]\n");
        if (chatServers.isEmpty()) {
            sb.append("\t(None)\n");
        } else {
            for (ChatServerRecord s : chatServers) {
                sb.append(String.format("\tPID: %-5s | Address: %-15s | C.Count: %-3d | Status: %s%n",
                        s.getPID(), s.getHostAddress(), s.getClientCount(), s.getStatus()));
            }
        }

        sb.append("\n|===============================================================|\n");
        out.print(sb.toString());
        out.flush();
    }
}
