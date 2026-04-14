package io.github.cpsc559.team16.common.dto;

/**
 * Enum representing different types of connections this server can manage.
 * <ul>
 * <li>{@code CLIENT} — incoming user/client connection</li>
 * <li>{@code SERVER} — connection to or from another peer chat server</li>
 * <li>{@code ADDRESSING_SERVER} — initial registration and update
 * coordination</li>
 * </ul>
 */
public enum ConnectionType {
    CLIENT, SERVER, ADDRESSING_SERVER
}