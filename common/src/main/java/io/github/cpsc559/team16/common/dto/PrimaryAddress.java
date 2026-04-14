package io.github.cpsc559.team16.common.dto;

/**
 * Data Transfer Object (DTO) to hold the parsed address information.
 */
public record PrimaryAddress(String hostAddress, int replicaPort, int chatServerPort, int clientPort) {}
