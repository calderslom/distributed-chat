package io.github.cpsc559.team16.common.utilities;

/**
 * Utility class for controlling verbosity of server logs.
 * Moved from ChatServer to provide centralized logging across the system.
 */
public class DebugLogger {

    /**
     * The current debug level for controlling verbosity of server logs.
     * <p>
     * This is configurable at runtime using the environment variable
     * <b>DEBUG_LEVEL</b>.
     * If the environment variable is not set, the default level is
     * {@code DEBUG_EXTREME} (5),
     * meaning all debug messages will be printed.
     * </p>
     * <p>
     * Example usage in shell to reduce output to basic info only:
     *
     * <pre>{@code
     * export DEBUG_LEVEL=1
     * }</pre>
     * </p>
     */
    public static final int DEBUG_LEVEL = Integer.parseInt(System.getenv().getOrDefault("DEBUG_LEVEL", "5"));

    // Debug level constants

    /**
     * Debug level: No debug output. Use in production mode where logs are minimal.
     */
    public static final int DEBUG_NONE = 0; // No debug output (production mode)

    /**
     * Debug level: Basic events such as startup, shutdown, and major transitions.
     */
    public static final int DEBUG_BASIC = 1; // Basic info: startup, shutdown, major events

    /**
     * Debug level: Normal runtime activity such as new connections or message
     * processing.
     */
    public static final int DEBUG_NORMAL = 2; // Normal operation details: connections, requests

    /**
     * Debug level: Step-by-step logic, including function entry points and internal
     * decisions.
     */
    public static final int DEBUG_DETAILED = 3; // Detailed flow: entering methods, decision points

    /**
     * Debug level: Low-level I/O activity like byte reads/writes and selector
     * state.
     */
    public static final int DEBUG_LOW_LEVEL = 4; // Low-level operations: byte-level I/O, parsing

    /**
     * Debug level: Maximum verbosity including every possible detail.
     * Useful for diagnosing edge cases or unexpected behavior.
     */
    public static final int DEBUG_EXTREME = 5; // Extreme detail: everything, for deep debugging

    /**
     * Logs a debug message to standard output if the message level is
     * less than or equal to the configured {@link #DEBUG_LEVEL}.
     * <p>
     * Each message is prefixed with a tag representing its severity.
     * This helps developers visually filter relevant messages while debugging.
     * </p>
     *
     * @param level   the severity level of the message (0–5)
     * @param message the message to log
     */
    public static void debug(int level, String message) {
        if (level <= DEBUG_LEVEL) {
            String prefix = switch (level) {
                case 1 -> "[BASIC] ";
                case 2 -> "[NORMAL] ";
                case 3 -> "[DETAILED] ";
                case 4 -> "[LOW_LEVEL] ";
                case 5 -> "[EXTREME] ";
                default -> "[INFO] ";
            };
            System.out.println(prefix + message);
        }
    }
}