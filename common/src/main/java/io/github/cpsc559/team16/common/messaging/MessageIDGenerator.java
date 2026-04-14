package io.github.cpsc559.team16.common.messaging;

import java.util.function.Supplier;

public class MessageIDGenerator {

    /** A functional supplier that fetches the latest PID from the server configuration. */
    private final Supplier<Long> pidSupplier;
    private long lastTimestamp = -1;
    private int sequence = 0;

    public MessageIDGenerator(Supplier<Long> pidSupplier) {
        this.pidSupplier = pidSupplier;
    }

    public long getPID() {
        return pidSupplier.get();
    }


    public synchronized long nextID() {
        long currentPID = pidSupplier.get();
        long currentTime = System.currentTimeMillis() & 0xFFFFFFFFFFL; // 40 bits

        if (currentTime == lastTimestamp) {
            sequence = (sequence + 1) & 0xFF; // Wrap around at 255
            if (sequence == 0) {
                // Busy wait: wait for next millisecond
                while ((currentTime = System.currentTimeMillis() & 0xFFFFFFFFFFL) == lastTimestamp) {
                    Thread.yield();
                }
            }
        } else {
            sequence = 0;
            lastTimestamp = currentTime;
        }

        return (currentPID << 48) | (currentTime << 8) | sequence;
    }

    public static long extractPid(long id) {
        return (id >>> 48) & 0xFFFFL;
    }

    public static long extractTimestamp(long id) {
        return (id >>> 8) & 0xFFFFFFFFFFL;
    }

    public static int extractSequence(long id) {
        return (int)(id & 0xFF);
    }
}

