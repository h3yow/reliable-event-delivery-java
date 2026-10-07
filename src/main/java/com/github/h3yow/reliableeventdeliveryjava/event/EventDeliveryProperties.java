package com.github.h3yow.reliableeventdeliveryjava.event;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Tuning knobs for automatic delivery, recovery and backpressure.
 * All values have safe defaults so the app runs with zero configuration.
 */
@Component
@ConfigurationProperties(prefix = "events")
public class EventDeliveryProperties {

    /** Delivery attempts per event before it goes DEAD. */
    private int maxAttempts = 3;

    private final Dispatcher dispatcher = new Dispatcher();
    private final Retry retry = new Retry();
    private final Recovery recovery = new Recovery();
    private final Capacity capacity = new Capacity();

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public Dispatcher getDispatcher() { return dispatcher; }
    public Retry getRetry() { return retry; }
    public Recovery getRecovery() { return recovery; }
    public Capacity getCapacity() { return capacity; }

    /** Automatic delivery loop. */
    public static class Dispatcher {
        /** Master switch (set false for tests/demos). */
        private boolean enabled = true;
        /** Delay between dispatcher ticks. */
        private long pollIntervalMs = 200;
        /** Max events dispatched per tick. */
        private int batchSize = 50;
        /** Worker threads executing handlers. */
        private int threads = 4;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public long getPollIntervalMs() { return pollIntervalMs; }
        public void setPollIntervalMs(long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }
        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
        public int getThreads() { return threads; }
        public void setThreads(int threads) { this.threads = threads; }
    }

    /** Bounded, delayed retry cadence for failed events. */
    public static class Retry {
        /** Delay before the first retry of a failed event. */
        private long initialBackoffMs = 500;
        /** Multiplier applied to the backoff after each failed attempt. */
        private double backoffMultiplier = 2.0;
        /** Upper bound of the retry backoff. */
        private long maxBackoffMs = 10_000;

        public long getInitialBackoffMs() { return initialBackoffMs; }
        public void setInitialBackoffMs(long initialBackoffMs) { this.initialBackoffMs = initialBackoffMs; }
        public double getBackoffMultiplier() { return backoffMultiplier; }
        public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
        public long getMaxBackoffMs() { return maxBackoffMs; }
        public void setMaxBackoffMs(long maxBackoffMs) { this.maxBackoffMs = maxBackoffMs; }

        /** Backoff before attempt number {@code nextAttempt} (1-based after a failure). */
        public long backoffForAttempt(int nextAttempt) {
            double delay = initialBackoffMs * Math.pow(backoffMultiplier, Math.max(0, nextAttempt - 1));
            return (long) Math.min(maxBackoffMs, delay);
        }
    }

    /** Recovery of interrupted deliveries. */
    public static class Recovery {
        /** An IN_PROGRESS event older than this is recovered back to PENDING. */
        private long stuckTimeoutMs = 30_000;

        public long getStuckTimeoutMs() { return stuckTimeoutMs; }
        public void setStuckTimeoutMs(long stuckTimeoutMs) { this.stuckTimeoutMs = stuckTimeoutMs; }
    }

    /** Backpressure. */
    public static class Capacity {
        /** Max PENDING + IN_PROGRESS events before new submissions are rejected. */
        private long maxPending = 10_000;

        public long getMaxPending() { return maxPending; }
        public void setMaxPending(long maxPending) { this.maxPending = maxPending; }
    }
}
