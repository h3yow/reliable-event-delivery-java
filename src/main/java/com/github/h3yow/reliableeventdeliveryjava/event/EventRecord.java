package com.github.h3yow.reliableeventdeliveryjava.event;

import java.time.Instant;

/**
 * Immutable snapshot of one stored event and its delivery progress.
 */
public final class EventRecord {

    private final String id;
    private final String idempotencyKey;
    private final String payload;
    private final EventStatus status;
    private final int attempts;
    private final int maxAttempts;
    private final DeadReason deadReason;
    private final String lastError;
    private final long version;
    private final Instant createdAt;
    private final Instant updatedAt;

    public EventRecord(String id, String idempotencyKey, String payload, EventStatus status,
                       int attempts, int maxAttempts, DeadReason deadReason, String lastError,
                       long version, Instant createdAt, Instant updatedAt) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.payload = payload;
        this.status = status;
        this.attempts = attempts;
        this.maxAttempts = maxAttempts;
        this.deadReason = deadReason;
        this.lastError = lastError;
        this.version = version;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public String getId() { return id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getPayload() { return payload; }
    public EventStatus getStatus() { return status; }
    public int getAttempts() { return attempts; }
    public int getMaxAttempts() { return maxAttempts; }
    public DeadReason getDeadReason() { return deadReason; }
    public String getLastError() { return lastError; }
    public long getVersion() { return version; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
