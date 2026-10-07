package com.github.h3yow.reliableeventdeliveryjava.event;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Thread-safe local store for events and their delivery progress.
 *
 * <p>Every state transition is atomic (guarded by per-event monitor + version
 * counter) and logged with the event id, producing an explainable transition
 * trail. The store never deletes records, so progress survives any consumer
 * interruption: an event left IN_PROGRESS by a crashed consumer can simply be
 * retried via {@link #markFailed} or re-claimed after being failed back.
 */
@Component
public class EventStore {

    private static final Logger log = LoggerFactory.getLogger(EventStore.class);

    private final Map<String, EventRecord> byId = new ConcurrentHashMap<>();
    private final Map<String, String> idByKey = new ConcurrentHashMap<>();
    private final AtomicLong versions = new AtomicLong();

    /**
     * Idempotent accept: same key + same payload returns the existing record
     * (no extra delivery); same key + different payload is rejected with
     * {@link EventConflictException}.
     */
    public EventRecord saveIfAbsent(String idempotencyKey, String payload, int maxAttempts) {
        String existingId = idByKey.get(idempotencyKey);
        if (existingId != null) {
            EventRecord existing = byId.get(existingId);
            if (existing.getPayload().equals(payload)) {
                log.info("event={} key={} duplicate submit ignored, status={}",
                        existing.getId(), idempotencyKey, existing.getStatus());
                return existing;
            }
            throw new EventConflictException(idempotencyKey,
                    "idempotency key already used with a different payload");
        }
        String id = UUID.randomUUID().toString();
        Instant now = Instant.now();
        EventRecord created = new EventRecord(id, idempotencyKey, payload, EventStatus.PENDING,
                0, maxAttempts, null, null, versions.incrementAndGet(), now, now, null);
        // Atomic: only the first writer of this key wins; losers re-check conflict.
        String winner = idByKey.putIfAbsent(idempotencyKey, id);
        if (winner != null) {
            return saveIfAbsent(idempotencyKey, payload, maxAttempts);
        }
        byId.put(id, created);
        log.info("event={} key={} transition null->PENDING attempts=0/{}", id, idempotencyKey, maxAttempts);
        return created;
    }

    /** Atomically move PENDING -> IN_PROGRESS; empty if already claimed/terminal. */
    public Optional<EventRecord> claim(String eventId) {
        return transition(eventId, EventStatus.PENDING, EventStatus.IN_PROGRESS, null, null, false, null);
    }

    /** IN_PROGRESS -> SUCCEEDED. */
    public EventRecord markSucceeded(String eventId) {
        return transition(eventId, EventStatus.IN_PROGRESS, EventStatus.SUCCEEDED, null, null, false, null)
                .orElseThrow(() -> new IllegalStateException("event " + eventId + " not IN_PROGRESS"));
    }

    /**
     * IN_PROGRESS -> PENDING (retryable, attempts left) or
     * IN_PROGRESS -> DEAD (retry exhausted / non-retryable), with a
     * distinguishable {@link DeadReason}. The event is due again immediately.
     */
    public EventRecord markFailed(String eventId, boolean retryable, String error) {
        return markFailed(eventId, retryable, error, null);
    }

    /**
     * As {@link #markFailed(String, boolean, String)} but schedules the next
     * attempt no earlier than {@code nextAttemptAt} (ignored when the event
     * goes DEAD). While waiting, the event stays PENDING but is not eligible
     * for delivery, so it never blocks other events.
     */
    public EventRecord markFailed(String eventId, boolean retryable, String error, Instant nextAttemptAt) {
        EventRecord current = require(eventId);
        boolean exhausted = current.getAttempts() + 1 >= current.getMaxAttempts();
        EventStatus target = (retryable && !exhausted) ? EventStatus.PENDING : EventStatus.DEAD;
        DeadReason reason = target == EventStatus.DEAD
                ? (retryable ? DeadReason.RETRY_EXHAUSTED : DeadReason.NON_RETRYABLE)
                : null;
        Instant scheduled = target == EventStatus.PENDING ? nextAttemptAt : null;
        return transition(eventId, EventStatus.IN_PROGRESS, target, reason, error, true, scheduled)
                .orElseThrow(() -> new IllegalStateException("event " + eventId + " not IN_PROGRESS"));
    }

    /** Controlled replay: DEAD -> PENDING with attempts and error reset. */
    public EventRecord replay(String eventId) {
        EventRecord current = require(eventId);
        EventRecord reset = new EventRecord(current.getId(), current.getIdempotencyKey(),
                current.getPayload(), EventStatus.PENDING, 0, current.getMaxAttempts(),
                null, null, versions.incrementAndGet(), current.getCreatedAt(), Instant.now(), null);
        synchronized (current.getId().intern()) {
            EventRecord latest = require(eventId);
            if (latest.getStatus() != EventStatus.DEAD) {
                throw new IllegalStateException(
                        "event " + eventId + " is " + latest.getStatus() + ", only DEAD can be replayed");
            }
            byId.put(eventId, reset);
        }
        log.info("event={} transition DEAD->PENDING replay attempts=0/{}",
                eventId, reset.getMaxAttempts());
        return reset;
    }

    /**
     * Recover events stuck IN_PROGRESS since before {@code cutoff} (consumer
     * crashed or never finished) back to PENDING, due immediately. Attempts
     * are preserved so progress is not lost; the applied-set in the processor
     * still suppresses any side effect that had already run.
     *
     * @return the recovered records, in recovery order
     */
    public List<EventRecord> recoverStuck(Instant cutoff) {
        List<EventRecord> recovered = new java.util.ArrayList<>();
        for (EventRecord record : findAll()) {
            if (record.getStatus() == EventStatus.IN_PROGRESS
                    && !record.getUpdatedAt().isAfter(cutoff)) {
                transition(record.getId(), EventStatus.IN_PROGRESS, EventStatus.PENDING,
                        null, "recovered from interrupted consumer", false, null)
                        .ifPresent(r -> {
                            log.warn("event={} recovered IN_PROGRESS->PENDING after being stuck since {}",
                                    r.getId(), record.getUpdatedAt());
                            recovered.add(r);
                        });
            }
        }
        return recovered;
    }

    /** PENDING events due for delivery at {@code now}, oldest first, capped. */
    public List<EventRecord> findDuePending(Instant now, int limit) {
        return byId.values().stream()
                .filter(r -> r.getStatus() == EventStatus.PENDING)
                .filter(r -> r.getNextAttemptAt() == null || !r.getNextAttemptAt().isAfter(now))
                .sorted(java.util.Comparator.comparing(EventRecord::getCreatedAt))
                .limit(limit)
                .toList();
    }

    /** Number of events still occupying delivery capacity (PENDING + IN_PROGRESS). */
    public long countActive() {
        return byId.values().stream()
                .filter(r -> r.getStatus() == EventStatus.PENDING
                        || r.getStatus() == EventStatus.IN_PROGRESS)
                .count();
    }

    public Optional<EventRecord> findByKey(String idempotencyKey) {
        String id = idByKey.get(idempotencyKey);
        return id == null ? Optional.empty() : Optional.ofNullable(byId.get(id));
    }

    public Optional<EventRecord> find(String eventId) {
        return Optional.ofNullable(byId.get(eventId));
    }

    public List<EventRecord> findAll() {
        return List.copyOf(byId.values());
    }

    private EventRecord require(String eventId) {
        EventRecord record = byId.get(eventId);
        if (record == null) {
            throw new IllegalArgumentException("unknown event id: " + eventId);
        }
        return record;
    }

    private Optional<EventRecord> transition(String eventId, EventStatus from, EventStatus to,
                                             DeadReason deadReason, String error,
                                             boolean incrementAttempts, Instant nextAttemptAt) {
        synchronized (eventId.intern()) {
            EventRecord current = byId.get(eventId);
            if (current == null || current.getStatus() != from) {
                return Optional.empty();
            }
            EventRecord next = new EventRecord(current.getId(), current.getIdempotencyKey(),
                    current.getPayload(), to,
                    incrementAttempts ? current.getAttempts() + 1 : current.getAttempts(),
                    current.getMaxAttempts(), deadReason, error,
                    versions.incrementAndGet(), current.getCreatedAt(), Instant.now(), nextAttemptAt);
            byId.put(eventId, next);
            log.info("event={} transition {}->{} attempts={}/{}{}{}{}",
                    eventId, from, to, next.getAttempts(), next.getMaxAttempts(),
                    deadReason != null ? " deadReason=" + deadReason : "",
                    error != null ? " error=" + error : "",
                    nextAttemptAt != null ? " nextAttemptAt=" + nextAttemptAt : "");
            return Optional.of(next);
        }
    }
}
