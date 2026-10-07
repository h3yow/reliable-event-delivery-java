package com.github.h3yow.reliableeventdeliveryjava.event;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
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
 * interruption: an event left IN_PROGRESS by a crashed consumer is returned
 * to PENDING by {@link #reclaimStale} once it exceeds the stale timeout.
 *
 * <p>Backpressure: {@link #saveIfAbsent} rejects new submissions with
 * {@link EventOverloadException} while the backlog (PENDING + IN_PROGRESS)
 * is at the configured capacity; admission reopens automatically once the
 * backlog drains below the limit.
 */
@Component
public class EventStore {

    private static final Logger log = LoggerFactory.getLogger(EventStore.class);

    private final Map<String, EventRecord> byId = new ConcurrentHashMap<>();
    private final Map<String, String> idByKey = new ConcurrentHashMap<>();
    private final AtomicLong versions = new AtomicLong();
    /** Serialises the capacity check + admit so the backlog cap holds under concurrency. */
    private final Object admitLock = new Object();

    /** Backwards-compatible admit without a capacity limit. */
    public EventRecord saveIfAbsent(String idempotencyKey, String payload, int maxAttempts) {
        return saveIfAbsent(idempotencyKey, payload, maxAttempts, Integer.MAX_VALUE);
    }

    /**
     * Idempotent accept: same key + same payload returns the existing record
     * (no extra delivery); same key + different payload is rejected with
     * {@link EventConflictException}.
     *
     * <p>If the backlog (PENDING + IN_PROGRESS) has reached {@code maxPending},
     * new keys are rejected with {@link EventOverloadException}; re-submits of
     * an already-known key are still answered idempotently.
     */
    public EventRecord saveIfAbsent(String idempotencyKey, String payload, int maxAttempts,
                                    int maxPending) {
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
        synchronized (admitLock) {
            // Re-check inside the lock: another thread may have admitted this key.
            existingId = idByKey.get(idempotencyKey);
            if (existingId != null) {
                return saveIfAbsent(idempotencyKey, payload, maxAttempts, maxPending);
            }
            int backlog = countBacklog();
            if (backlog >= maxPending) {
                log.warn("key={} submit rejected, backlog={} reached capacity maxPending={}",
                        idempotencyKey, backlog, maxPending);
                throw new EventOverloadException(maxPending,
                        "pending backlog " + backlog + " reached capacity " + maxPending
                                + ", retry after the backlog drains");
            }
            String id = UUID.randomUUID().toString();
            Instant now = Instant.now();
            EventRecord created = new EventRecord(id, idempotencyKey, payload, EventStatus.PENDING,
                    0, maxAttempts, null, null, versions.incrementAndGet(), now, now, null);
            idByKey.put(idempotencyKey, id);
            byId.put(id, created);
            log.info("event={} key={} transition null->PENDING attempts=0/{} backlog={}/{}",
                    id, idempotencyKey, maxAttempts, backlog + 1, maxPending);
            return created;
        }
    }

    /** Atomically move PENDING -> IN_PROGRESS; empty if already claimed/terminal. */
    public Optional<EventRecord> claim(String eventId) {
        return transition(eventId, EventStatus.PENDING, EventStatus.IN_PROGRESS,
                null, null, false, null);
    }

    /** IN_PROGRESS -> SUCCEEDED. */
    public EventRecord markSucceeded(String eventId) {
        return transition(eventId, EventStatus.IN_PROGRESS, EventStatus.SUCCEEDED,
                null, null, false, null)
                .orElseThrow(() -> new IllegalStateException("event " + eventId + " not IN_PROGRESS"));
    }

    /** Failure with no retry delay: the event is immediately due again. */
    public EventRecord markFailed(String eventId, boolean retryable, String error) {
        return markFailed(eventId, retryable, error, 0);
    }

    /**
     * IN_PROGRESS -> PENDING (retryable, attempts left) or
     * IN_PROGRESS -> DEAD (retry exhausted / non-retryable), with a
     * distinguishable {@link DeadReason}. When returning to PENDING the event
     * becomes due again only after {@code retryDelayMs}, so one event backing
     * off never blocks the dispatch of other due events.
     */
    public EventRecord markFailed(String eventId, boolean retryable, String error,
                                  long retryDelayMs) {
        EventRecord current = require(eventId);
        boolean exhausted = current.getAttempts() + 1 >= current.getMaxAttempts();
        EventStatus target = (retryable && !exhausted) ? EventStatus.PENDING : EventStatus.DEAD;
        DeadReason reason = target == EventStatus.DEAD
                ? (retryable ? DeadReason.RETRY_EXHAUSTED : DeadReason.NON_RETRYABLE)
                : null;
        Instant nextAttemptAt = target == EventStatus.PENDING && retryDelayMs > 0
                ? Instant.now().plusMillis(retryDelayMs)
                : null;
        return transition(eventId, EventStatus.IN_PROGRESS, target, reason, error, true, nextAttemptAt)
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
     * Recovery sweep: events stuck IN_PROGRESS since before {@code cutoff}
     * (consumer crashed or never finished) are returned to PENDING, due
     * immediately. The attempt counter is NOT incremented: the interrupted
     * attempt never reported an outcome, and the bounded-retry budget counts
     * completed failures only. Returns the reclaimed records.
     */
    public List<EventRecord> reclaimStale(Instant cutoff) {
        List<EventRecord> reclaimed = new ArrayList<>();
        for (EventRecord record : findAll()) {
            if (record.getStatus() != EventStatus.IN_PROGRESS
                    || record.getUpdatedAt().isAfter(cutoff)) {
                continue;
            }
            transition(record.getId(), EventStatus.IN_PROGRESS, EventStatus.PENDING,
                    null, "recovered from stale IN_PROGRESS", false, null)
                    .ifPresent(next -> {
                        log.info("event={} recovery=stale-reclaim attempts={}/{} kept, due immediately",
                                record.getId(), next.getAttempts(), next.getMaxAttempts());
                        reclaimed.add(next);
                    });
        }
        return reclaimed;
    }

    /** PENDING events whose retry backoff has elapsed, oldest first. */
    public List<EventRecord> findDuePending(Instant now) {
        List<EventRecord> due = new ArrayList<>();
        for (EventRecord record : byId.values()) {
            if (record.getStatus() == EventStatus.PENDING
                    && (record.getNextAttemptAt() == null || !record.getNextAttemptAt().isAfter(now))) {
                due.add(record);
            }
        }
        due.sort(Comparator.comparing(EventRecord::getUpdatedAt));
        return due;
    }

    /** Current backlog size: events still waiting for or undergoing delivery. */
    public int countBacklog() {
        int count = 0;
        for (EventRecord record : byId.values()) {
            if (record.getStatus() == EventStatus.PENDING
                    || record.getStatus() == EventStatus.IN_PROGRESS) {
                count++;
            }
        }
        return count;
    }

    public Optional<EventRecord> find(String eventId) {
        return Optional.ofNullable(byId.get(eventId));
    }

    /**
     * Operational summary: per-status counts, backlog vs capacity, the wait
     * time of the oldest PENDING event, and the retry (attempts) distribution.
     */
    public EventStats summary(int maxPending) {
        Instant now = Instant.now();
        Map<EventStatus, Long> counts = new EnumMap<>(EventStatus.class);
        for (EventStatus status : EventStatus.values()) {
            counts.put(status, 0L);
        }
        Map<Integer, Long> retries = new TreeMap<>();
        Instant oldestPendingSince = null;
        long backlog = 0;
        for (EventRecord record : byId.values()) {
            counts.merge(record.getStatus(), 1L, Long::sum);
            if (record.getStatus() == EventStatus.PENDING
                    || record.getStatus() == EventStatus.IN_PROGRESS) {
                backlog++;
            }
            if (record.getStatus() == EventStatus.PENDING
                    && (oldestPendingSince == null
                        || record.getUpdatedAt().isBefore(oldestPendingSince))) {
                oldestPendingSince = record.getUpdatedAt();
            }
            if (record.getAttempts() > 0) {
                retries.merge(record.getAttempts(), 1L, Long::sum);
            }
        }
        Long oldestWaitMs = oldestPendingSince == null
                ? null
                : Duration.between(oldestPendingSince, now).toMillis();
        return new EventStats(Collections.unmodifiableMap(counts), backlog, maxPending,
                oldestWaitMs, Collections.unmodifiableMap(retries), now);
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
                    versions.incrementAndGet(), current.getCreatedAt(), Instant.now(),
                    nextAttemptAt);
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
