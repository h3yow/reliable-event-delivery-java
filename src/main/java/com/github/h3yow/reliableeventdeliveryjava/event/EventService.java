package com.github.h3yow.reliableeventdeliveryjava.event;

import org.springframework.stereotype.Service;

/**
 * Entry point for business actions: accepts events idempotently and exposes
 * query / replay operations.
 */
@Service
public class EventService {

    private final EventStore store;
    private final EventDeliveryProperties properties;

    @org.springframework.beans.factory.annotation.Autowired
    public EventService(EventStore store, EventDeliveryProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    /** Convenience for tests: default tuning with an explicit attempt budget. */
    public EventService(EventStore store, int maxAttempts) {
        this(store, defaultProperties(maxAttempts));
    }

    private static EventDeliveryProperties defaultProperties(int maxAttempts) {
        EventDeliveryProperties p = new EventDeliveryProperties();
        p.setMaxAttempts(maxAttempts);
        return p;
    }

    /**
     * Accept a business action exactly once per idempotency key. The event is
     * stored before this method returns, so it cannot be lost once the action
     * is accepted.
     *
     * <p>Backpressure: when the backlog (PENDING + IN_PROGRESS) reaches
     * {@code events.capacity.max-pending}, <em>new</em> keys are rejected with
     * {@link EventCapacityException} (HTTP 429) — never silently dropped and
     * distinct from a business conflict. Re-submissions of an already known
     * key stay idempotent and are still accepted while full.
     *
     * @throws EventConflictException same key resubmitted with a different payload
     * @throws EventCapacityException backlog full; retry after it drains
     */
    public EventRecord submit(String idempotencyKey, String payload) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        if (store.findByKey(idempotencyKey).isEmpty()
                && store.countActive() >= properties.getCapacity().getMaxPending()) {
            throw new EventCapacityException(properties.getCapacity().getMaxPending());
        }
        return store.saveIfAbsent(idempotencyKey, payload, properties.getMaxAttempts());
    }

    public EventRecord get(String eventId) {
        return store.find(eventId)
                .orElseThrow(() -> new IllegalArgumentException("unknown event id: " + eventId));
    }

    /** Controlled replay of a DEAD event back to PENDING. */
    public EventRecord replay(String eventId) {
        return store.replay(eventId);
    }

    /**
     * Operations-facing summary: per-status counts, capacity state, the age
     * of the oldest waiting event, and how retries are distributed — enough
     * to tell where a backlog is stuck.
     */
    public EventStatsView stats() {
        var all = store.findAll();
        java.util.EnumMap<EventStatus, Long> counts = new java.util.EnumMap<>(EventStatus.class);
        for (EventStatus s : EventStatus.values()) {
            counts.put(s, 0L);
        }
        java.util.TreeMap<Integer, Long> retries = new java.util.TreeMap<>();
        java.time.Instant oldestPending = null;
        long active = 0;
        for (EventRecord r : all) {
            counts.merge(r.getStatus(), 1L, Long::sum);
            if (r.getStatus() == EventStatus.PENDING || r.getStatus() == EventStatus.IN_PROGRESS) {
                active++;
            }
            if (r.getStatus() == EventStatus.PENDING
                    && (oldestPending == null || r.getCreatedAt().isBefore(oldestPending))) {
                oldestPending = r.getCreatedAt();
            }
            if (r.getAttempts() > 0) {
                retries.merge(r.getAttempts(), 1L, Long::sum);
            }
        }
        Long oldestAge = oldestPending == null ? null
                : java.time.Duration.between(oldestPending, java.time.Instant.now()).toMillis();
        return new EventStatsView(java.util.Collections.unmodifiableMap(counts), active,
                properties.getCapacity().getMaxPending(),
                active < properties.getCapacity().getMaxPending(),
                oldestAge, java.util.Collections.unmodifiableMap(retries));
    }
}
