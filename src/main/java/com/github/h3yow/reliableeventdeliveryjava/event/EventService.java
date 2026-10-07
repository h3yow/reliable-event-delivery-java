package com.github.h3yow.reliableeventdeliveryjava.event;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Entry point for business actions: accepts events idempotently and exposes
 * query / replay operations.
 */
@Service
public class EventService {

    private final EventStore store;
    private final int defaultMaxAttempts;
    private final int maxPending;

    /** Convenience constructor with the default backlog capacity. */
    public EventService(EventStore store, int defaultMaxAttempts) {
        this(store, defaultMaxAttempts, 1000);
    }

    @Autowired
    public EventService(EventStore store,
                        @Value("${events.max-attempts:3}") int defaultMaxAttempts,
                        @Value("${events.capacity.max-pending:1000}") int maxPending) {        this.store = store;
        this.defaultMaxAttempts = defaultMaxAttempts;
        this.maxPending = maxPending;
    }

    /**
     * Accept a business action exactly once per idempotency key. The event is
     * stored before this method returns, so it cannot be lost once the action
     * is accepted.
     *
     * @throws EventConflictException same key resubmitted with a different payload
     * @throws EventOverloadException backlog at capacity; retry after it drains
     */
    public EventRecord submit(String idempotencyKey, String payload) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        return store.saveIfAbsent(idempotencyKey, payload, defaultMaxAttempts, maxPending);
    }

    public EventRecord get(String eventId) {
        return store.find(eventId)
                .orElseThrow(() -> new IllegalArgumentException("unknown event id: " + eventId));
    }

    /** Controlled replay of a DEAD event back to PENDING. */
    public EventRecord replay(String eventId) {
        return store.replay(eventId);
    }

    /** Operational summary snapshot for monitoring the backlog. */
    public EventStats stats() {
        return store.summary(maxPending);
    }
}
