package com.github.h3yow.reliableeventdeliveryjava.event;

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

    public EventService(EventStore store,
                        @Value("${events.max-attempts:3}") int defaultMaxAttempts) {
        this.store = store;
        this.defaultMaxAttempts = defaultMaxAttempts;
    }

    /**
     * Accept a business action exactly once per idempotency key. The event is
     * stored before this method returns, so it cannot be lost once the action
     * is accepted.
     *
     * @throws EventConflictException same key resubmitted with a different payload
     */
    public EventRecord submit(String idempotencyKey, String payload) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        return store.saveIfAbsent(idempotencyKey, payload, defaultMaxAttempts);
    }

    public EventRecord get(String eventId) {
        return store.find(eventId)
                .orElseThrow(() -> new IllegalArgumentException("unknown event id: " + eventId));
    }

    /** Controlled replay of a DEAD event back to PENDING. */
    public EventRecord replay(String eventId) {
        return store.replay(eventId);
    }
}
