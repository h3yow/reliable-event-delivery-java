package com.github.h3yow.reliableeventdeliveryjava.event;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Delivers events to a handler with bounded retries and exactly-once side
 * effect semantics under concurrent / duplicate consumption.
 *
 * <p>Two guards cooperate:
 * <ul>
 *   <li>{@link EventStore#claim} atomically moves PENDING -&gt; IN_PROGRESS, so
 *       only one concurrent caller executes the handler per attempt.</li>
 *   <li>a local applied-set records event ids whose side effect already ran,
 *       so a duplicate delivery of an already-succeeded event is a no-op.</li>
 * </ul>
 * Retryable failures return the event to PENDING with a per-event
 * {@code nextAttemptAt} backoff, so one event waiting to retry never blocks
 * the delivery of other events. If the JVM is interrupted mid-attempt the
 * event stays IN_PROGRESS in the store until {@link EventStore#reclaimStale}
 * returns it to PENDING without losing the record.
 */
@Component
public class EventProcessor {

    private static final Logger log = LoggerFactory.getLogger(EventProcessor.class);

    private final EventStore store;
    private final long retryDelayMs;
    private final Set<String> applied = ConcurrentHashMap.newKeySet();

    /** Convenience constructor: failures are due again immediately. */
    public EventProcessor(EventStore store) {
        this(store, 0);
    }

    @Autowired
    public EventProcessor(EventStore store,
                          @Value("${events.retry-delay-ms:2000}") long retryDelayMs) {
        this.store = store;
        this.retryDelayMs = retryDelayMs;
    }

    /**
     * Attempt one delivery of the given event. Safe to call concurrently for
     * the same event id: only one caller applies the side effect.
     *
     * @return the resulting record after this attempt
     */
    public EventRecord process(String eventId, EventHandler handler) {
        EventRecord current = store.find(eventId)
                .orElseThrow(() -> new IllegalArgumentException("unknown event id: " + eventId));

        if (current.getStatus() == EventStatus.SUCCEEDED || applied.contains(eventId)) {
            log.info("event={} duplicate delivery suppressed, side effect already applied", eventId);
            return current;
        }
        if (current.getStatus() == EventStatus.DEAD) {
            log.info("event={} is DEAD ({}), requires replay before delivery",
                    eventId, current.getDeadReason());
            return current;
        }

        var claimed = store.claim(eventId);
        if (claimed.isEmpty()) {
            // Another consumer holds this attempt; no duplicate side effect.
            log.info("event={} claim lost, another consumer is processing", eventId);
            return store.find(eventId).orElseThrow();
        }

        try {
            handler.handle(claimed.get());
            applied.add(eventId);
            return store.markSucceeded(eventId);
        } catch (NonRetryableException e) {
            log.warn("event={} non-retryable failure: {}", eventId, e.getMessage());
            return store.markFailed(eventId, false, e.getMessage());
        } catch (Exception e) {
            log.warn("event={} retryable failure, next attempt in {}ms: {}",
                    eventId, retryDelayMs, e.getMessage());
            return store.markFailed(eventId, true, e.getMessage(), retryDelayMs);
        }
    }
}
