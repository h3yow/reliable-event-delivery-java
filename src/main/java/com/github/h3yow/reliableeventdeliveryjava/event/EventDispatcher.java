package com.github.h3yow.reliableeventdeliveryjava.event;

import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Drives delivery without any external trigger: each sweep first recovers
 * events stuck IN_PROGRESS beyond the stale timeout, then delivers every
 * PENDING event whose retry backoff has elapsed.
 *
 * <p>The loop is invoked periodically by {@code EventDispatcherScheduling}
 * when {@code events.dispatcher.enabled=true}; {@link #runOnce()} is public
 * so tests and operators can trigger a sweep directly. One event backing off
 * (nextAttemptAt in the future) is simply skipped, never blocking the others.
 */
@Component
public class EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(EventDispatcher.class);

    private final EventStore store;
    private final EventProcessor processor;
    private final EventHandler handler;
    private final long staleTimeoutMs;

    public EventDispatcher(EventStore store, EventProcessor processor, EventHandler handler,
                           @Value("${events.recovery.stale-timeout-ms:30000}") long staleTimeoutMs) {
        this.store = store;
        this.processor = processor;
        this.handler = handler;
        this.staleTimeoutMs = staleTimeoutMs;
    }

    /**
     * One dispatch sweep: recover stale IN_PROGRESS events, then process all
     * due PENDING events oldest-first.
     *
     * @return number of delivery attempts made in this sweep
     */
    public int runOnce() {
        Instant now = Instant.now();
        List<EventRecord> reclaimed = store.reclaimStale(now.minusMillis(staleTimeoutMs));
        if (!reclaimed.isEmpty()) {
            log.info("recovery sweep reclaimed {} stale event(s): {}", reclaimed.size(),
                    reclaimed.stream().map(EventRecord::getId).toList());
        }

        List<EventRecord> due = store.findDuePending(now);
        if (due.isEmpty()) {
            return 0;
        }
        log.info("dispatch sweep: {} due event(s) {}", due.size(),
                due.stream().map(EventRecord::getId).toList());
        int attempts = 0;
        for (EventRecord event : due) {
            processor.process(event.getId(), handler);
            attempts++;
        }
        return attempts;
    }
}
