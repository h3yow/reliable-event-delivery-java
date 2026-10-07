package com.github.h3yow.reliableeventdeliveryjava.event;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Automatically and continuously delivers pending events, so no manual or
 * external trigger is needed after submission.
 *
 * <p>Each tick (every {@code events.dispatcher.poll-interval-ms}):
 * <ol>
 *   <li>recovers events stuck IN_PROGRESS longer than
 *       {@code events.recovery.stuck-timeout-ms} back to PENDING;</li>
 *   <li>picks up to {@code events.dispatcher.batch-size} PENDING events whose
 *       {@code nextAttemptAt} is due (oldest first) and hands them to a worker
 *       pool — a failed event waiting out its backoff is simply not due and
 *       never blocks the others.</li>
 * </ol>
 * The whole dispatcher can be switched off with
 * {@code events.dispatcher.enabled=false} (tests / demos).
 */
@Component
@ConditionalOnProperty(prefix = "events.dispatcher", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(EventDispatcher.class);

    private final EventStore store;
    private final EventProcessor processor;
    private final EventDeliveryProperties properties;
    private final EventHandler handler;
    private final ExecutorService workers;
    /** Events currently being delivered by a worker; avoids re-submitting. */
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();

    @org.springframework.beans.factory.annotation.Autowired
    public EventDispatcher(EventStore store, EventProcessor processor,
                           EventDeliveryProperties properties, EventHandler handler) {
        this(store, processor, properties, handler,
                Executors.newFixedThreadPool(properties.getDispatcher().getThreads(), r -> {
                    Thread t = new Thread(r, "event-dispatcher");
                    t.setDaemon(true);
                    return t;
                }));
    }

    /** Test-friendly constructor with a caller-provided executor. */
    public EventDispatcher(EventStore store, EventProcessor processor,
                           EventDeliveryProperties properties, EventHandler handler,
                           ExecutorService workers) {
        this.store = store;
        this.processor = processor;
        this.properties = properties;
        this.handler = handler;
        this.workers = workers;
    }

    @Scheduled(fixedDelayString = "${events.dispatcher.poll-interval-ms:200}")
    public void scheduledTick() {
        tick();
    }

    /** One recovery + dispatch round. Package-visible for deterministic tests. */
    public void tick() {
        List<EventRecord> recovered = store.recoverStuck(
                Instant.now().minusMillis(properties.getRecovery().getStuckTimeoutMs()));
        if (!recovered.isEmpty()) {
            log.info("recovered {} stuck event(s) back to PENDING", recovered.size());
        }

        List<EventRecord> due = store.findDuePending(Instant.now(), properties.getDispatcher().getBatchSize());
        for (EventRecord event : due) {
            if (!inFlight.add(event.getId())) {
                continue;
            }
            workers.submit(() -> {
                try {
                    processor.process(event.getId(), handler);
                } catch (Exception e) {
                    log.warn("event={} dispatch error: {}", event.getId(), e.getMessage());
                } finally {
                    inFlight.remove(event.getId());
                }
            });
        }
    }

    @PreDestroy
    void shutdown() {
        workers.shutdown();
        try {
            workers.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
