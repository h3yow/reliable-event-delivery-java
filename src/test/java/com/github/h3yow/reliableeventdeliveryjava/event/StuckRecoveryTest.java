package com.github.h3yow.reliableeventdeliveryjava.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Events whose consumer crashed (or never finished) mid-attempt are
 * automatically recovered to PENDING after a bounded wait, then delivered
 * without losing progress and without duplicating side effects.
 */
class StuckRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(StuckRecoveryTest.class);

    private static final ExecutorService DIRECT = new AbstractExecutorService() {
        @Override public void shutdown() { }
        @Override public List<Runnable> shutdownNow() { return List.of(); }
        @Override public boolean isShutdown() { return false; }
        @Override public boolean isTerminated() { return false; }
        @Override public boolean awaitTermination(long t, TimeUnit u) { return true; }
        @Override public void execute(Runnable command) { command.run(); }
    };

    private EventStore store;
    private EventService service;
    private EventProcessor processor;
    private EventDeliveryProperties properties;

    @BeforeEach
    void setUp() {
        store = new EventStore();
        properties = new EventDeliveryProperties();
        service = new EventService(store, properties);
        processor = new EventProcessor(store, properties);
    }

    @Test
    void stuckEventIsRecoveredByDispatcherAndDelivered() {
        EventRecord event = service.submit("stuck-1", "payload");
        // Simulate a consumer that claimed the event and then died silently.
        store.claim(event.getId());
        assertThat(service.get(event.getId()).getStatus()).isEqualTo(EventStatus.IN_PROGRESS);

        properties.getRecovery().setStuckTimeoutMs(0); // everything stuck is due now
        AtomicInteger sideEffects = new AtomicInteger();
        EventDispatcher dispatcher = new EventDispatcher(store, processor,
                properties, e -> sideEffects.incrementAndGet(), DIRECT);

        dispatcher.tick(); // recovery + redelivery happen in the same round

        EventRecord done = service.get(event.getId());
        log.info("event={} recovered and delivered: status={} attempts={} sideEffects={}",
                done.getId(), done.getStatus(), done.getAttempts(), sideEffects.get());
        assertThat(done.getStatus()).isEqualTo(EventStatus.SUCCEEDED);
        assertThat(sideEffects).hasValue(1);
    }

    @Test
    void recoveryKeepsProgressAndDoesNotDuplicateSideEffects() {
        EventRecord event = service.submit("stuck-2", "payload");
        AtomicInteger sideEffects = new AtomicInteger();

        // One failed attempt: attempts=1, back to PENDING.
        processor.process(event.getId(), e -> { throw new RuntimeException("transient"); });
        // Consumer claims the retry and crashes mid-attempt.
        store.claim(event.getId());

        List<EventRecord> recovered = store.recoverStuck(Instant.now().plusSeconds(1));
        assertThat(recovered).hasSize(1);
        EventRecord restored = service.get(event.getId());
        log.info("event={} recovered: status={} attempts={}/{} (progress kept)",
                restored.getId(), restored.getStatus(), restored.getAttempts(), restored.getMaxAttempts());
        assertThat(restored.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(restored.getAttempts()).isEqualTo(1); // progress not lost

        EventRecord done = processor.process(event.getId(), e -> sideEffects.incrementAndGet());
        assertThat(done.getStatus()).isEqualTo(EventStatus.SUCCEEDED);
        assertThat(sideEffects).hasValue(1);

        // The "crashed" consumer wakes up late and tries to report success:
        // rejected, state untouched, no extra side effect.
        assertThatThrownBy(() -> store.markSucceeded(event.getId()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(service.get(event.getId()).getStatus()).isEqualTo(EventStatus.SUCCEEDED);
        assertThat(sideEffects).hasValue(1);
    }

    @Test
    void recentInProgressEventsAreNotRecoveredPrematurely() {
        EventRecord event = service.submit("stuck-3", "payload");
        store.claim(event.getId());

        // Cutoff in the past: a freshly claimed event is still legitimately running.
        List<EventRecord> recovered = store.recoverStuck(Instant.now().minusSeconds(60));
        assertThat(recovered).isEmpty();
        assertThat(service.get(event.getId()).getStatus()).isEqualTo(EventStatus.IN_PROGRESS);
    }
}
