package com.github.h3yow.reliableeventdeliveryjava.event;

import static org.assertj.core.api.Assertions.assertThat;

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
 * The dispatcher drains pending events on its own: no manual trigger needed,
 * failed events retry on a bounded, delayed cadence, and an event waiting out
 * its backoff never blocks the others.
 */
class DispatcherAutoRunTest {

    private static final Logger log = LoggerFactory.getLogger(DispatcherAutoRunTest.class);

    /** Runs each task synchronously inside tick(): deterministic tests. */
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
    private EventDeliveryProperties properties;

    @BeforeEach
    void setUp() {
        store = new EventStore();
        properties = new EventDeliveryProperties();
        service = new EventService(store, properties);
    }

    private EventDispatcher dispatcher(EventHandler handler) {
        return new EventDispatcher(store, new EventProcessor(store, properties),
                properties, handler, DIRECT);
    }

    @Test
    void pendingEventsAreDeliveredWithoutManualTrigger() {
        EventRecord a = service.submit("auto-1", "p1");
        EventRecord b = service.submit("auto-2", "p2");
        AtomicInteger delivered = new AtomicInteger();

        dispatcher(e -> delivered.incrementAndGet()).tick();

        log.info("event={} status={} / event={} status={} after one tick",
                a.getId(), service.get(a.getId()).getStatus(),
                b.getId(), service.get(b.getId()).getStatus());

        assertThat(delivered).hasValue(2);
        assertThat(service.get(a.getId()).getStatus()).isEqualTo(EventStatus.SUCCEEDED);
        assertThat(service.get(b.getId()).getStatus()).isEqualTo(EventStatus.SUCCEEDED);
    }

    @Test
    void eventWaitingForRetryDoesNotBlockOthers() {
        properties.getRetry().setInitialBackoffMs(60_000); // A won't be due again soon
        EventRecord flaky = service.submit("auto-flaky", "p1");
        AtomicInteger flakyCalls = new AtomicInteger();

        EventDispatcher dispatcher = dispatcher(e -> {
            flakyCalls.incrementAndGet();
            throw new RuntimeException("transient");
        });
        dispatcher.tick();

        EventRecord waiting = service.get(flaky.getId());
        log.info("event={} waiting for retry: status={} attempts={} nextAttemptAt={}",
                waiting.getId(), waiting.getStatus(), waiting.getAttempts(), waiting.getNextAttemptAt());
        assertThat(waiting.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(waiting.getNextAttemptAt()).isNotNull();

        // A healthy event submitted later is delivered immediately, while the
        // flaky one is not due and stays untouched.
        EventRecord healthy = service.submit("auto-healthy", "p2");
        AtomicInteger healthyCalls = new AtomicInteger();
        dispatcher(e -> healthyCalls.incrementAndGet()).tick();

        log.info("event={} delivered while event={} waits out its backoff",
                healthy.getId(), flaky.getId());
        assertThat(healthyCalls).hasValue(1);
        assertThat(service.get(healthy.getId()).getStatus()).isEqualTo(EventStatus.SUCCEEDED);
        assertThat(flakyCalls).hasValue(1); // not retried before its backoff elapses
        assertThat(service.get(flaky.getId()).getStatus()).isEqualTo(EventStatus.PENDING);
    }

    @Test
    void failedEventIsRetriedAutomaticallyAfterBackoff() throws Exception {
        properties.getRetry().setInitialBackoffMs(30);
        EventRecord event = service.submit("auto-retry", "p1");
        AtomicInteger calls = new AtomicInteger();
        EventDispatcher dispatcher = dispatcher(e -> {
            if (calls.incrementAndGet() == 1) {
                throw new RuntimeException("transient");
            }
        });

        dispatcher.tick(); // first attempt fails, retry scheduled
        assertThat(service.get(event.getId()).getStatus()).isEqualTo(EventStatus.PENDING);

        dispatcher.tick(); // backoff not elapsed yet: nothing happens
        assertThat(calls).hasValue(1);

        Thread.sleep(60);
        dispatcher.tick(); // backoff elapsed: automatic retry succeeds

        EventRecord done = service.get(event.getId());
        log.info("event={} retried automatically: status={} attempts={}",
                done.getId(), done.getStatus(), done.getAttempts());
        assertThat(done.getStatus()).isEqualTo(EventStatus.SUCCEEDED);
        assertThat(calls).hasValue(2);
    }

    @Test
    void retriesStayBoundedAndEndInDead() {
        properties.getRetry().setInitialBackoffMs(0); // due again immediately
        properties.setMaxAttempts(3);
        EventRecord event = service.submit("auto-exhaust", "p1");
        AtomicInteger calls = new AtomicInteger();
        EventDispatcher dispatcher = dispatcher(e -> {
            calls.incrementAndGet();
            throw new RuntimeException("always fails");
        });

        for (int i = 0; i < 5; i++) {
            dispatcher.tick();
        }

        EventRecord dead = service.get(event.getId());
        log.info("event={} exhausted automatically: status={} deadReason={} attempts={}/{}",
                dead.getId(), dead.getStatus(), dead.getDeadReason(),
                dead.getAttempts(), dead.getMaxAttempts());
        assertThat(dead.getStatus()).isEqualTo(EventStatus.DEAD);
        assertThat(dead.getDeadReason()).isEqualTo(DeadReason.RETRY_EXHAUSTED);
        assertThat(calls).hasValue(3); // bounded: no attempts beyond the budget
    }
}
