package com.github.h3yow.reliableeventdeliveryjava.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concurrent / duplicate consumption of the same event must apply the side
 * effect exactly once, and progress must survive interruption mid-attempt.
 */
class ConcurrentConsumptionTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentConsumptionTest.class);

    private EventStore store;
    private EventService service;
    private EventProcessor processor;

    @BeforeEach
    void setUp() {
        store = new EventStore();
        service = new EventService(store, 3);
        processor = new EventProcessor(store);
    }

    @Test
    void concurrentConsumersApplySideEffectExactlyOnce() throws Exception {
        EventRecord event = service.submit("race-1", "payload");
        AtomicInteger sideEffects = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        EventHandler slowHandler = e -> {
            sideEffects.incrementAndGet();
            entered.countDown();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
        };

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<EventRecord>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return processor.process(event.getId(), slowHandler);
            }));
        }
        start.countDown();
        // Hold the winner inside the handler while the others lose the claim.
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(100);
        release.countDown();

        for (Future<EventRecord> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        EventRecord finished = service.get(event.getId());
        log.info("event={} after {} concurrent consumers: status={} sideEffects={}",
                finished.getId(), threads, finished.getStatus(), sideEffects.get());

        assertThat(sideEffects).hasValue(1);
        assertThat(finished.getStatus()).isEqualTo(EventStatus.SUCCEEDED);
    }

    @Test
    void duplicateDeliveryAfterSuccessIsSuppressed() {
        EventRecord event = service.submit("dup-1", "payload");
        AtomicInteger sideEffects = new AtomicInteger();

        processor.process(event.getId(), e -> sideEffects.incrementAndGet());
        EventRecord again = processor.process(event.getId(), e -> sideEffects.incrementAndGet());

        log.info("event={} duplicate delivery suppressed, sideEffects={}",
                again.getId(), sideEffects.get());

        assertThat(sideEffects).hasValue(1);
        assertThat(again.getStatus()).isEqualTo(EventStatus.SUCCEEDED);
    }

    @Test
    void interruptionMidAttemptLeavesRecoverableState() {
        EventRecord event = service.submit("crash-1", "payload");
        // Simulate a consumer that claims the event and then "crashes"
        // (never reports success or failure).
        EventRecord claimed = store.claim(event.getId()).orElseThrow();
        assertThat(claimed.getStatus()).isEqualTo(EventStatus.IN_PROGRESS);

        // Recovery: the record is intact and can be failed back to PENDING,
        // then delivered by a new consumer without loss.
        EventRecord recovered = store.markFailed(event.getId(), true, "consumer interrupted");
        log.info("event={} recovered after interruption: status={} attempts={}",
                recovered.getId(), recovered.getStatus(), recovered.getAttempts());
        assertThat(recovered.getStatus()).isEqualTo(EventStatus.PENDING);

        AtomicInteger sideEffects = new AtomicInteger();
        EventRecord done = processor.process(event.getId(), e -> sideEffects.incrementAndGet());
        assertThat(done.getStatus()).isEqualTo(EventStatus.SUCCEEDED);
        assertThat(sideEffects).hasValue(1);
    }
}
