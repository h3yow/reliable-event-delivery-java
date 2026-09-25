package com.github.h3yow.reliableeventdeliveryjava.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded retries: retryable failures return to PENDING until the attempt
 * budget is exhausted, then the event goes DEAD with a distinguishable
 * reason. DEAD events are queryable, replayable, and never block others.
 */
class RetryExhaustionTest {

    private static final Logger log = LoggerFactory.getLogger(RetryExhaustionTest.class);

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
    void retryableFailureExhaustsBudgetThenDies() {
        EventRecord event = service.submit("flaky-1", "payload");
        AtomicInteger calls = new AtomicInteger();
        EventHandler alwaysFail = e -> {
            calls.incrementAndGet();
            throw new RuntimeException("transient downstream error");
        };

        EventRecord r1 = processor.process(event.getId(), alwaysFail);
        assertThat(r1.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(r1.getAttempts()).isEqualTo(1);

        EventRecord r2 = processor.process(event.getId(), alwaysFail);
        assertThat(r2.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(r2.getAttempts()).isEqualTo(2);

        EventRecord r3 = processor.process(event.getId(), alwaysFail);
        log.info("event={} exhausted: status={} deadReason={} attempts={}/{}",
                r3.getId(), r3.getStatus(), r3.getDeadReason(), r3.getAttempts(), r3.getMaxAttempts());

        assertThat(r3.getStatus()).isEqualTo(EventStatus.DEAD);
        assertThat(r3.getDeadReason()).isEqualTo(DeadReason.RETRY_EXHAUSTED);
        assertThat(r3.getLastError()).contains("transient downstream error");
        assertThat(calls).hasValue(3);

        // Terminal: further processing is a no-op until replay.
        EventRecord still = processor.process(event.getId(), alwaysFail);
        assertThat(still.getStatus()).isEqualTo(EventStatus.DEAD);
        assertThat(calls).hasValue(3);
    }

    @Test
    void nonRetryableFailureDiesImmediately() {
        EventRecord event = service.submit("bad-1", "payload");

        EventRecord result = processor.process(event.getId(),
                e -> { throw new NonRetryableException("schema invalid"); });

        log.info("event={} non-retryable: status={} deadReason={}",
                result.getId(), result.getStatus(), result.getDeadReason());

        assertThat(result.getStatus()).isEqualTo(EventStatus.DEAD);
        assertThat(result.getDeadReason()).isEqualTo(DeadReason.NON_RETRYABLE);
        assertThat(result.getAttempts()).isEqualTo(1);
    }

    @Test
    void deadEventCanBeReplayedAndThenSucceed() {
        EventRecord event = service.submit("replay-1", "payload");
        processor.process(event.getId(), e -> { throw new NonRetryableException("boom"); });
        assertThat(service.get(event.getId()).getStatus()).isEqualTo(EventStatus.DEAD);

        EventRecord replayed = service.replay(event.getId());
        log.info("event={} replayed: status={} attempts={}",
                replayed.getId(), replayed.getStatus(), replayed.getAttempts());
        assertThat(replayed.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(replayed.getAttempts()).isZero();

        EventRecord done = processor.process(event.getId(), e -> { });
        assertThat(done.getStatus()).isEqualTo(EventStatus.SUCCEEDED);
    }

    @Test
    void deadEventDoesNotBlockOtherEvents() {
        EventRecord dead = service.submit("dead-1", "a");
        EventRecord healthy = service.submit("ok-1", "b");

        processor.process(dead.getId(), e -> { throw new NonRetryableException("boom"); });
        EventRecord result = processor.process(healthy.getId(), e -> { });

        log.info("event={} healthy event delivered despite event={} being DEAD",
                result.getId(), dead.getId());

        assertThat(service.get(dead.getId()).getStatus()).isEqualTo(EventStatus.DEAD);
        assertThat(result.getStatus()).isEqualTo(EventStatus.SUCCEEDED);
    }
}
