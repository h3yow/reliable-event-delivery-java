package com.github.h3yow.reliableeventdeliveryjava.event;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operations summary: per-status counts, oldest waiting age, retry spread
 * and the capacity/acceptance view.
 */
class EventStatsTest {

    private static final Logger log = LoggerFactory.getLogger(EventStatsTest.class);

    private EventStore store;
    private EventService service;
    private EventProcessor processor;
    private EventDeliveryProperties properties;

    @BeforeEach
    void setUp() {
        store = new EventStore();
        properties = new EventDeliveryProperties();
        properties.getCapacity().setMaxPending(10);
        service = new EventService(store, properties);
        processor = new EventProcessor(store, properties);
    }

    @Test
    void emptyStoreReportsZerosAndAccepting() {
        EventStatsView stats = service.stats();
        log.info("empty stats: counts={} active={} accepting={}",
                stats.counts(), stats.active(), stats.accepting());
        assertThat(stats.counts().get(EventStatus.PENDING)).isZero();
        assertThat(stats.active()).isZero();
        assertThat(stats.accepting()).isTrue();
        assertThat(stats.oldestPendingAgeMillis()).isNull();
        assertThat(stats.retryDistribution()).isEmpty();
    }

    @Test
    void summaryReflectsEveryStatusAndRetrySpread() throws Exception {
        EventRecord pending = service.submit("s-pending", "p");
        EventRecord ok = service.submit("s-ok", "p");
        EventRecord flaky = service.submit("s-flaky", "p");
        EventRecord dead = service.submit("s-dead", "p");
        EventRecord stuck = service.submit("s-stuck", "p");

        processor.process(ok.getId(), e -> { });
        processor.process(flaky.getId(), e -> { throw new RuntimeException("t1"); });
        processor.process(flaky.getId(), e -> { throw new RuntimeException("t2"); });
        processor.process(dead.getId(), e -> { throw new NonRetryableException("bad"); });
        store.claim(stuck.getId());

        Thread.sleep(20); // give the pending event a measurable age
        EventStatsView stats = service.stats();
        log.info("stats: counts={} active={}/{} oldestPendingAgeMs={} retries={}",
                stats.counts(), stats.active(), stats.maxPending(),
                stats.oldestPendingAgeMillis(), stats.retryDistribution());

        assertThat(stats.counts().get(EventStatus.PENDING)).isEqualTo(2);   // pending + flaky
        assertThat(stats.counts().get(EventStatus.IN_PROGRESS)).isEqualTo(1); // stuck
        assertThat(stats.counts().get(EventStatus.SUCCEEDED)).isEqualTo(1);   // ok
        assertThat(stats.counts().get(EventStatus.DEAD)).isEqualTo(1);        // dead
        assertThat(stats.active()).isEqualTo(3);

        // Oldest waiting event is the first submitted PENDING one.
        assertThat(stats.oldestPendingAgeMillis()).isNotNull().isGreaterThanOrEqualTo(0);

        // Retry spread: flaky used 2 attempts, dead used 1.
        assertThat(stats.retryDistribution()).containsEntry(1, 1L).containsEntry(2, 1L);
    }

    @Test
    void statsExposeBackpressureState() {
        properties.getCapacity().setMaxPending(1);
        service.submit("s-full", "p");

        EventStatsView stats = service.stats();
        log.info("backpressure stats: active={}/{} accepting={}",
                stats.active(), stats.maxPending(), stats.accepting());
        assertThat(stats.active()).isEqualTo(1);
        assertThat(stats.maxPending()).isEqualTo(1);
        assertThat(stats.accepting()).isFalse();
    }
}
