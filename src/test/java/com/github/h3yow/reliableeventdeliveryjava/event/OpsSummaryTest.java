package com.github.h3yow.reliableeventdeliveryjava.event;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The operational summary explains where the backlog sits: per-status
 * counts, how long the oldest pending event has been waiting, and how
 * retries are distributed across the attempt range.
 */
class OpsSummaryTest {

    private static final Logger log = LoggerFactory.getLogger(OpsSummaryTest.class);

    private EventStore store;
    private EventService service;
    private EventProcessor processor;

    @BeforeEach
    void setUp() {
        store = new EventStore();
        service = new EventService(store, 5, 100);
        processor = new EventProcessor(store, 0);
    }

    @Test
    void summaryCountsEveryStatus() {
        EventRecord pending = service.submit("s-pending", "p");
        EventRecord inProgress = service.submit("s-inprogress", "p");
        EventRecord succeeded = service.submit("s-succeeded", "p");
        EventRecord dead = service.submit("s-dead", "p");

        store.claim(inProgress.getId());
        processor.process(succeeded.getId(), e -> { });
        store.claim(dead.getId());
        store.markFailed(dead.getId(), false, "permanent");

        EventStats stats = service.stats();

        assertThat(stats.countsByStatus())
                .containsEntry(EventStatus.PENDING, 1L)
                .containsEntry(EventStatus.IN_PROGRESS, 1L)
                .containsEntry(EventStatus.SUCCEEDED, 1L)
                .containsEntry(EventStatus.DEAD, 1L);
        assertThat(stats.backlog()).isEqualTo(2); // PENDING + IN_PROGRESS
        assertThat(stats.maxPending()).isEqualTo(100);
        log.info("summary counts={} backlog={}/{}", stats.countsByStatus(),
                stats.backlog(), stats.maxPending());
    }

    @Test
    void summaryReportsOldestPendingWait() throws Exception {
        EventRecord oldest = service.submit("w-old", "p");
        Thread.sleep(120);
        service.submit("w-new", "p");

        EventStats stats = service.stats();

        assertThat(stats.oldestPendingWaitMs()).isNotNull();
        assertThat(stats.oldestPendingWaitMs()).isGreaterThanOrEqualTo(100);
        log.info("oldest pending event={} waiting ~{}ms", oldest.getId(),
                stats.oldestPendingWaitMs());

        // Once everything is delivered the wait signal disappears.
        EventDispatcher dispatcher = new EventDispatcher(store, processor, e -> { }, 30_000);
        dispatcher.runOnce();
        assertThat(service.stats().oldestPendingWaitMs()).isNull();
    }

    @Test
    void summaryShowsRetryDistribution() {
        // One event that failed twice (attempts=2, still pending),
        // one that failed once then died non-retryable... use retryable path:
        EventRecord twice = service.submit("r-twice", "p");
        EventRecord once = service.submit("r-once", "p");
        EventRecord clean = service.submit("r-clean", "p");

        fail(twice.getId());
        fail(twice.getId());
        fail(once.getId());

        EventStats stats = service.stats();

        assertThat(stats.retryDistribution())
                .containsEntry(1, 1L)   // r-once used 1 attempt
                .containsEntry(2, 1L);  // r-twice used 2 attempts
        assertThat(stats.retryDistribution()).doesNotContainKey(0); // r-clean not listed
        log.info("retry distribution={} (attempts -> events)", stats.retryDistribution());
    }

    @Test
    void emptyStoreReportsZeros() {
        EventStats stats = service.stats();

        assertThat(stats.countsByStatus().values()).allMatch(c -> c == 0L);
        assertThat(stats.backlog()).isZero();
        assertThat(stats.oldestPendingWaitMs()).isNull();
        assertThat(stats.retryDistribution()).isEmpty();
    }

    private void fail(String eventId) {
        store.claim(eventId);
        store.markFailed(eventId, true, "transient");
    }
}
