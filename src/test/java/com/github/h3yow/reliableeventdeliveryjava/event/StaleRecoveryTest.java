package com.github.h3yow.reliableeventdeliveryjava.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Events whose consumer vanished mid-attempt (process crash, hung handler)
 * are reclaimed automatically after the stale timeout and continue delivery
 * without losing progress and without duplicating side effects.
 */
class StaleRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(StaleRecoveryTest.class);

    private static final long STALE_TIMEOUT_MS = 100;

    private EventStore store;
    private EventService service;
    private EventProcessor processor;
    private List<String> applied;
    private EventDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        store = new EventStore();
        service = new EventService(store, 3);
        processor = new EventProcessor(store, 0);
        applied = new CopyOnWriteArrayList<>();
        dispatcher = new EventDispatcher(store, processor,
                e -> applied.add(e.getId()), STALE_TIMEOUT_MS);
    }

    @Test
    void crashedAttemptIsReclaimedAndDeliveredExactlyOnce() throws Exception {
        EventRecord event = service.submit("crash-1", "payload");

        // Simulate a consumer that claimed the event and then died:
        // IN_PROGRESS forever, no markSucceeded / markFailed.
        store.claim(event.getId());
        assertThat(service.get(event.getId()).getStatus()).isEqualTo(EventStatus.IN_PROGRESS);
        log.info("event={} stuck IN_PROGRESS, simulating crashed consumer", event.getId());

        // Before the stale timeout the event must NOT be reclaimed.
        dispatcher.runOnce();
        assertThat(service.get(event.getId()).getStatus()).isEqualTo(EventStatus.IN_PROGRESS);
        assertThat(applied).isEmpty();

        // After the timeout the sweep reclaims it and delivers it in the same pass.
        Thread.sleep(STALE_TIMEOUT_MS + 100);
        dispatcher.runOnce();

        assertThat(service.get(event.getId()).getStatus()).isEqualTo(EventStatus.SUCCEEDED);
        assertThat(applied).containsExactly(event.getId());
        log.info("event={} recovered and SUCCEEDED, side effect applied once", event.getId());

        // Further sweeps never re-apply the side effect.
        dispatcher.runOnce();
        assertThat(applied).hasSize(1);
    }

    @Test
    void recoveryPreservesRetryProgress() throws Exception {
        AtomicInteger failures = new AtomicInteger(1); // fail the first attempt only
        EventProcessor flakyProcessor = new EventProcessor(store, 0);
        EventDispatcher flakyDispatcher = new EventDispatcher(store, flakyProcessor, e -> {
            if (failures.getAndDecrement() > 0) {
                throw new RuntimeException("transient downstream error");
            }
            applied.add(e.getId());
        }, STALE_TIMEOUT_MS);

        EventRecord event = service.submit("crash-2", "payload");

        // One real failed attempt: attempts=1, back to PENDING.
        flakyDispatcher.runOnce();
        assertThat(service.get(event.getId()).getAttempts()).isEqualTo(1);
        assertThat(service.get(event.getId()).getStatus()).isEqualTo(EventStatus.PENDING);

        // Second attempt crashes mid-flight after being claimed.
        store.claim(event.getId());
        log.info("event={} crashed mid-attempt with attempts=1", event.getId());
        Thread.sleep(STALE_TIMEOUT_MS + 100);

        // Recovery: back to PENDING with the attempt budget intact (the
        // interrupted attempt reported no outcome, so it is not charged),
        // then delivered successfully in the same sweep.
        flakyDispatcher.runOnce();
        EventRecord recovered = service.get(event.getId());
        assertThat(recovered.getStatus()).isEqualTo(EventStatus.SUCCEEDED);
        // attempts counts failed attempts only: 1 real failure, the crashed
        // attempt was not charged, and the successful delivery adds nothing.
        assertThat(recovered.getAttempts()).isEqualTo(1);
        assertThat(applied).containsExactly(event.getId());
        log.info("event={} recovered with attempts={}/{}, progress preserved",
                event.getId(), recovered.getAttempts(), recovered.getMaxAttempts());
    }

    @Test
    void freshInProgressEventsAreNotReclaimed() {
        EventRecord event = service.submit("inflight-1", "payload");
        store.claim(event.getId());

        // Sweep well within the stale timeout: nothing happens.
        int attempts = dispatcher.runOnce();

        assertThat(attempts).isZero();
        assertThat(service.get(event.getId()).getStatus()).isEqualTo(EventStatus.IN_PROGRESS);
        assertThat(applied).isEmpty();
        log.info("event={} still IN_PROGRESS, fresh attempt untouched by recovery", event.getId());
    }

    @Test
    void reclaimStaleOnlyAffectsStaleEvents() throws Exception {
        EventRecord stale = service.submit("stale-1", "payload");
        EventRecord fresh = service.submit("fresh-1", "payload");
        store.claim(stale.getId());
        Thread.sleep(STALE_TIMEOUT_MS + 100);
        store.claim(fresh.getId()); // claimed after the cutoff

        List<EventRecord> reclaimed = store.reclaimStale(Instant.now().minusMillis(STALE_TIMEOUT_MS));

        assertThat(reclaimed).extracting(EventRecord::getId).containsExactly(stale.getId());
        assertThat(service.get(stale.getId()).getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(service.get(fresh.getId()).getStatus()).isEqualTo(EventStatus.IN_PROGRESS);
        log.info("reclaimed only stale event={}, fresh event={} untouched",
                stale.getId(), fresh.getId());
    }
}
