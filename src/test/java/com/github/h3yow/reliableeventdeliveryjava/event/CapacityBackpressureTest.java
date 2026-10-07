package com.github.h3yow.reliableeventdeliveryjava.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Backpressure: when the backlog (PENDING + IN_PROGRESS) reaches the
 * configured capacity, new submissions are rejected with a dedicated
 * overload signal — never silently dropped, never a business conflict,
 * never an internal fault — and admission reopens once the backlog drains.
 */
class CapacityBackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(CapacityBackpressureTest.class);

    private EventStore store;
    private EventService service;
    private EventProcessor processor;
    private EventDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        store = new EventStore();
        service = new EventService(store, 3, 2); // capacity: 2 pending events
        processor = new EventProcessor(store, 0);
        dispatcher = new EventDispatcher(store, processor, e -> { }, 30_000);
    }

    @Test
    void submissionsBeyondCapacityGetDedicatedOverloadSignal() {
        service.submit("k1", "p1");
        service.submit("k2", "p2");
        log.info("backlog at capacity 2, next submit must be rejected");

        assertThatThrownBy(() -> service.submit("k3", "p3"))
                .isInstanceOf(EventOverloadException.class)
                .isNotInstanceOf(EventConflictException.class)
                .hasMessageContaining("capacity");

        // The rejected event is not silently stored.
        assertThat(store.findAll()).hasSize(2);
        assertThat(store.countBacklog()).isEqualTo(2);
        log.info("overload rejected explicitly, nothing dropped silently");
    }

    @Test
    void idempotentResubmitStillWorksWhileOverloaded() {
        EventRecord first = service.submit("k1", "p1");
        service.submit("k2", "p2");

        // Re-submitting an already-known key is answered idempotently even at
        // capacity: it creates no new backlog, so it is not backpressured.
        EventRecord again = service.submit("k1", "p1");
        assertThat(again.getId()).isEqualTo(first.getId());

        // But a conflicting payload for the known key is still a 409-style conflict.
        assertThatThrownBy(() -> service.submit("k1", "different"))
                .isInstanceOf(EventConflictException.class);
        log.info("idempotent resubmit and conflict detection unaffected by backpressure");
    }

    @Test
    void admissionReopensAfterBacklogDrains() {
        service.submit("k1", "p1");
        service.submit("k2", "p2");
        assertThatThrownBy(() -> service.submit("k3", "p3"))
                .isInstanceOf(EventOverloadException.class);

        // Drain the backlog: both events delivered successfully.
        dispatcher.runOnce();
        assertThat(store.countBacklog()).isZero();
        log.info("backlog drained, capacity available again");

        EventRecord accepted = service.submit("k3", "p3");
        assertThat(accepted.getStatus()).isEqualTo(EventStatus.PENDING);
        log.info("event={} accepted after recovery from overload", accepted.getId());
    }

    @Test
    void inProgressEventsCountTowardsCapacity() {
        service.submit("k1", "p1");
        EventRecord second = service.submit("k2", "p2");
        store.claim(second.getId()); // now IN_PROGRESS, still backlog

        assertThatThrownBy(() -> service.submit("k3", "p3"))
                .isInstanceOf(EventOverloadException.class);
        log.info("IN_PROGRESS event={} still counts towards the backlog cap", second.getId());
    }

    @Test
    void deadAndSucceededEventsFreeUpCapacity() {
        service.submit("k1", "p1");
        EventRecord second = service.submit("k2", "p2");

        // k2 fails permanently (non-retryable) -> DEAD, no longer backlog.
        store.claim(second.getId());
        store.markFailed(second.getId(), false, "permanent failure");
        assertThat(service.get(second.getId()).getStatus()).isEqualTo(EventStatus.DEAD);

        EventRecord accepted = service.submit("k3", "p3");
        assertThat(accepted.getStatus()).isEqualTo(EventStatus.PENDING);
        log.info("DEAD event freed capacity, event={} accepted", accepted.getId());
    }
}
