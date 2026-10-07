package com.github.h3yow.reliableeventdeliveryjava.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Backpressure: when the backlog (PENDING + IN_PROGRESS) reaches the
 * configured capacity, new submissions are rejected with a signal that is
 * clearly distinct from a business conflict; nothing is silently dropped,
 * and acceptance resumes once the backlog drains.
 */
class CapacityBackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(CapacityBackpressureTest.class);

    private EventStore store;
    private EventService service;
    private EventProcessor processor;
    private EventDeliveryProperties properties;

    @BeforeEach
    void setUp() {
        store = new EventStore();
        properties = new EventDeliveryProperties();
        properties.getCapacity().setMaxPending(2);
        service = new EventService(store, properties);
        processor = new EventProcessor(store, properties);
    }

    @Test
    void newSubmissionsAreRejectedWhenBacklogIsFull() {
        service.submit("cap-1", "p1");
        service.submit("cap-2", "p2");

        assertThatThrownBy(() -> service.submit("cap-3", "p3"))
                .isInstanceOf(EventCapacityException.class)
                .hasMessageContaining("maxPending=2");
        log.info("backlog full at {}, new key rejected with EventCapacityException",
                store.countActive());

        // The signal is distinguishable from a business conflict.
        assertThatThrownBy(() -> service.submit("cap-1", "DIFFERENT"))
                .isInstanceOf(EventConflictException.class)
                .isNotInstanceOf(EventCapacityException.class);
    }

    @Test
    void knownKeysStayIdempotentEvenWhenFull() {
        EventRecord first = service.submit("cap-dup", "p1");
        service.submit("cap-2", "p2"); // now full

        // Re-submitting an existing key is not a new load: still idempotent.
        EventRecord again = service.submit("cap-dup", "p1");
        assertThat(again.getId()).isEqualTo(first.getId());
        log.info("event={} duplicate submit accepted idempotently while backlog full",
                again.getId());
    }

    @Test
    void acceptanceResumesAfterBacklogDrains() {
        EventRecord a = service.submit("cap-a", "p1");
        service.submit("cap-b", "p2");
        assertThatThrownBy(() -> service.submit("cap-c", "p3"))
                .isInstanceOf(EventCapacityException.class);

        // Drain one event: capacity frees up, new submissions flow again.
        processor.process(a.getId(), e -> { });
        EventRecord c = service.submit("cap-c", "p3");
        log.info("event={} accepted after backlog drained, status={}",
                c.getId(), c.getStatus());
        assertThat(c.getStatus()).isEqualTo(EventStatus.PENDING);
    }

    @Test
    void deadEventsDoNotConsumeCapacity() {
        EventRecord a = service.submit("cap-x", "p1");
        service.submit("cap-y", "p2"); // full

        // A DEAD event is terminal: it frees its slot for new submissions.
        processor.process(a.getId(), e -> { throw new NonRetryableException("bad"); });
        assertThat(service.get(a.getId()).getStatus()).isEqualTo(EventStatus.DEAD);

        EventRecord z = service.submit("cap-z", "p3");
        log.info("event={} accepted because DEAD event={} no longer occupies capacity",
                z.getId(), a.getId());
        assertThat(z.getStatus()).isEqualTo(EventStatus.PENDING);
    }
}
