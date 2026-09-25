package com.github.h3yow.reliableeventdeliveryjava.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Duplicate submission of the same business action must not create extra
 * deliveries, and conflicting resubmissions must be rejected with a
 * distinguishable reason.
 */
class DuplicateSubmissionTest {

    private static final Logger log = LoggerFactory.getLogger(DuplicateSubmissionTest.class);

    private EventStore store;
    private EventService service;

    @BeforeEach
    void setUp() {
        store = new EventStore();
        service = new EventService(store, 3);
    }

    @Test
    void sameKeyAndPayloadIsIdempotent() {
        EventRecord first = service.submit("order-1", "{\"amount\":100}");
        EventRecord second = service.submit("order-1", "{\"amount\":100}");

        log.info("event={} idempotent resubmit resolved to same record, status={}",
                second.getId(), second.getStatus());

        assertThat(second.getId()).isEqualTo(first.getId());
        assertThat(store.findAll()).hasSize(1);
        assertThat(second.getStatus()).isEqualTo(EventStatus.PENDING);
    }

    @Test
    void sameKeyDifferentPayloadIsRejectedAsConflict() {
        EventRecord first = service.submit("order-2", "{\"amount\":100}");

        assertThatThrownBy(() -> service.submit("order-2", "{\"amount\":200}"))
                .isInstanceOfSatisfying(EventConflictException.class, e -> {
                    log.info("event={} conflict rejected for key={}: {}",
                            first.getId(), e.getIdempotencyKey(), e.getMessage());
                    assertThat(e.getIdempotencyKey()).isEqualTo("order-2");
                });

        assertThat(store.findAll()).hasSize(1);
        assertThat(service.get(first.getId()).getPayload()).isEqualTo("{\"amount\":100}");
    }

    @Test
    void resubmitAfterSuccessDoesNotCreateNewDelivery() {
        EventRecord first = service.submit("order-3", "payload");
        EventProcessor processor = new EventProcessor(store);
        processor.process(first.getId(), e -> { });
        assertThat(service.get(first.getId()).getStatus()).isEqualTo(EventStatus.SUCCEEDED);

        EventRecord again = service.submit("order-3", "payload");
        log.info("event={} resubmit after success, status={}", again.getId(), again.getStatus());

        assertThat(again.getId()).isEqualTo(first.getId());
        assertThat(again.getStatus()).isEqualTo(EventStatus.SUCCEEDED);
        assertThat(store.findAll()).hasSize(1);
    }
}
