package com.github.h3yow.reliableeventdeliveryjava.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * The dispatcher drains pending events on its own: no manual per-event
 * trigger is needed, retry backoff of one event never blocks the others,
 * and the scheduling loop honours the enabled/disabled toggle.
 */
class DispatcherAutoRunTest {

    private static final Logger log = LoggerFactory.getLogger(DispatcherAutoRunTest.class);

    private static final long RETRY_DELAY_MS = 150;

    private EventStore store;
    private EventService service;
    private EventProcessor processor;

    @BeforeEach
    void setUp() {
        store = new EventStore();
        service = new EventService(store, 3);
        processor = new EventProcessor(store, RETRY_DELAY_MS);
    }

    @Test
    void submittedEventsAreDeliveredWithoutManualTrigger() {
        List<String> applied = new CopyOnWriteArrayList<>();
        EventDispatcher dispatcher = new EventDispatcher(store, processor,
                e -> applied.add(e.getId()), 30_000);

        EventRecord first = service.submit("auto-1", "payload-1");
        EventRecord second = service.submit("auto-2", "payload-2");
        log.info("submitted event={} and event={}, running automatic sweep",
                first.getId(), second.getId());

        int attempts = dispatcher.runOnce();

        assertThat(attempts).isEqualTo(2);
        assertThat(applied).containsExactlyInAnyOrder(first.getId(), second.getId());
        assertThat(service.get(first.getId()).getStatus()).isEqualTo(EventStatus.SUCCEEDED);
        assertThat(service.get(second.getId()).getStatus()).isEqualTo(EventStatus.SUCCEEDED);
        log.info("both events reached SUCCEEDED via automatic dispatch");
    }

    @Test
    void eventWaitingForRetryDoesNotBlockOtherEvents() throws Exception {
        AtomicInteger flakyCalls = new AtomicInteger();
        EventHandler handler = e -> {
            if (e.getIdempotencyKey().equals("flaky") && flakyCalls.incrementAndGet() == 1) {
                throw new RuntimeException("transient downstream error");
            }
        };
        EventDispatcher dispatcher = new EventDispatcher(store, processor, handler, 30_000);

        EventRecord flaky = service.submit("flaky", "payload");
        EventRecord healthy = service.submit("healthy", "payload");

        // Sweep 1: flaky fails and backs off; healthy succeeds in the same sweep.
        dispatcher.runOnce();
        assertThat(service.get(flaky.getId()).getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(service.get(flaky.getId()).getNextAttemptAt()).isNotNull();
        assertThat(service.get(healthy.getId()).getStatus()).isEqualTo(EventStatus.SUCCEEDED);
        log.info("event={} backing off until {}", flaky.getId(),
                service.get(flaky.getId()).getNextAttemptAt());

        // Sweep 2 (immediately): flaky is not due yet; a newly submitted event
        // is still delivered, proving the backoff blocks nobody else.
        EventRecord late = service.submit("late", "payload");
        int attempts = dispatcher.runOnce();
        assertThat(attempts).isEqualTo(1);
        assertThat(service.get(late.getId()).getStatus()).isEqualTo(EventStatus.SUCCEEDED);
        assertThat(flakyCalls).hasValue(1); // no premature retry of flaky
        log.info("event={} still backing off, event={} delivered meanwhile",
                flaky.getId(), late.getId());

        // Sweep 3 (after the backoff elapses): flaky is retried and succeeds.
        Thread.sleep(RETRY_DELAY_MS + 100);
        dispatcher.runOnce();
        assertThat(flakyCalls).hasValue(2);
        assertThat(service.get(flaky.getId()).getStatus()).isEqualTo(EventStatus.SUCCEEDED);
        log.info("event={} retried after backoff and reached SUCCEEDED", flaky.getId());
    }

    @Test
    void schedulingLoopIsConditionalOnEnabledFlag() {
        // Enabled: the scheduling bean is present.
        try (var ctx = new AnnotationConfigApplicationContext()) {
            TestPropertyValues.of("events.dispatcher.enabled=true").applyTo(ctx);
            ctx.register(EventStore.class, EventProcessor.class, EventDispatcher.class,
                    EventDeliveryConfig.class);
            ctx.refresh();
            assertThat(ctx.containsBean(
                    "com.github.h3yow.reliableeventdeliveryjava.event.EventDeliveryConfig$Scheduling"))
                    .isTrue();
            log.info("dispatcher scheduling active when enabled=true");
        }
        // Disabled: the scheduling bean is absent, dispatcher itself still usable manually.
        try (var ctx = new AnnotationConfigApplicationContext()) {
            TestPropertyValues.of("events.dispatcher.enabled=false").applyTo(ctx);
            ctx.register(EventStore.class, EventProcessor.class, EventDispatcher.class,
                    EventDeliveryConfig.class);
            ctx.refresh();
            assertThat(ctx.containsBean(
                    "com.github.h3yow.reliableeventdeliveryjava.event.EventDeliveryConfig$Scheduling"))
                    .isFalse();
            assertThat(ctx.getBean(EventDispatcher.class)).isNotNull();
            log.info("dispatcher scheduling inactive when enabled=false, manual runOnce still available");
        }
    }
}
