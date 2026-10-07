package com.github.h3yow.reliableeventdeliveryjava.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.github.h3yow.reliableeventdeliveryjava.Application;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * End-to-end: with the scheduling loop enabled, submitted events are
 * delivered by the system itself — no manual trigger, no external caller.
 */
@SpringBootTest(classes = {Application.class, DispatcherSchedulingTest.RecordingHandlerConfig.class},
        properties = {
                "events.dispatcher.enabled=true",
                "events.dispatcher.interval-ms=50",
                "events.retry-delay-ms=50",
                "events.recovery.stale-timeout-ms=100"
        })
class DispatcherSchedulingTest {

    private static final Logger log = LoggerFactory.getLogger(DispatcherSchedulingTest.class);

    static final List<String> APPLIED = new CopyOnWriteArrayList<>();

    @TestConfiguration
    static class RecordingHandlerConfig {
        @Bean
        @Primary
        EventHandler recordingHandler() {
            return event -> {
                APPLIED.add(event.getId());
                log.info("event={} side effect applied by scheduled dispatch", event.getId());
            };
        }
    }

    @Autowired
    private EventService service;

    @Test
    void scheduledLoopDeliversSubmittedEventsAutomatically() {
        APPLIED.clear();
        EventRecord event = service.submit("sched-1", "payload");
        log.info("event={} submitted, waiting for the scheduled loop (no manual trigger)",
                event.getId());

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(service.get(event.getId()).getStatus())
                        .isEqualTo(EventStatus.SUCCEEDED));

        assertThat(APPLIED).containsExactly(event.getId());
        log.info("event={} delivered automatically by the scheduled loop", event.getId());
    }
}
