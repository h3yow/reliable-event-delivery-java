package com.github.h3yow.reliableeventdeliveryjava.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Wires the automatic dispatch loop and a default handler.
 *
 * <p>The periodic sweep is enabled by default and can be switched off with
 * {@code events.dispatcher.enabled=false} (useful for tests and demos that
 * prefer to drive {@link EventDispatcher#runOnce()} manually). The cadence
 * is {@code events.dispatcher.interval-ms} (fixed delay between sweeps).
 */
@Configuration
@EnableScheduling
public class EventDeliveryConfig {

    /**
     * Default side effect: logs and succeeds. Replace with a real
     * {@link EventHandler} bean to integrate an actual downstream; the
     * handler must be idempotent per event id.
     */
    @Bean
    @ConditionalOnMissingBean
    public EventHandler eventHandler() {
        Logger log = LoggerFactory.getLogger("com.github.h3yow.reliableeventdeliveryjava.event.DefaultEventHandler");
        return event -> log.info("event={} default handler applied side effect, payload={}",
                event.getId(), event.getPayload());
    }

    /** Periodic dispatch loop, active only when explicitly enabled (default on). */
    @Configuration
    @ConditionalOnProperty(name = "events.dispatcher.enabled", havingValue = "true", matchIfMissing = true)
    public static class Scheduling {

        private static final Logger log = LoggerFactory.getLogger(Scheduling.class);

        private final EventDispatcher dispatcher;

        public Scheduling(EventDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Scheduled(fixedDelayString = "${events.dispatcher.interval-ms:1000}")
        public void sweep() {
            try {
                dispatcher.runOnce();
            } catch (Exception e) {
                // The loop must never die; individual events already carry their own state.
                log.error("dispatch sweep failed, will retry next interval", e);
            }
        }
    }
}
