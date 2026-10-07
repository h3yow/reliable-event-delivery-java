package com.github.h3yow.reliableeventdeliveryjava.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Enables the dispatcher scheduling and provides the default side-effect
 * handler. Applications override the handler by declaring their own
 * {@link EventHandler} bean.
 */
@Configuration
@EnableScheduling
public class EventDeliveryConfig {

    private static final Logger log = LoggerFactory.getLogger(EventDeliveryConfig.class);

    /** Default handler: logs the delivery and succeeds (local demo semantics). */
    @Bean
    @ConditionalOnMissingBean
    public EventHandler eventHandler() {
        return event -> log.info("event={} delivered (default handler), payload={}",
                event.getId(), event.getPayload());
    }
}
