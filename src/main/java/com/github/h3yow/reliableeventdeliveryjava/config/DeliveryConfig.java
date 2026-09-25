package com.github.h3yow.reliableeventdeliveryjava.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.h3yow.reliableeventdeliveryjava.domain.RetryPolicy;

/** 投递参数绑定与重试策略 Bean。 */
@Configuration
@EnableConfigurationProperties(DeliveryProperties.class)
public class DeliveryConfig {

    @Bean
    public RetryPolicy retryPolicy(DeliveryProperties props) {
        return new RetryPolicy(props.getMaxAttempts(), props.getInitialBackoff(),
                props.getBackoffMultiplier(), props.getMaxBackoff());
    }
}
