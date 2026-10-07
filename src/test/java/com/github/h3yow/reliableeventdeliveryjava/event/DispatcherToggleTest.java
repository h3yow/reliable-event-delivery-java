package com.github.h3yow.reliableeventdeliveryjava.event;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.h3yow.reliableeventdeliveryjava.Application;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * The automatic dispatcher is on by default and can be switched off with
 * {@code events.dispatcher.enabled=false} for tests and demos.
 */
class DispatcherToggleTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(Application.class);

    @Test
    void dispatcherIsActiveByDefault() {
        runner.run(context -> assertThat(context).hasSingleBean(EventDispatcher.class));
    }

    @Test
    void dispatcherIsSwitchedOffByConfiguration() {
        runner.withPropertyValues("events.dispatcher.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(EventDispatcher.class));
    }
}
