package com.github.h3yow.reliableeventdeliveryjava.tests;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;
import com.github.h3yow.reliableeventdeliveryjava.repository.OutboxRepository;
import com.github.h3yow.reliableeventdeliveryjava.service.EventCommandService;
import com.github.h3yow.reliableeventdeliveryjava.service.EventQueryService;
import com.github.h3yow.reliableeventdeliveryjava.service.EventReplayService;

/**
 * 集成测试基类：默认启用真实 poller（后台自动投递），
 * 并提供状态/副作用等待辅助方法与轨迹打印。
 * 每个测试方法重建上下文并使用独立内存库，彻底隔离后台线程与数据。
 */
@SpringBootTest
@ActiveProfiles("test")
@ContextConfiguration(initializers = IsolatedH2Initializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
abstract class IntegrationTestBase {

    @Autowired
    protected EventCommandService commands;
    @Autowired
    protected EventQueryService queries;
    @Autowired
    protected EventReplayService replays;
    @Autowired
    protected OutboxRepository repository;
    @Autowired
    protected RecordingHandler handler;

    @BeforeEach
    void resetHandler() {
        handler.reset();
    }

    protected OutboxEvent accept(String key, String payload) {
        return commands.accept(key, RecordingHandler.TYPE, payload);
    }

    protected OutboxEvent awaitStatus(String eventId, EventStatus status) {
        return awaitStatus(eventId, status, Duration.ofSeconds(10));
    }

    protected OutboxEvent awaitStatus(String eventId, EventStatus status, Duration timeout) {
        Awaitility.await().atMost(timeout).pollInterval(Duration.ofMillis(20)).until(
                () -> queries.get(eventId).getStatus(), Predicate.isEqual(status));
        return queries.get(eventId);
    }

    protected void printTrail(String eventId) {
        List<?> transitions = queries.transitions(eventId);
        System.out.println("---- transition trail eventId=" + eventId + " ----");
        transitions.forEach(t -> System.out.println("  " + t));
        System.out.println("-------------------------------------------");
    }
}
