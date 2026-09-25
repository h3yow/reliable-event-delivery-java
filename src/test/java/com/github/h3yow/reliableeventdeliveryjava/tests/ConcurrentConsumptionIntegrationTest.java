package com.github.h3yow.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;

/**
 * 场景：一批事件被多个 worker（配置 workerCount=4）并发、重复地消费。
 * 预期：全部成功；每个事件恰好一次副作用；总数严格相等。
 */
class ConcurrentConsumptionIntegrationTest extends IntegrationTestBase {

    @Test
    void manyEventsAreDeliveredExactlyOnceUnderConcurrency() throws Exception {
        int eventCount = 40;
        List<OutboxEvent> events = new ArrayList<>();

        // 并发提交
        ExecutorService submitPool = Executors.newFixedThreadPool(8);
        for (int i = 0; i < eventCount; i++) {
            int idx = i;
            submitPool.submit(() -> events.add(accept("concurrent-" + UUID.randomUUID() + "-" + idx, "ok")));
        }
        submitPool.shutdown();
        assertThat(submitPool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        // 等待全部 DELIVERED
        for (OutboxEvent e : events) {
            awaitStatus(e.getEventId(), EventStatus.DELIVERED);
        }

        // 每个事件恰好一次
        for (OutboxEvent e : events) {
            assertThat(handler.invocationCount(e.getEventId()))
                    .as("side effect must happen exactly once for %s", e.getEventId())
                    .isEqualTo(1);
        }
        assertThat(handler.totalInvocations()).isEqualTo(eventCount);
        assertThat(handler.deliveredOrder()).hasSize(eventCount);
        printTrail(events.get(0).getEventId());
    }
}
