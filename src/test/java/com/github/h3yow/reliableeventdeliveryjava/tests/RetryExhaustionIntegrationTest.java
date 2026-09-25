package com.github.h3yow.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.EventTransition;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;

/**
 * 场景：处理持续失败。
 * 预期：尝试次数有上限（maxAttempts=3）→ FAILED_RETRY_EXHAUSTED；
 * 失败原因落库可查；事件之间互不阻塞。
 */
class RetryExhaustionIntegrationTest extends IntegrationTestBase {

    @Test
    void retriesUpToMaxAttemptsThenTerminatesWithoutBlockingOthers() {
        String failingKey = "order-fail-" + UUID.randomUUID();
        String healthyKey = "order-ok-" + UUID.randomUUID();

        OutboxEvent failing = accept(failingKey, "FAIL_ALWAYS");
        OutboxEvent healthy = accept(healthyKey, "ok");

        OutboxEvent terminal = awaitStatus(failing.getEventId(), EventStatus.FAILED_RETRY_EXHAUSTED);
        awaitStatus(healthy.getEventId(), EventStatus.DELIVERED);

        // 恰好尝试 maxAttempts 次
        assertThat(terminal.getAttempts()).isEqualTo(terminal.getMaxAttempts()).isEqualTo(3);
        assertThat(terminal.getFailureReason()).contains("simulated transient failure");
        assertThat(handler.invocationCount(failing.getEventId())).isEqualTo(3);

        // 健康事件未被失败事件阻塞，且只投递一次
        assertThat(handler.invocationCount(healthy.getEventId())).isEqualTo(1);

        // 终态可查询
        List<OutboxEvent> exhausted = queries.listByStatus(EventStatus.FAILED_RETRY_EXHAUSTED);
        assertThat(exhausted).extracting(OutboxEvent::getEventId).contains(failing.getEventId());

        // 迁移轨迹：PENDING → IN_FLIGHT → RETRYING → IN_FLIGHT → RETRYING → IN_FLIGHT → FAILED_RETRY_EXHAUSTED
        List<EventTransition> trail = queries.transitions(failing.getEventId());
        assertThat(trail).extracting(EventTransition::getToStatus)
                .containsExactly(EventStatus.PENDING,
                        EventStatus.IN_FLIGHT, EventStatus.RETRYING,
                        EventStatus.IN_FLIGHT, EventStatus.RETRYING,
                        EventStatus.IN_FLIGHT, EventStatus.FAILED_RETRY_EXHAUSTED);
        printTrail(failing.getEventId());
    }
}
