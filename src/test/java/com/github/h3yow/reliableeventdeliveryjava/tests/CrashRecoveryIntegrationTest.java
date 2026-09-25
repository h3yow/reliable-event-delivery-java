package com.github.h3yow.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.EventTransition;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;

/**
 * 场景：worker 在副作用执行中崩溃——事件停留在 IN_FLIGHT。
 * 预期：超过 lockTimeout 后被其他 worker 当作 stale 锁回收并重新投递，事件不丢失；
 * 轨迹中出现 IN_FLIGHT -> IN_FLIGHT（recovered）记录；旧 worker 迟到的完成写回落空。
 */
class CrashRecoveryIntegrationTest extends IntegrationTestBase {

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    void staleInFlightEventIsRecoveredAfterLockTimeout() {
        CountDownLatch release = new CountDownLatch(1);
        handler.blockUntil(release);
        OutboxEvent event = accept("crash-" + UUID.randomUUID(), "BLOCK_FIRST_ONLY");

        // 等待真实 worker 认领并卡在副作用内（事件处于 IN_FLIGHT）
        Awaitility.await().untilAsserted(() ->
                assertThat(handler.invocationCount(event.getEventId())).isEqualTo(1));
        OutboxEvent stuck = queries.get(event.getEventId());
        assertThat(stuck.getStatus()).isEqualTo(EventStatus.IN_FLIGHT);
        String originalWorker = stuck.getLockedBy();

        // 模拟该 worker 崩溃：把锁回拨到超时之前的时刻（副作用线程仍在 await 上悬挂）
        Instant oldTime = Instant.now().minus(10, ChronoUnit.SECONDS);
        jdbc.update("UPDATE outbox_event SET locked_at = ? WHERE event_id = ?",
                Timestamp.from(oldTime), event.getEventId());

        // 等待新 worker 回收锁并完成投递
        OutboxEvent redelivered = awaitStatus(event.getEventId(), EventStatus.DELIVERED,
                java.time.Duration.ofSeconds(15));
        assertThat(redelivered.getAttempts()).isEqualTo(2);

        // 释放“崩溃前悬挂”的副作用线程；它返回后的 markDelivered 必须因归属变更而落空
        release.countDown();
        Awaitility.await().pollDelay(java.time.Duration.ofMillis(300))
                .untilAsserted(() ->
                        assertThat(queries.get(event.getEventId()).getStatus())
                                .isEqualTo(EventStatus.DELIVERED));

        // 副作用被调用两次（崩溃一次 + 恢复一次）——至少一次不丢；处理器需按 eventId 幂等
        assertThat(handler.invocationCount(event.getEventId())).isEqualTo(2);

        var trail = queries.transitions(event.getEventId());
        assertThat(trail).extracting(EventTransition::getToStatus)
                .containsSubsequence(EventStatus.IN_FLIGHT, EventStatus.IN_FLIGHT, EventStatus.DELIVERED);
        EventTransition recovery = trail.stream()
                .filter(t -> t.getFromStatus() == EventStatus.IN_FLIGHT
                        && t.getToStatus() == EventStatus.IN_FLIGHT)
                .findFirst().orElseThrow();
        assertThat(recovery.getReason()).contains("stale lock recovered");
        assertThat(recovery.getWorker()).isNotEqualTo(originalWorker);
        printTrail(event.getEventId());
    }
}
