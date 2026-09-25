package com.github.h3yow.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import com.github.h3yow.reliableeventdeliveryjava.domain.ConflictReason;
import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;
import com.github.h3yow.reliableeventdeliveryjava.exception.EventConflictException;

/**
 * 场景：
 * 1. 不可重试失败立即进入 DEAD_LETTER（只调用一次，不重试）；
 * 2. 重放非终态被拒绝（REPLAY_NOT_TERMINAL）；
 * 3. 重放已成功事件默认被拒绝（REPLAY_ALREADY_DELIVERED），force=true 后可重放；
 * 4. 终态重放后事件被重新投递。
 */
class DeadLetterAndReplayIntegrationTest extends IntegrationTestBase {

    @Test
    void nonRetryableFailureGoesStraightToDeadLetterAndCanBeReplayed() {
        OutboxEvent event = accept("dl-" + UUID.randomUUID(), "FAIL_NON_RETRYABLE");
        OutboxEvent dead = awaitStatus(event.getEventId(), EventStatus.DEAD_LETTER);

        assertThat(dead.getAttempts()).isEqualTo(1);
        assertThat(dead.getFailureReason()).startsWith("non-retryable:");
        assertThat(handler.invocationCount(event.getEventId())).isEqualTo(1);

        // 重放必须给原因
        assertThatThrownBy(() -> replays.replay(event.getEventId(), false, "  "))
                .isInstanceOf(IllegalArgumentException.class);

        // 受控重放：死信事件回到 PENDING 并被再次投递（处理器仍失败，故再次进入死信）
        OutboxEvent reset = replays.replay(event.getEventId(), false, "investigated and retry ops fix");
        assertThat(reset.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(reset.getAttempts()).isZero();

        OutboxEvent deadAgain = awaitStatus(event.getEventId(), EventStatus.DEAD_LETTER);
        assertThat(deadAgain.getAttempts()).isEqualTo(1);
        assertThat(handler.invocationCount(event.getEventId())).isEqualTo(2); // 重放后再次执行
        printTrail(event.getEventId());
    }

    @Test
    void replayOfNonTerminalEventIsRejectedWithReason() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        handler.blockUntil(release);
        OutboxEvent event = accept("inflight-" + UUID.randomUUID(), "BLOCK");

        // 等待副作用开始：事件稳定处于 IN_FLIGHT（非终态）
        Awaitility.await().untilAsserted(() ->
                assertThat(handler.invocationCount(event.getEventId())).isEqualTo(1));
        assertThat(queries.get(event.getEventId()).getStatus()).isEqualTo(EventStatus.IN_FLIGHT);

        try {
            assertThatThrownBy(() -> replays.replay(event.getEventId(), false, "replay while active"))
                    .isInstanceOf(EventConflictException.class)
                    .extracting(e -> ((EventConflictException) e).getReason())
                    .isEqualTo(ConflictReason.REPLAY_NOT_TERMINAL);
        } finally {
            release.countDown(); // 放行，让事件正常完成，避免拖住 worker
        }
        awaitStatus(event.getEventId(), EventStatus.DELIVERED);
    }

    @Test
    void replayOfDeliveredEventRequiresForceButDeliversAgainWhenForced() {
        OutboxEvent event = accept("delivered-" + UUID.randomUUID(), "ok");
        awaitStatus(event.getEventId(), EventStatus.DELIVERED);
        assertThat(handler.invocationCount(event.getEventId())).isEqualTo(1);

        assertThatThrownBy(() -> replays.replay(event.getEventId(), false, "please redeliver"))
                .isInstanceOf(EventConflictException.class)
                .extracting(e -> ((EventConflictException) e).getReason())
                .isEqualTo(ConflictReason.REPLAY_ALREADY_DELIVERED);
        assertThat(handler.invocationCount(event.getEventId())).isEqualTo(1);

        OutboxEvent reset = replays.replay(event.getEventId(), true, "user requested forced replay");
        assertThat(reset.getStatus()).isEqualTo(EventStatus.PENDING);
        awaitStatus(event.getEventId(), EventStatus.DELIVERED);
        assertThat(handler.invocationCount(event.getEventId())).isEqualTo(2);
        printTrail(event.getEventId());
    }
}
