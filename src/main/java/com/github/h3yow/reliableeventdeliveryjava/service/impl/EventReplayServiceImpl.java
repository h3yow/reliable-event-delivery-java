package com.github.h3yow.reliableeventdeliveryjava.service.impl;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.github.h3yow.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.h3yow.reliableeventdeliveryjava.domain.ConflictReason;
import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;
import com.github.h3yow.reliableeventdeliveryjava.exception.EventConflictException;
import com.github.h3yow.reliableeventdeliveryjava.exception.EventNotFoundException;
import com.github.h3yow.reliableeventdeliveryjava.repository.OutboxRepository;
import com.github.h3yow.reliableeventdeliveryjava.service.EventReplayService;

/**
 * 受控重放规则：
 * <ul>
 *   <li>只有终态（DELIVERED / FAILED_RETRY_EXHAUSTED / DEAD_LETTER）可重放，
 *       非终态返回 REPLAY_NOT_TERMINAL；</li>
 *   <li>DELIVERED 的重放必须显式 {@code force=true}，否则返回 REPLAY_ALREADY_DELIVERED；</li>
 *   <li>{@code reason} 必填，写入迁移轨迹以便审计；</li>
 *   <li>重放把 attempts 清零、状态重置为 PENDING，由正常调度链路再次投递；
 *       处理器必须按 eventId 幂等。</li>
 * </ul>
 */
@Service
public class EventReplayServiceImpl implements EventReplayService {

    private static final Logger log = LoggerFactory.getLogger(EventReplayServiceImpl.class);
    private static final String WORKER = "replay";

    private final OutboxRepository repository;
    private final DeliveryProperties properties;

    public EventReplayServiceImpl(OutboxRepository repository, DeliveryProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    @Override
    @Transactional
    public OutboxEvent replay(String eventId, boolean force, String reason) {
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("replay reason must not be blank");
        }
        OutboxEvent event = repository.findByEventId(eventId)
                .orElseThrow(() -> new EventNotFoundException(eventId));
        EventStatus status = event.getStatus();
        if (!status.isTerminal()) {
            throw new EventConflictException(ConflictReason.REPLAY_NOT_TERMINAL, eventId,
                    "event is not in a terminal state: " + status);
        }
        if (status == EventStatus.DELIVERED && !force) {
            throw new EventConflictException(ConflictReason.REPLAY_ALREADY_DELIVERED, eventId,
                    "event already delivered; replay requires force=true");
        }
        Instant now = Instant.now();
        int rows = repository.resetForReplay(eventId, WORKER, properties.getMaxAttempts(), now,
                "replay(force=" + force + "): " + reason);
        if (rows == 0) {
            // 并发：事件在检查后被其他操作改动
            throw new EventConflictException(ConflictReason.REPLAY_NOT_TERMINAL, eventId,
                    "event state changed concurrently; replay aborted");
        }
        OutboxEvent reloaded = repository.findByEventId(eventId).orElseThrow();
        log.info("event replay accepted eventId={} transition={}->PENDING force={} reason='{}'",
                eventId, status, force, reason);
        return reloaded;
    }
}
