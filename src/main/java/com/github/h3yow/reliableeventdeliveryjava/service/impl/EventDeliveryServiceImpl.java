package com.github.h3yow.reliableeventdeliveryjava.service.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import com.github.h3yow.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;
import com.github.h3yow.reliableeventdeliveryjava.domain.RetryPolicy;
import com.github.h3yow.reliableeventdeliveryjava.handler.EventHandler;
import com.github.h3yow.reliableeventdeliveryjava.handler.EventHandlerRegistry;
import com.github.h3yow.reliableeventdeliveryjava.handler.NonRetryableException;
import com.github.h3yow.reliableeventdeliveryjava.repository.OutboxRepository;
import com.github.h3yow.reliableeventdeliveryjava.service.EventDeliveryService;

/**
 * 单事件投递流程。
 *
 * <p><b>事务/崩溃边界：</b>{@code claimNext} 在独立短事务里用条件 UPDATE 完成认领；
 * 副作用在事务外执行（不得把外部副作用包进数据库事务）；结果再用一个带
 * 「eventId + worker + IN_FLIGHT」前置条件的短事务写回。
 * 副作用执行中进程崩溃 → 事件停留在 IN_FLIGHT，锁超时后被其他 worker 回收重试，
 * 不丢事件；因此处理器必须按 eventId 幂等。
 *
 * <p><b>并发：</b>认领的条件更新保证同一事件同一时刻只有一个 worker 持有，
 * 写回同样带归属条件，回收后旧 worker 的迟到写回会失败（影响 0 行），
 * 不会出现两个 worker 同时成功完成同一事件。
 */
@Service
public class EventDeliveryServiceImpl implements EventDeliveryService {

    private static final Logger log = LoggerFactory.getLogger(EventDeliveryServiceImpl.class);
    private static final String MDC_EVENT = "eventId";

    private final OutboxRepository repository;
    private final EventHandlerRegistry registry;
    private final RetryPolicy retryPolicy;
    private final DeliveryProperties properties;
    private final String nodeId = "node-" + UUID.randomUUID();

    public EventDeliveryServiceImpl(OutboxRepository repository, EventHandlerRegistry registry,
                                    RetryPolicy retryPolicy, DeliveryProperties properties) {
        this.repository = repository;
        this.registry = registry;
        this.retryPolicy = retryPolicy;
        this.properties = properties;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public boolean processOne() {
        // 每次认领用独立的执行标识：崩溃后另一线程/节点回收锁时 lockedBy 必然不同
        String executionId = nodeId + "-exec-" + UUID.randomUUID();
        Instant now = Instant.now();
        Instant staleBefore = now.minus(properties.getLockTimeout());
        Optional<OutboxEvent> claimed = repository.claimNext(executionId, now, staleBefore);
        if (claimed.isEmpty()) {
            return false;
        }
        OutboxEvent event = claimed.get();
        MDC.put(MDC_EVENT, event.getEventId());
        try {
            // 认领后状态已是 IN_FLIGHT；attempts==1 且此前无锁为首次认领，否则为退避重试/锁回收
            String fromState = event.getAttempts() <= 1 ? "PENDING" : "RECLAIMED";
            log.info("event claimed eventId={} transition={}->IN_FLIGHT attempt={} lockedBy={}",
                    event.getEventId(), fromState, event.getAttempts(), executionId);
            deliver(event, executionId);
        } finally {
            MDC.remove(MDC_EVENT);
        }
        return true;
    }

    private void deliver(OutboxEvent event, String workerId) {
        String eventId = event.getEventId();
        int attempt = event.getAttempts(); // 认领时已 +1
        Optional<EventHandler> handler = registry.find(event.getEventType());
        if (handler.isEmpty()) {
            String reason = "no handler registered for eventType=" + event.getEventType();
            repository.markDeadLetter(eventId, workerId, attempt, reason);
            log.warn("event dead-lettered eventId={} transition=IN_FLIGHT->DEAD_LETTER attempt={} reason={}",
                    eventId, attempt, reason);
            return;
        }
        try {
            handler.get().handle(eventId, event.getPayload());
        } catch (NonRetryableException e) {
            String reason = "non-retryable: " + e.getMessage();
            repository.markDeadLetter(eventId, workerId, attempt, reason);
            log.warn("event dead-lettered eventId={} transition=IN_FLIGHT->DEAD_LETTER attempt={} reason={}",
                    eventId, attempt, reason);
            return;
        } catch (Exception e) {
            handleRetryableFailure(event, attempt, e, workerId);
            return;
        }
        boolean done = repository.markDelivered(eventId, workerId);
        if (done) {
            log.info("event delivered eventId={} transition=IN_FLIGHT->DELIVERED attempt={}", eventId, attempt);
        } else {
            log.warn("delivered mark lost eventId={} attempt={} (ownership changed, result ignored)",
                    eventId, attempt);
        }
    }

    private void handleRetryableFailure(OutboxEvent event, int attempt, Exception e, String workerId) {
        String eventId = event.getEventId();
        String reason = e.getClass().getSimpleName()
                + (e.getMessage() == null ? "" : ": " + e.getMessage());
        if (attempt >= event.getMaxAttempts()) {
            repository.markRetryExhausted(eventId, workerId, attempt, reason);
            log.warn("event failed-retry-exhausted eventId={} transition=IN_FLIGHT->FAILED_RETRY_EXHAUSTED attempt={}/{} reason={}",
                    eventId, attempt, event.getMaxAttempts(), reason);
            return;
        }
        Duration backoff = retryPolicy.backoffAfter(attempt);
        Instant nextAttemptAt = Instant.now().plus(backoff);
        boolean marked = repository.markRetrying(eventId, workerId, attempt, nextAttemptAt, reason);
        if (marked) {
            log.warn("event retry scheduled eventId={} transition=IN_FLIGHT->RETRYING attempt={}/{} backoff={}ms nextAttemptAt={} reason={}",
                    eventId, attempt, event.getMaxAttempts(), backoff.toMillis(), nextAttemptAt, reason);
        } else {
            log.warn("retrying mark lost eventId={} attempt={} (ownership changed, result ignored)",
                    eventId, attempt);
        }
    }
}
