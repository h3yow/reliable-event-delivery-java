package com.github.h3yow.reliableeventdeliveryjava.service.impl;

import java.time.Instant;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import com.github.h3yow.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.EventTransition;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;
import com.github.h3yow.reliableeventdeliveryjava.exception.DuplicateEventException;
import com.github.h3yow.reliableeventdeliveryjava.repository.OutboxRepository;
import com.github.h3yow.reliableeventdeliveryjava.service.EventCommandService;

/**
 * 接受业务动作：事件先落库（PENDING），随后由 poller 异步投递。
 *
 * <p>businessKey 上有唯一约束：并发重复提交时只有一个 INSERT 成功。
 * 冲突的 INSERT 在其事务内触发约束违例，因此插入+审计放在独立事务
 * {@link TransactionTemplate} 中执行；失败回滚后再在新事务里回读已存在事件，
 * 转换为带可区分原因（DUPLICATE_BUSINESS_KEY）的拒绝。
 */
@Service
public class EventCommandServiceImpl implements EventCommandService {

    private static final Logger log = LoggerFactory.getLogger(EventCommandServiceImpl.class);
    private static final String WORKER = "accept";

    private final OutboxRepository repository;
    private final DeliveryProperties properties;
    private final TransactionTemplate txTemplate;

    public EventCommandServiceImpl(OutboxRepository repository, DeliveryProperties properties,
                                   TransactionTemplate txTemplate) {
        this.repository = repository;
        this.properties = properties;
        this.txTemplate = txTemplate;
    }

    @Override
    public OutboxEvent accept(String businessKey, String eventType, String payload) {
        if (businessKey == null || businessKey.isBlank()) {
            throw new IllegalArgumentException("businessKey must not be blank");
        }
        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException("eventType must not be blank");
        }
        Instant now = Instant.now();
        OutboxEvent event = newEvent(businessKey, eventType, payload, now);
        try {
            txTemplate.executeWithoutResult(status -> {
                repository.insert(event);
                repository.recordTransition(acceptTransition(event, now));
            });
        } catch (DuplicateKeyException dup) {
            String existingId = repository.findByBusinessKey(businessKey)
                    .map(OutboxEvent::getEventId).orElse("(unknown)");
            log.warn("duplicate submission rejected eventId={} businessKey={} existingEventId={} reason=DUPLICATE_BUSINESS_KEY",
                    event.getEventId(), businessKey, existingId);
            throw new DuplicateEventException(businessKey, existingId);
        }
        log.info("event accepted eventId={} businessKey={} type={} transition=null->PENDING",
                event.getEventId(), businessKey, eventType);
        return event;
    }

    private OutboxEvent newEvent(String businessKey, String eventType, String payload, Instant now) {
        OutboxEvent event = new OutboxEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setBusinessKey(businessKey);
        event.setEventType(eventType);
        event.setPayload(payload);
        event.setStatus(EventStatus.PENDING);
        event.setAttempts(0);
        event.setMaxAttempts(properties.getMaxAttempts());
        event.setNextAttemptAt(now);
        event.setCreatedAt(now);
        event.setUpdatedAt(now);
        return event;
    }

    private EventTransition acceptTransition(OutboxEvent event, Instant now) {
        EventTransition transition = new EventTransition();
        transition.setEventId(event.getEventId());
        transition.setFromStatus(null);
        transition.setToStatus(EventStatus.PENDING);
        transition.setAttempt(0);
        transition.setReason("accepted: businessKey=" + event.getBusinessKey()
                + ", type=" + event.getEventType());
        transition.setWorker(WORKER);
        transition.setOccurredAt(now);
        return transition;
    }
}
