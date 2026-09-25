package com.github.h3yow.reliableeventdeliveryjava.service.impl;

import java.util.List;

import org.springframework.stereotype.Service;

import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.EventTransition;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;
import com.github.h3yow.reliableeventdeliveryjava.exception.EventNotFoundException;
import com.github.h3yow.reliableeventdeliveryjava.repository.OutboxRepository;
import com.github.h3yow.reliableeventdeliveryjava.service.EventQueryService;

/** 只读查询；终态（含失败原因与全部迁移轨迹）均可查询。 */
@Service
public class EventQueryServiceImpl implements EventQueryService {

    private final OutboxRepository repository;

    public EventQueryServiceImpl(OutboxRepository repository) {
        this.repository = repository;
    }

    @Override
    public OutboxEvent get(String eventId) {
        return repository.findByEventId(eventId)
                .orElseThrow(() -> new EventNotFoundException(eventId));
    }

    @Override
    public List<OutboxEvent> listByStatus(EventStatus status) {
        return repository.findByStatus(status);
    }

    @Override
    public List<EventTransition> transitions(String eventId) {
        get(eventId); // 不存在则抛出 EVENT_NOT_FOUND
        return repository.findTransitions(eventId);
    }
}
