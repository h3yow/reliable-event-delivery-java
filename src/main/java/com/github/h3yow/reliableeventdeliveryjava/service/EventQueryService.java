package com.github.h3yow.reliableeventdeliveryjava.service;

import java.util.List;

import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.EventTransition;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;

/** 查询事件状态、终态列表与状态迁移轨迹。 */
public interface EventQueryService {

    OutboxEvent get(String eventId);

    List<OutboxEvent> listByStatus(EventStatus status);

    List<EventTransition> transitions(String eventId);
}
