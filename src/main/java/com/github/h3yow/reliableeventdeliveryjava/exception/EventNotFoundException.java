package com.github.h3yow.reliableeventdeliveryjava.exception;

import com.github.h3yow.reliableeventdeliveryjava.domain.ConflictReason;

/** 查询/操作的事件不存在。 */
public class EventNotFoundException extends RuntimeException {
    private final String eventId;

    public EventNotFoundException(String eventId) {
        super("event not found: " + eventId);
        this.eventId = eventId;
    }

    public String getEventId() { return eventId; }
    public ConflictReason getReason() { return ConflictReason.EVENT_NOT_FOUND; }
}
