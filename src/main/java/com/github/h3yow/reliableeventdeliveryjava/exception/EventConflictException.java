package com.github.h3yow.reliableeventdeliveryjava.exception;

import com.github.h3yow.reliableeventdeliveryjava.domain.ConflictReason;

/**
 * 受控操作（如重放）与事件当前状态冲突，被拒绝。
 */
public class EventConflictException extends RuntimeException {
    private final ConflictReason reason;
    private final String eventId;

    public EventConflictException(ConflictReason reason, String eventId, String message) {
        super(message);
        this.reason = reason;
        this.eventId = eventId;
    }

    public ConflictReason getReason() { return reason; }
    public String getEventId() { return eventId; }
}
