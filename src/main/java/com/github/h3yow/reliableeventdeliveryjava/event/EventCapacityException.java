package com.github.h3yow.reliableeventdeliveryjava.event;

/**
 * Rejected because the delivery backlog reached its configured capacity.
 * Distinct from {@link EventConflictException} (a business-level duplicate
 * conflict): this is a transient backpressure signal (HTTP 429) and the
 * caller may retry once the backlog drains.
 */
public class EventCapacityException extends RuntimeException {

    public EventCapacityException(long maxPending) {
        super("delivery backlog full (maxPending=" + maxPending
                + "); retry after the backlog drains");
    }
}
