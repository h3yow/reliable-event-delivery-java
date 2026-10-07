package com.github.h3yow.reliableeventdeliveryjava.event;

/**
 * Raised when the pending backlog reached the configured capacity and new
 * submissions are temporarily rejected (backpressure). Distinct from
 * {@link EventConflictException}: this is a transient, retryable overload
 * signal, not a business conflict, and not an internal fault.
 */
public class EventOverloadException extends RuntimeException {

    private final int maxPending;

    public EventOverloadException(int maxPending, String message) {
        super(message);
        this.maxPending = maxPending;
    }

    public int getMaxPending() {
        return maxPending;
    }
}
