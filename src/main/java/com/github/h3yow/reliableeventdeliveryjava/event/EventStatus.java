package com.github.h3yow.reliableeventdeliveryjava.event;

/**
 * Lifecycle states of a locally stored event.
 */
public enum EventStatus {
    /** Accepted, waiting to be claimed by a consumer. */
    PENDING,
    /** Claimed by a consumer, side effect in flight. */
    IN_PROGRESS,
    /** Side effect applied exactly once; terminal. */
    SUCCEEDED,
    /** Delivery gave up (retry exhausted or non-retryable); terminal, replayable. */
    DEAD
}
