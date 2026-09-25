package com.github.h3yow.reliableeventdeliveryjava.event;

/**
 * Distinguishable causes for an event entering the DEAD terminal state.
 */
public enum DeadReason {
    /** Bounded retry policy exhausted. */
    RETRY_EXHAUSTED,
    /** Handler signalled the failure can never succeed. */
    NON_RETRYABLE
}
