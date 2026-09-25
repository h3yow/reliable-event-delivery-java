package com.github.h3yow.reliableeventdeliveryjava.event;

/**
 * Applies the side effect of an event. Implementations must be safe to invoke
 * more than once for the same event id; the processor additionally guards
 * against duplicate application.
 */
@FunctionalInterface
public interface EventHandler {

    /**
     * @throws NonRetryableException if the failure can never succeed
     * @throws RuntimeException      for transient, retryable failures
     */
    void handle(EventRecord event) throws Exception;
}
