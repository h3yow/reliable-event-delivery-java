package com.github.h3yow.reliableeventdeliveryjava.event;

/**
 * Thrown by an {@link EventHandler} to mark a failure as permanently
 * non-retryable; the event goes straight to DEAD.
 */
public class NonRetryableException extends RuntimeException {

    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
