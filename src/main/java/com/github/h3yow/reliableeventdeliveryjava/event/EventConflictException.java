package com.github.h3yow.reliableeventdeliveryjava.event;

/**
 * Raised when a business action is resubmitted with the same idempotency key
 * but a different payload.
 */
public class EventConflictException extends RuntimeException {

    private final String idempotencyKey;

    public EventConflictException(String idempotencyKey, String message) {
        super(message);
        this.idempotencyKey = idempotencyKey;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }
}
