package com.github.h3yow.reliableeventdeliveryjava.api;

import java.util.List;
import java.util.Map;

import com.github.h3yow.reliableeventdeliveryjava.event.EventConflictException;
import com.github.h3yow.reliableeventdeliveryjava.event.EventOverloadException;
import com.github.h3yow.reliableeventdeliveryjava.event.EventRecord;
import com.github.h3yow.reliableeventdeliveryjava.event.EventStats;
import com.github.h3yow.reliableeventdeliveryjava.event.EventService;
import com.github.h3yow.reliableeventdeliveryjava.event.EventStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST facade for submitting, querying and replaying events.
 */
@RestController
@RequestMapping("/api/events")
public class EventController {

    private final EventService service;
    private final EventStore store;

    public EventController(EventService service, EventStore store) {
        this.service = service;
        this.store = store;
    }

    public record SubmitRequest(String idempotencyKey, String payload) {}

    public record ErrorResponse(String reason, String message) {}

    /** Idempotent accept; 200 with the (possibly pre-existing) record. */
    @PostMapping
    public EventRecord submit(@RequestBody SubmitRequest request) {
        return service.submit(request.idempotencyKey(), request.payload());
    }

    @GetMapping
    public List<EventRecord> list() {
        return store.findAll();
    }

    @GetMapping("/{id}")
    public EventRecord get(@PathVariable String id) {
        return service.get(id);
    }

    /** Controlled replay of a DEAD event. */
    @PostMapping("/{id}/replay")
    public EventRecord replay(@PathVariable String id) {
        return service.replay(id);
    }

    /** Operational summary: per-status counts, backlog, oldest wait, retry spread. */
    @GetMapping("/stats")
    public EventStats stats() {
        return service.stats();
    }

    @ExceptionHandler(EventOverloadException.class)
    public ResponseEntity<ErrorResponse> overloaded(EventOverloadException e) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body(new ErrorResponse("OVERLOADED", e.getMessage()));
    }

    @ExceptionHandler(EventConflictException.class)
    public ResponseEntity<ErrorResponse> conflict(EventConflictException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ErrorResponse("DUPLICATE_CONFLICT", e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> notFound(IllegalArgumentException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorResponse("NOT_FOUND", e.getMessage()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ErrorResponse> illegalState(IllegalStateException e) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(new ErrorResponse("ILLEGAL_STATE", e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> fallback(Exception e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse("INTERNAL", String.valueOf(e.getMessage())));
    }
}
