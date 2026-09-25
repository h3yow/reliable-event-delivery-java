package com.github.h3yow.reliableeventdeliveryjava.web;

import java.time.Instant;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.github.h3yow.reliableeventdeliveryjava.exception.DuplicateEventException;
import com.github.h3yow.reliableeventdeliveryjava.exception.EventConflictException;
import com.github.h3yow.reliableeventdeliveryjava.exception.EventNotFoundException;

/** 把领域异常映射为带可区分 {@code reason} 码的 HTTP 响应。 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(DuplicateEventException.class)
    public ResponseEntity<Map<String, Object>> duplicate(DuplicateEventException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                "status", 409,
                "reason", e.getReason().name(),
                "message", e.getMessage(),
                "businessKey", e.getBusinessKey(),
                "existingEventId", e.getExistingEventId(),
                "timestamp", Instant.now().toString()));
    }

    @ExceptionHandler(EventConflictException.class)
    public ResponseEntity<Map<String, Object>> conflict(EventConflictException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                "status", 409,
                "reason", e.getReason().name(),
                "eventId", e.getEventId(),
                "message", e.getMessage(),
                "timestamp", Instant.now().toString()));
    }

    @ExceptionHandler(EventNotFoundException.class)
    public ResponseEntity<Map<String, Object>> notFound(EventNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                "status", 404,
                "reason", e.getReason().name(),
                "eventId", e.getEventId(),
                "message", e.getMessage(),
                "timestamp", Instant.now().toString()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of(
                "status", 400,
                "reason", "VALIDATION_ERROR",
                "message", e.getMessage(),
                "timestamp", Instant.now().toString()));
    }
}
