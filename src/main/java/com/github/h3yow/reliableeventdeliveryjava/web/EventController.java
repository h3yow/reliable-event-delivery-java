package com.github.h3yow.reliableeventdeliveryjava.web;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;
import com.github.h3yow.reliableeventdeliveryjava.service.EventCommandService;
import com.github.h3yow.reliableeventdeliveryjava.service.EventQueryService;
import com.github.h3yow.reliableeventdeliveryjava.service.EventReplayService;
import com.github.h3yow.reliableeventdeliveryjava.web.dto.AcceptEventRequest;
import com.github.h3yow.reliableeventdeliveryjava.web.dto.ReplayRequest;

/** 事件接受 / 查询 / 重放接口。 */
@RestController
@RequestMapping("/api/events")
public class EventController {

    private final EventCommandService commands;
    private final EventQueryService queries;
    private final EventReplayService replays;

    public EventController(EventCommandService commands, EventQueryService queries,
                           EventReplayService replays) {
        this.commands = commands;
        this.queries = queries;
        this.replays = replays;
    }

    /** 接受业务动作；重复 businessKey → 409 + reason=DUPLICATE_BUSINESS_KEY。 */
    @PostMapping
    public ResponseEntity<OutboxEvent> accept(@RequestBody AcceptEventRequest request,
                                              UriComponentsBuilder uriBuilder) {
        OutboxEvent event = commands.accept(request.getBusinessKey(),
                request.getEventType(), request.getPayload());
        URI location = uriBuilder.path("/api/events/{id}").build(event.getEventId());
        return ResponseEntity.created(location).body(event);
    }

    @GetMapping("/{eventId}")
    public OutboxEvent get(@PathVariable String eventId) {
        return queries.get(eventId);
    }

    /** 按状态列出事件（查询终态用 FAILED_RETRY_EXHAUSTED / DEAD_LETTER / DELIVERED）。 */
    @GetMapping
    public List<OutboxEvent> list(@RequestParam(defaultValue = "PENDING") EventStatus status) {
        return queries.listByStatus(status);
    }

    @GetMapping("/{eventId}/transitions")
    public Map<String, Object> transitions(@PathVariable String eventId) {
        return Map.of("eventId", eventId, "transitions", queries.transitions(eventId));
    }

    /** 受控重放终态事件。 */
    @PostMapping("/{eventId}/replay")
    public OutboxEvent replay(@PathVariable String eventId, @RequestBody ReplayRequest request) {
        return replays.replay(eventId, request.isForce(), request.getReason());
    }
}
