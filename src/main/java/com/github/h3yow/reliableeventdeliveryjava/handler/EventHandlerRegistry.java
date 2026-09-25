package com.github.h3yow.reliableeventdeliveryjava.handler;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

/** 按事件类型查找处理器。无匹配处理器时以空结果表示（投递层将其转入死信）。 */
@Component
public class EventHandlerRegistry {

    private final Map<String, EventHandler> handlers;

    public EventHandlerRegistry(List<EventHandler> handlerList) {
        this.handlers = handlerList.stream()
                .collect(Collectors.toUnmodifiableMap(EventHandler::supportsType, Function.identity()));
    }

    public Optional<EventHandler> find(String eventType) {
        return Optional.ofNullable(handlers.get(eventType));
    }
}
