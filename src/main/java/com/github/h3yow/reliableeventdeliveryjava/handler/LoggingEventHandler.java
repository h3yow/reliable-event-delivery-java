package com.github.h3yow.reliableeventdeliveryjava.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** 演示用处理器：把副作用记到日志。测试中会替换为可控处理器。 */
@Component
public class LoggingEventHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggingEventHandler.class);
    public static final String TYPE = "demo.log";

    @Override public String supportsType() { return TYPE; }

    @Override
    public void handle(String eventId, String payload) {
        log.info("side-effect applied: eventId={}, payload={}", eventId, payload);
    }
}
