package com.github.h3yow.reliableeventdeliveryjava.service;

import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;

/** 终态事件的受控重放。 */
public interface EventReplayService {

    /**
     * @param force true 时允许重放已成功投递（DELIVERED）的事件
     * @param reason 重放原因（审计必填）
     */
    OutboxEvent replay(String eventId, boolean force, String reason);
}
