package com.github.h3yow.reliableeventdeliveryjava.service;

import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;

/** 接受业务动作、产生本地事件。 */
public interface EventCommandService {

    /**
     * 接受一个业务动作并持久化事件（与业务数据在同一数据库本地事务语义下提交）。
     * 重复 businessKey 将被拒绝。
     */
    OutboxEvent accept(String businessKey, String eventType, String payload);
}
