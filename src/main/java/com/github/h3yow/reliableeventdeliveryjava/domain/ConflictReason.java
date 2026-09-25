package com.github.h3yow.reliableeventdeliveryjava.domain;

/**
 * 业务动作/重放请求被拒绝时可区分的原因。
 */
public enum ConflictReason {
    /** 同一 businessKey 的业务动作已经接受过（唯一约束冲突）。 */
    DUPLICATE_BUSINESS_KEY,
    /** 目标事件不是终态，不允许重放。 */
    REPLAY_NOT_TERMINAL,
    /** 目标事件已成功投递；非强制（force）的重放会被拒绝。 */
    REPLAY_ALREADY_DELIVERED,
    /** 目标事件不存在。 */
    EVENT_NOT_FOUND
}
