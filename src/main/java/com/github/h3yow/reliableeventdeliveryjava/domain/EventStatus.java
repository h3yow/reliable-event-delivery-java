package com.github.h3yow.reliableeventdeliveryjava.domain;

/**
 * 事件生命周期状态。
 *
 * <ul>
 *   <li>{@link #PENDING}   — 已接受，等待投递（非终态）</li>
 *   <li>{@link #IN_FLIGHT}— 已被某个 worker 认领，正在投递（非终态；worker 崩溃后可回收）</li>
 *   <li>{@link #DELIVERED}— 投递成功（终态）</li>
 *   <li>{@link #RETRYING} — 本次失败可重试，已退避等待下一轮（非终态）</li>
 *   <li>{@link #FAILED_RETRY_EXHAUSTED} — 可重试失败但已达到次数上限（终态）</li>
 *   <li>{@link #DEAD_LETTER} — 不可重试的失败（终态）</li>
 * </ul>
 */
public enum EventStatus {
    PENDING,
    IN_FLIGHT,
    DELIVERED,
    RETRYING,
    FAILED_RETRY_EXHAUSTED,
    DEAD_LETTER;

    /** 终态：没有人工/受控重放不会再自动迁移。 */
    public boolean isTerminal() {
        return this == DELIVERED || this == FAILED_RETRY_EXHAUSTED || this == DEAD_LETTER;
    }
}
