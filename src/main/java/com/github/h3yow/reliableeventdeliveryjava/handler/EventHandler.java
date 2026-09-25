package com.github.h3yow.reliableeventdeliveryjava.handler;

/**
 * 业务事件处理器（执行真正的副作用）。
 *
 * <p><b>幂等边界：</b>投递保证是「至少一次」——崩溃恢复或受控重放都可能导致同一事件
 * 被再次调用，因此实现方必须按 {@code eventId} 自行保证副作用幂等。
 */
public interface EventHandler {

    /** 该处理器支持的事件类型。 */
    String supportsType();

    /**
     * 处理事件。
     *
     * @param eventId 事件标识（幂等键）
     * @param payload 事件负载
     * @throws NonRetryableException 不可重试失败 → 事件直接进入 DEAD_LETTER
     * @throws Exception             其他异常视为可重试失败
     */
    void handle(String eventId, String payload) throws Exception;
}
