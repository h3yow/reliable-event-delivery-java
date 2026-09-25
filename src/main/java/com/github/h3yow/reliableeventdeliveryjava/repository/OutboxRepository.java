package com.github.h3yow.reliableeventdeliveryjava.repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.EventTransition;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;

/** 事件与状态迁移记录的数据访问。 */
public interface OutboxRepository {

    /** 插入新事件。businessKey 唯一冲突由底层抛 DuplicateKeyException。 */
    void insert(OutboxEvent event);

    Optional<OutboxEvent> findByEventId(String eventId);

    Optional<OutboxEvent> findByBusinessKey(String businessKey);

    List<OutboxEvent> findByStatus(EventStatus status);

    /**
     * 原子认领一条可投递事件：
     * PENDING 且到时，或 RETRYING 且退避结束，或 IN_FLIGHT 锁超时被回收。
     * 条件更新保证并发 worker 中只有一个能成功；返回空表示没有可处理事件。
     */
    Optional<OutboxEvent> claimNext(String workerId, Instant now, Instant staleBefore);

    /** 条件标记成功：仅当事件仍处于该 worker 的 IN_FLIGHT 状态时生效。 */
    boolean markDelivered(String eventId, String workerId);

    /** 条件标记为可重试等待（RETRYING），记录退避时间与失败原因。 */
    boolean markRetrying(String eventId, String workerId, int attempts, Instant nextAttemptAt, String reason);

    /** 条件标记为重试耗尽终态。 */
    boolean markRetryExhausted(String eventId, String workerId, int attempts, String reason);

    /** 条件标记为不可重试死信终态。 */
    boolean markDeadLetter(String eventId, String workerId, int attempts, String reason);

    /**
     * 受控重放：仅终态事件可被重置回 PENDING（同事务写迁移记录）。
     * @return 受影响行数（0 表示状态不满足，如事件已被并发改回非终态）
     */
    int resetForReplay(String eventId, String worker, int maxAttempts, Instant now, String reason);

    /** 在同一事务内追加一条状态迁移审计。 */
    void recordTransition(EventTransition transition);

    List<EventTransition> findTransitions(String eventId);
}
