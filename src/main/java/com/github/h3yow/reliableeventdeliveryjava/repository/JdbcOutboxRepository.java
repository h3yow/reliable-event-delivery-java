package com.github.h3yow.reliableeventdeliveryjava.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.github.h3yow.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.EventTransition;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;

/**
 * 基于 {@link JdbcTemplate} 的实现。
 *
 * <p>并发安全的核心：认领与所有终态/退避标记都是「带前置状态条件的 UPDATE」，
 * 以受影响行数判断是否抢到；多个 worker 并发时数据库行锁天然串行化，
 * 同一条事件只会被一个 worker 成功认领，因此不会产生重复副作用。
 */
@Repository
public class JdbcOutboxRepository implements OutboxRepository {

    private final JdbcTemplate jdbc;
    private final int claimBatchSize;

    public JdbcOutboxRepository(JdbcTemplate jdbc, DeliveryProperties properties) {
        this.jdbc = jdbc;
        this.claimBatchSize = Math.max(1, properties.getClaimBatchSize());
    }

    private static final EventRowMapper EVENT_MAPPER = new EventRowMapper();

    @Override
    public void insert(OutboxEvent e) {
        jdbc.update("""
                INSERT INTO outbox_event
                  (event_id, business_key, event_type, payload, status, attempts, max_attempts,
                   failure_reason, locked_by, locked_at, next_attempt_at, created_at, updated_at, version)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,0)
                """,
                e.getEventId(), e.getBusinessKey(), e.getEventType(), e.getPayload(),
                e.getStatus().name(), e.getAttempts(), e.getMaxAttempts(),
                e.getFailureReason(), e.getLockedBy(), ts(e.getLockedAt()),
                ts(e.getNextAttemptAt()), ts(e.getCreatedAt()), ts(e.getUpdatedAt()));
    }

    @Override
    public Optional<OutboxEvent> findByEventId(String eventId) {
        return jdbc.query("SELECT * FROM outbox_event WHERE event_id = ?", EVENT_MAPPER, eventId)
                .stream().findFirst();
    }

    @Override
    public Optional<OutboxEvent> findByBusinessKey(String businessKey) {
        return jdbc.query("SELECT * FROM outbox_event WHERE business_key = ?", EVENT_MAPPER, businessKey)
                .stream().findFirst();
    }

    @Override
    public List<OutboxEvent> findByStatus(EventStatus status) {
        return jdbc.query("SELECT * FROM outbox_event WHERE status = ? ORDER BY created_at",
                EVENT_MAPPER, status.name());
    }

    /**
     * 候选行用 {@code FOR UPDATE} 锁定后，再逐条尝试条件更新；只认领第一条更新成功的。
     * 事务边界由本方法持有，保证「锁定 + 条件更新 + 回读」原子。
     */
    @Override
    @Transactional
    public Optional<OutboxEvent> claimNext(String workerId, Instant now, Instant staleBefore) {
        java.util.List<ClaimCandidate> candidates = jdbc.query("""
                SELECT event_id, status FROM outbox_event
                WHERE (status IN ('PENDING','RETRYING')
                         AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
                   OR (status = 'IN_FLIGHT' AND locked_at <= ?)
                ORDER BY next_attempt_at NULLS FIRST, created_at
                FETCH FIRST %d ROWS ONLY
                FOR UPDATE
                """.formatted(claimBatchSize),
                (rs, n) -> new ClaimCandidate(rs.getString(1), EventStatus.valueOf(rs.getString(2))),
                Timestamp.from(now), Timestamp.from(staleBefore));

        // 高并发下本事务开头锁定的候选可能已被他人抢先认领（更新 0 行）。
        // 跳过这些 id，在同一候选窗口内继续尝试靠后的行，避免全员活锁。
        for (ClaimCandidate candidate : candidates) {
            String id = candidate.eventId();
            int updated = jdbc.update("""
                    UPDATE outbox_event
                    SET status = 'IN_FLIGHT', attempts = attempts + 1,
                        locked_by = ?, locked_at = ?, next_attempt_at = NULL,
                        failure_reason = NULL, updated_at = ?, version = version + 1
                    WHERE event_id = ?
                      AND ( (status IN ('PENDING','RETRYING')
                               AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
                            OR (status = 'IN_FLIGHT' AND locked_at <= ?) )
                    """,
                    workerId, Timestamp.from(now), Timestamp.from(now), id,
                    Timestamp.from(now), Timestamp.from(staleBefore));
            if (updated == 1) {
                OutboxEvent claimed = findByEventId(id).orElseThrow();
                String reason = candidate.previousStatus() == EventStatus.IN_FLIGHT
                        ? "stale lock recovered after worker crash/timeout"
                        : "claimed for delivery";
                recordTransition(new TransitionBuilder(id)
                        .from(candidate.previousStatus()).to(EventStatus.IN_FLIGHT)
                        .attempt(claimed.getAttempts()).worker(workerId).reason(reason).build());
                return Optional.of(claimed);
            }
            // updated == 0：该候选被并发者抢先认领/回收，尝试窗口内下一个候选
        }
        return Optional.empty();
    }

    private record ClaimCandidate(String eventId, EventStatus previousStatus) {
    }

    @Override
    @Transactional
    public boolean markDelivered(String eventId, String workerId) {
        return conditionalMark(eventId, workerId, EventStatus.DELIVERED, null, null,
                "delivered");
    }

    @Override
    @Transactional
    public boolean markRetrying(String eventId, String workerId, int attempts,
                                Instant nextAttemptAt, String reason) {
        return conditionalMark(eventId, workerId, EventStatus.RETRYING, attempts,
                nextAttemptAt, reason);
    }

    @Override
    @Transactional
    public boolean markRetryExhausted(String eventId, String workerId, int attempts, String reason) {
        return conditionalMark(eventId, workerId, EventStatus.FAILED_RETRY_EXHAUSTED, attempts,
                null, reason);
    }

    @Override
    @Transactional
    public boolean markDeadLetter(String eventId, String workerId, int attempts, String reason) {
        return conditionalMark(eventId, workerId, EventStatus.DEAD_LETTER, attempts, null, reason);
    }

    private boolean conditionalMark(String eventId, String workerId, EventStatus target,
                                    Integer attempts, Instant nextAttemptAt, String reason) {
        List<Integer> currentAttemptsRows = jdbc.queryForList(
                "SELECT attempts FROM outbox_event WHERE event_id = ? AND status = 'IN_FLIGHT' AND locked_by = ?",
                Integer.class, eventId, workerId);
        if (currentAttemptsRows.isEmpty()) {
            return false; // 已不归属本 worker（被回收/重放），放弃写入
        }
        int currentAttempts = currentAttemptsRows.get(0);
        int updated = jdbc.update("""
                UPDATE outbox_event
                SET status = ?, attempts = COALESCE(?, attempts),
                    next_attempt_at = ?, failure_reason = ?,
                    locked_by = NULL, locked_at = NULL, updated_at = ?, version = version + 1
                WHERE event_id = ? AND status = 'IN_FLIGHT' AND locked_by = ?
                """,
                target.name(), attempts, ts(nextAttemptAt), truncate(reason),
                Timestamp.from(Instant.now()), eventId, workerId);
        if (updated == 1) {
            recordTransition(new TransitionBuilder(eventId)
                    .from(EventStatus.IN_FLIGHT).to(target)
                    .attempt(currentAttempts).worker(workerId).reason(reason).build());
        }
        return updated == 1;
    }

    @Override
    @Transactional
    public int resetForReplay(String eventId, String worker, int maxAttempts, Instant now, String reason) {
        OutboxEvent existing = findByEventId(eventId).orElse(null);
        if (existing == null || !existing.getStatus().isTerminal()) {
            return 0;
        }
        EventStatus from = existing.getStatus();
        int updated = jdbc.update("""
                UPDATE outbox_event
                SET status = 'PENDING', attempts = 0, max_attempts = ?,
                    failure_reason = NULL, locked_by = NULL, locked_at = NULL,
                    next_attempt_at = ?, updated_at = ?, version = version + 1
                WHERE event_id = ? AND status IN ('DELIVERED','FAILED_RETRY_EXHAUSTED','DEAD_LETTER')
                """,
                maxAttempts, Timestamp.from(now), Timestamp.from(now), eventId);
        if (updated == 1) {
            recordTransition(new TransitionBuilder(eventId)
                    .from(from).to(EventStatus.PENDING)
                    .attempt(0).worker(worker).reason(reason).build());
        }
        return updated;
    }

    @Override
    public void recordTransition(EventTransition t) {
        jdbc.update("""
                INSERT INTO event_transition
                  (event_id, from_status, to_status, attempt, reason, worker, occurred_at)
                VALUES (?,?,?,?,?,?,?)
                """,
                t.getEventId(),
                t.getFromStatus() == null ? null : t.getFromStatus().name(),
                t.getToStatus().name(), t.getAttempt(), truncate(t.getReason()),
                t.getWorker(), Timestamp.from(t.getOccurredAt()));
    }

    @Override
    public List<EventTransition> findTransitions(String eventId) {
        return jdbc.query("SELECT * FROM event_transition WHERE event_id = ? ORDER BY id",
                new TransitionRowMapper(), eventId);
    }

    private static Timestamp ts(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= 1900 ? s : s.substring(0, 1900) + "...(truncated)";
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }

    /** 插入迁移记录的小构建器。 */
    private static final class TransitionBuilder {
        private final EventTransition t = new EventTransition();

        TransitionBuilder(String eventId) {
            t.setEventId(eventId);
            t.setOccurredAt(Instant.now());
        }

        TransitionBuilder from(EventStatus s) { t.setFromStatus(s); return this; }
        TransitionBuilder to(EventStatus s) { t.setToStatus(s); return this; }
        TransitionBuilder attempt(int a) { t.setAttempt(a); return this; }
        TransitionBuilder worker(String w) { t.setWorker(w); return this; }
        TransitionBuilder reason(String r) { t.setReason(r); return this; }
        EventTransition build() { return t; }
    }

    private static final class EventRowMapper implements RowMapper<OutboxEvent> {
        @Override
        public OutboxEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
            OutboxEvent e = new OutboxEvent();
            e.setEventId(rs.getString("event_id"));
            e.setBusinessKey(rs.getString("business_key"));
            e.setEventType(rs.getString("event_type"));
            e.setPayload(rs.getString("payload"));
            e.setStatus(EventStatus.valueOf(rs.getString("status")));
            e.setAttempts(rs.getInt("attempts"));
            e.setMaxAttempts(rs.getInt("max_attempts"));
            e.setFailureReason(rs.getString("failure_reason"));
            e.setLockedBy(rs.getString("locked_by"));
            e.setLockedAt(toInstant(rs.getTimestamp("locked_at")));
            e.setNextAttemptAt(toInstant(rs.getTimestamp("next_attempt_at")));
            e.setCreatedAt(toInstant(rs.getTimestamp("created_at")));
            e.setUpdatedAt(toInstant(rs.getTimestamp("updated_at")));
            e.setVersion(rs.getInt("version"));
            return e;
        }
    }

    private static final class TransitionRowMapper implements RowMapper<EventTransition> {
        @Override
        public EventTransition mapRow(ResultSet rs, int rowNum) throws SQLException {
            EventTransition t = new EventTransition();
            t.setId(rs.getLong("id"));
            t.setEventId(rs.getString("event_id"));
            String from = rs.getString("from_status");
            t.setFromStatus(from == null ? null : EventStatus.valueOf(from));
            t.setToStatus(EventStatus.valueOf(rs.getString("to_status")));
            t.setAttempt(rs.getInt("attempt"));
            t.setReason(rs.getString("reason"));
            t.setWorker(rs.getString("worker"));
            t.setOccurredAt(toInstant(rs.getTimestamp("occurred_at")));
            return t;
        }
    }
}
