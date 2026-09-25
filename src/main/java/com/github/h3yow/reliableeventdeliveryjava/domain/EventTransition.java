package com.github.h3yow.reliableeventdeliveryjava.domain;

import java.time.Instant;

/**
 * 一次事件状态迁移的审计记录（与状态变更在同一事务内写入）。
 */
public class EventTransition {
    private Long id;
    private String eventId;
    private EventStatus fromStatus;
    private EventStatus toStatus;
    private int attempt;
    private String reason;
    private String worker;
    private Instant occurredAt;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public EventStatus getFromStatus() { return fromStatus; }
    public void setFromStatus(EventStatus fromStatus) { this.fromStatus = fromStatus; }
    public EventStatus getToStatus() { return toStatus; }
    public void setToStatus(EventStatus toStatus) { this.toStatus = toStatus; }
    public int getAttempt() { return attempt; }
    public void setAttempt(int attempt) { this.attempt = attempt; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
    public String getWorker() { return worker; }
    public void setWorker(String worker) { this.worker = worker; }
    public Instant getOccurredAt() { return occurredAt; }
    public void setOccurredAt(Instant occurredAt) { this.occurredAt = occurredAt; }

    @Override
    public String toString() {
        return occurredAt + "  " + fromStatus + " -> " + toStatus
                + "  (attempt=" + attempt + ", worker=" + worker
                + (reason == null ? "" : ", reason='" + reason + "'") + ")";
    }
}
