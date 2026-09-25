-- 可靠事件投递：本地 outbox 表 + 状态迁移审计表（H2 方言）
CREATE TABLE IF NOT EXISTS outbox_event (
    event_id         VARCHAR(64)  NOT NULL PRIMARY KEY,
    business_key     VARCHAR(128) NOT NULL,
    event_type       VARCHAR(128) NOT NULL,
    payload          CLOB,
    status           VARCHAR(32)  NOT NULL,
    attempts         INT          NOT NULL DEFAULT 0,
    max_attempts     INT          NOT NULL,
    failure_reason   VARCHAR(2000),
    locked_by        VARCHAR(128),
    locked_at        TIMESTAMP WITH TIME ZONE,
    next_attempt_at  TIMESTAMP WITH TIME ZONE,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    version          INT          NOT NULL DEFAULT 0,
    CONSTRAINT uq_outbox_business_key UNIQUE (business_key)
);

-- 调度扫描：按状态 + 下次尝试时间
CREATE INDEX IF NOT EXISTS idx_outbox_dispatch
    ON outbox_event (status, next_attempt_at);
-- 卡死锁回收：按状态 + 加锁时间
CREATE INDEX IF NOT EXISTS idx_outbox_locked
    ON outbox_event (status, locked_at);

CREATE TABLE IF NOT EXISTS event_transition (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id    VARCHAR(64) NOT NULL,
    from_status VARCHAR(32),
    to_status   VARCHAR(32) NOT NULL,
    attempt     INT         NOT NULL,
    reason      VARCHAR(2000),
    worker      VARCHAR(128),
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transition_event
    ON event_transition (event_id, id);
