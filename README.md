# Reliable Event Delivery（本地可靠事件投递）

一套完全在本地运行的可靠事件投递能力：业务动作被接受后产生本地事件，
由后台 worker 按「至少一次（at-least-once）+ 处理器幂等」的方式投递给业务处理器。
不依赖任何外部消息中间件或真实凭据——仅使用嵌入式 H2 数据库与 Spring JDBC。

## 1. 投递语义

- **本地事件表（Transactional Outbox）**：业务动作被接受时，事件与一条
  `null -> PENDING` 的迁移记录在同一个本地事务中写入 `outbox_event` 表。
  接受即落库，事件不会因进程崩溃而丢失。
- **至少一次投递**：事件先被「认领」为 `IN_FLIGHT`，副作用在数据库事务**之外**执行，
  结果再用带归属条件的短事务写回。崩溃恢复（见 §4）可能让同一事件被再次执行，
  因此语义是 at-least-once。
- **恰好一次副作用由幂等保证**：投递层保证同一时刻同一事件只有一个执行者；
  跨崩溃/重放的重复调用由 `EventHandler` 按 `eventId` 去重。这是系统的幂等边界。

### 状态机

```
                 accept
   (不存在) ───────────────▶ PENDING
                               │ claim（条件更新，唯一赢家）
                               ▼
        ┌────────────────▶ IN_FLIGHT ────────────────┐
        │ stale 锁回收          │ 成功                 │ 失败
        │                      ▼                      ▼
        │                  DELIVERED（终态）      可重试异常
        │                                           │
        │                              attempts<max │ attempts>=max
        │                      ┌────────────┐       ▼
        │                      │            │  FAILED_RETRY_EXHAUSTED（终态）
        └────── RETRYING ◀─────┘ 退避结束     不可重试异常
                   │                          └▶ DEAD_LETTER（终态）
                   └ 受控重放：三个终态均可重置回 PENDING
```

| 状态 | 含义 | 是否终态 |
|---|---|---|
| `PENDING` | 已接受，等待投递 | 否 |
| `IN_FLIGHT` | 已被某个执行器认领，副作用进行中 | 否（锁超时可回收） |
| `RETRYING` | 本次失败可重试，退避等待中 | 否 |
| `DELIVERED` | 投递成功 | 是 |
| `FAILED_RETRY_EXHAUSTED` | 可重试失败但已达次数上限 | 是 |
| `DEAD_LETTER` | 不可重试失败（含无匹配处理器） | 是 |

## 2. 重复提交与冲突原因

- `outbox_event.business_key` 上有**唯一约束**。重复提交同一业务动作时，
  只有一个 INSERT 成功；并发提交由数据库行锁串行化，恰好接受一次。
- 重复提交被拒绝为 HTTP `409`，响应体带可区分的 `reason` 码与已存在事件 id：

| reason | 触发条件 |
|---|---|
| `DUPLICATE_BUSINESS_KEY` | 同 businessKey 的业务动作已接受过 |
| `REPLAY_NOT_TERMINAL` | 对非终态事件请求重放（含并发竞争失败） |
| `REPLAY_ALREADY_DELIVERED` | 对已成功事件未带 `force=true` 就请求重放 |
| `EVENT_NOT_FOUND` | 事件不存在（HTTP 404） |

## 3. 重试与终止规则

- 有上限的指数退避：`backoff = min(initialBackoff × multiplier^(n-1), maxBackoff)`，
  尝试次数（含首次）达到 `max-attempts` 后不再重试，进入
  `FAILED_RETRY_EXHAUSTED` 终态，失败原因写入 `failure_reason`。
- 处理器抛出 `NonRetryableException` 立即进入 `DEAD_LETTER`，不消耗后续重试预算；
  没有注册处理器的事件类型同样进入 `DEAD_LETTER`（原因 `no handler registered ...`）。
- **失败事件不阻塞其他事件**：每个事件独立认领、独立退避（`next_attempt_at`），
  worker 线程池继续处理其余事件。
- 两个失败终态均可通过 API 查询（事件详情、按状态列表、失败原因与完整迁移轨迹）。

## 4. 并发、崩溃恢复与幂等边界

**并发认领。** 认领是一条带前置条件的 UPDATE：

```sql
UPDATE outbox_event SET status='IN_FLIGHT', attempts=attempts+1, locked_by=:execId, ...
WHERE event_id=:id
  AND ( (status IN ('PENDING','RETRYING') AND next_attempt_at<=:now)
        OR (status='IN_FLIGHT' AND locked_at<=:staleBefore) )
```

候选行先 `SELECT ... FOR UPDATE` 锁定，多个 worker 竞争时数据库行锁串行化，
只有受影响行数为 1 的执行者得到该事件；高并发下抢空的执行者会在候选窗口内
继续尝试后续行，避免活锁。

**结果写回带归属条件。** `markDelivered / markRetrying / markRetryExhausted /
markDeadLetter` 都要求 `status='IN_FLIGHT' AND locked_by=:execId`，
影响 0 行即说明归属已变（被回收），写回被忽略，旧执行者不会覆盖新执行者的结果。

**中断恢复。** 副作用执行中进程崩溃 → 事件停留在 `IN_FLIGHT`。
`locked_at` 超过 `lock-timeout` 后，该锁被视为 stale，任意 worker 可重新认领
（迁移轨迹中出现 `IN_FLIGHT -> IN_FLIGHT`，reason 为 `stale lock recovered ...`）。
因此中断后进度可恢复、事件不丢失；代价是这次副作用可能已部分发生，
**处理器必须按 eventId 幂等**。

**执行标识。** 每次认领用全局唯一的 `node-<uuid>-exec-<uuid>` 写入 `locked_by`，
使崩溃后新执行者的标识必然不同于旧执行者，归属判断与审计轨迹可区分。

**幂等边界总结：**
- 投递层保证：事件不丢、不出现两个执行者同时完成同一事件、终态可查可重放。
- 处理器负责：同一 `eventId` 的外部副作用去重（at-least-once → effectively-once）。

## 5. 受控重放

仅终态事件可重放；重放必须提供 `reason`（写入审计轨迹）：

- `FAILED_RETRY_EXHAUSTED` / `DEAD_LETTER` → 直接重放（重置 attempts、回到 PENDING）；
- `DELIVERED` → 必须 `force=true`，否则 409 `REPLAY_ALREADY_DELIVERED`；
- 非终态 → 409 `REPLAY_NOT_TERMINAL`。

重放只重置状态与重试计数，**不会更换 eventId**，因此幂等键保持稳定。

## 6. 可观测性

- 每次状态迁移都在**同一事务内**追加到 `event_transition` 表
  （from/to、attempt、worker、reason、时间），可通过 API 查询完整轨迹。
- 日志通过 SLF4J MDC 输出 `eventId`，关键迁移点均有结构化日志，例如：
  `event claimed ... transition=PENDING->IN_FLIGHT attempt=1 lockedBy=...`、
  `event retry scheduled ... transition=IN_FLIGHT->RETRYING ... backoff=10ms`、
  `event failed-retry-exhausted ... attempt=3/3 reason=...`、
  `delivered mark lost ... (ownership changed, result ignored)`。

## 7. 配置项（前缀 `reliable.delivery`）

| 属性 | 默认值 | 说明 |
|---|---|---|
| `max-attempts` | 5 | 单事件最大尝试次数（含首次） |
| `initial-backoff` | 500ms | 首次失败后退避 |
| `backoff-multiplier` | 2.0 | 退避指数 |
| `max-backoff` | 30s | 退避上限 |
| `lock-timeout` | 60s | IN_FLIGHT 锁多久后视为 worker 死亡并回收 |
| `worker-count` | 2 | 并发消费者线程数 |
| `poll-interval` | 200ms | poller 调度间隔 |
| `claim-batch-size` | 32 | 单次认领扫描的候选窗口 |
| `poller-enabled` | true | 是否启用后台 poller |

## 8. HTTP 接口

| 方法与路径 | 说明 |
|---|---|
| `POST /api/events` | 接受业务动作，body：`businessKey, eventType, payload`；201 创建，409 重复 |
| `GET  /api/events/{id}` | 事件详情（状态、尝试次数、失败原因） |
| `GET  /api/events?status=FAILED_RETRY_EXHAUSTED` | 按状态列出（终态可查） |
| `GET  /api/events/{id}/transitions` | 状态迁移轨迹 |
| `POST /api/events/{id}/replay` | 受控重放，body：`force, reason` |

## 9. 本地运行与验证

```bash
# 运行应用（嵌入式 H2 内存库，server.port=0 随机端口）
mvn spring-boot:run

# 运行全部测试（14 个：单元 + Spring 集成）
mvn test
```

手工验证（应用启动后）：

```bash
# 接受事件
curl -s -X POST localhost:$PORT/api/events -H 'Content-Type: application/json' \
  -d '{"businessKey":"order-1","eventType":"demo.log","payload":"hello"}'
# 重复提交 → 409 + DUPLICATE_BUSINESS_KEY
curl -s -i -X POST localhost:$PORT/api/events -H 'Content-Type: application/json' \
  -d '{"businessKey":"order-1","eventType":"demo.log","payload":"hello"}'
# 查询轨迹
curl -s localhost:$PORT/api/events/$EVENT_ID/transitions
```

### 测试覆盖场景

| 测试类 | 覆盖点 |
|---|---|
| `RetryPolicyTest` | 退避指数增长、封顶、尝试预算有上限 |
| `DuplicateSubmissionIntegrationTest` | 顺序/12 线程并发重复提交：恰好接受一次、409 原因可区分、无重复投递 |
| `RetryExhaustionIntegrationTest` | 持续失败按上限（3 次）终止；失败原因落库；健康事件不被阻塞；迁移轨迹完整有序 |
| `DeadLetterAndReplayIntegrationTest` | 不可重试失败直达 DEAD_LETTER；非终态/已投递重放冲突原因；force 重放后再次投递 |
| `ConcurrentConsumptionIntegrationTest` | 8 线程提交 40 事件、4 worker 并发消费：每个事件恰好一次副作用 |
| `OutboxRepositoryConcurrencyTest` | 8 个认领器并发压仓储：20 条事件各被唯一认领一次（关闭 poller） |
| `CrashRecoveryIntegrationTest` | 副作用执行中“崩溃”：stale 锁 2s 后被不同执行器回收，事件最终投递成功；旧执行者迟到写回落空 |
| `EventApiIntegrationTest` | HTTP 层 409/404 的 reason 码与重放接口 |

测试日志在控制台打印 `eventId` 与每次状态迁移，测试方法内还会通过
`printTrail(...)` 输出该事件的完整迁移轨迹（见 surefire 报告中的 system-out）。
