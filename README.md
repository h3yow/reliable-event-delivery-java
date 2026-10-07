# reliable-event-delivery-java

一套完全在本地运行的可靠事件投递能力（不依赖外部消息中间件 / 数据库 / 真实凭据）。
业务动作被接受后产生的事件不会丢失：系统自己持续投递、失败按有界节奏自动重试、
中断后自动恢复、积压时有明确的背压信号，并且每一步状态迁移都可查询、可解释。

## 架构

```
业务动作 ──submit(key, payload)──▶ EventService ──▶ EventStore (本地线程安全存储)
                │ 容量满 → 429 CAPACITY_EXCEEDED          │  PENDING(nextAttemptAt 到期才算可投递)
                ▼                                          ▼
        EventDispatcher（可开关的自动循环，每个 tick：）
          1) recoverStuck: 卡死 IN_PROGRESS → PENDING
          2) findDuePending → 线程池 → EventProcessor.process(eventId, handler)
                │  claim: PENDING→IN_PROGRESS（原子，仅一个消费者成功）
                │  handler.handle(event)   ← 副作用，applied-set 保证 exactly-once
                ├─ 成功 ─▶ SUCCEEDED（终止）
                ├─ 可重试失败 ─▶ PENDING（attempts+1，nextAttemptAt=退避后的时间）
                │                耗尽 → DEAD(RETRY_EXHAUSTED)
                └─ NonRetryableException ─▶ DEAD(NON_RETRYABLE)
DEAD ──replay()──▶ PENDING（受控重放，attempts 归零）
GET /api/events/stats ──▶ 运维汇总（各状态计数 / 最久等待 / 重试分布 / 容量）
```

- `event/EventStore`：事件与进度的唯一事实来源；所有状态迁移原子完成并打印日志。
- `event/EventDispatcher`：自动运行循环（恢复 + 派发），`events.dispatcher.enabled` 可整体关闭。
- `event/EventProcessor`：投递与重试策略；并发/重复消费的幂等防线。
- `event/EventService` + `api/EventController`：提交、查询、重放、运维汇总入口。

## 投递语义

- **接受即不丢**：`submit` 返回前事件已落库（PENDING），之后任何消费者崩溃都不会丢失记录。
- **幂等提交**：同一 `idempotencyKey` + 相同 payload 重复提交返回同一条记录，不产生额外投递。
- **冲突拒绝**：同一 `idempotencyKey` + 不同 payload 抛出 `EventConflictException`，
  HTTP 层返回 `409 {"reason":"DUPLICATE_CONFLICT"}`。
- **at-least-once 投递 + exactly-once 副作用**：投递可能重复，但副作用只应用一次（见并发边界）。

## 自动运行（开关与节奏）

提交之后无需任何人工或外部触发，`EventDispatcher` 按固定节奏自动运转：

- 每个 tick（`events.dispatcher.poll-interval-ms`，默认 200ms）做两件事：
  1. **恢复**：把卡住超过 `events.recovery.stuck-timeout-ms`（默认 30s）的 IN_PROGRESS 事件放回 PENDING；
  2. **派发**：取最多 `events.dispatcher.batch-size`（默认 50）条**已到期**的 PENDING 事件
     （`nextAttemptAt` 为空或已过），交给 `events.dispatcher.threads`（默认 4）个工作线程处理。
- **失败重试节奏**：可重试失败后按指数退避安排下次尝试——
  `events.retry.initial-backoff-ms`（默认 500ms）× `events.retry.backoff-multiplier`（默认 2.0）^n，
  上限 `events.retry.max-backoff-ms`（默认 10s）；次数上限仍是 `events.max-attempts`（默认 3）。
- **等待重试不阻塞他人**：等待退避的事件只是"未到期"，调度器每轮只取到期事件，
  其他事件照常投递（`DispatcherAutoRunTest` 覆盖）。
- **开关**：`events.dispatcher.enabled=false` 可整体关闭自动运行（测试/演示用），
  关闭后仍可用 `EventProcessor.process` 手动投递（`DispatcherToggleTest` 覆盖）。

## 中断恢复的语义边界

- 消费者在 IN_PROGRESS 中途消失（进程崩溃、处理迟迟不收尾）时，记录完整保留；
  超过 `stuck-timeout-ms` 后由 dispatcher 自动放回 PENDING，**attempts 等进度不丢**。
- 恢复后不会产生重复副作用：已成功过的尝试记录在 applied-set 中，重复投递直接跳过；
  迟到完成上报（recovery 与旧消费者竞争）被安全忽略，状态不被破坏（`StuckRecoveryTest` 覆盖）。
- **边界说明**：applied-set 与事件存储均为进程内本地状态；进程重启后 applied-set 清空，
  但 SUCCEEDED 状态仍阻止重复投递，IN_PROGRESS 记录会被恢复流程重新认领。
  若某次处理"永远挂起"，恢复会把它放回队列、之后再次卡住——`stuck-timeout-ms` 应大于
  handler 的正常耗时；副作用若落到外部系统，handler 应以事件 id 做幂等键（`EventHandler` 契约要求可安全重入）。

## 容量与背压规则

- 积压量 = PENDING + IN_PROGRESS 条数（SUCCEEDED / DEAD 是终止态，不占容量）。
- 达到 `events.capacity.max-pending`（默认 10000）后，**新 key** 的提交被拒绝：
  HTTP `429 {"reason":"CAPACITY_EXCEEDED"}`——明确、可重试的背压信号，
  与业务冲突（409 `DUPLICATE_CONFLICT`）和系统故障（500）都可区分；事件不会被悄悄丢掉。
- **已满时已知 key 的重复提交仍然幂等接受**（不产生新负载，不算新提交）。
- 积压缓和（事件成功或进入 DEAD）后自动恢复接收，无需人工干预（`CapacityBackpressureTest` 覆盖）。

## 运维汇总

`GET /api/events/stats` 返回：

```json
{
  "counts": {"PENDING": 2, "IN_PROGRESS": 1, "SUCCEEDED": 5, "DEAD": 1},
  "active": 3,
  "maxPending": 10000,
  "accepting": true,
  "oldestPendingAgeMillis": 1234,
  "retryDistribution": {"1": 2, "2": 1}
}
```

- `counts`：各状态条数，看积压卡在哪个阶段；
- `oldestPendingAgeMillis`：最久待处理事件的等待时长（无待处理时为 null）；
- `retryDistribution`：已用尝试次数 → 事件数，看重试集中在哪个区间；
- `active` / `maxPending` / `accepting`：容量与背压状态。

## 重试与终止规则

- 每次投递尝试计 1 次 `attempts`，上限 `maxAttempts`（默认 3，可用 `events.max-attempts` 配置）。
- 可重试失败（普通异常）：未耗尽回到 PENDING 并按退避节奏等待下次投递；耗尽进入 `DEAD`，`deadReason=RETRY_EXHAUSTED`。
- 不可重试失败（`NonRetryableException`）：立即进入 `DEAD`，`deadReason=NON_RETRYABLE`。
- `DEAD` 是**可查询**的终止状态（`GET /api/events/{id}` 可见 `status/deadReason/lastError`），
  不阻塞其他事件的投递。
- **受控重放**：仅 DEAD 事件可 `POST /api/events/{id}/replay` 回到 PENDING（attempts 归零）；
  其他状态重放返回 `422 ILLEGAL_STATE`。

## 并发与幂等边界

- **认领互斥**：`claim` 原子地将 PENDING→IN_PROGRESS，并发消费者中只有一个进入 handler，
  其余立即返回当前状态，不会重复执行副作用。
- **重复投递抑制**：已成功（或已在 applied-set 中）的事件再次被 process 时直接跳过 handler。
- **中断可恢复**：见上文"中断恢复的语义边界"。

## 配置一览

| 配置 | 默认 | 说明 |
|---|---|---|
| `events.max-attempts` | 3 | 每条事件的投递尝试上限 |
| `events.dispatcher.enabled` | true | 自动运行总开关 |
| `events.dispatcher.poll-interval-ms` | 200 | tick 间隔 |
| `events.dispatcher.batch-size` | 50 | 每 tick 最多派发条数 |
| `events.dispatcher.threads` | 4 | 处理线程数 |
| `events.retry.initial-backoff-ms` | 500 | 首次重试退避 |
| `events.retry.backoff-multiplier` | 2.0 | 退避倍率 |
| `events.retry.max-backoff-ms` | 10000 | 退避上限 |
| `events.recovery.stuck-timeout-ms` | 30000 | IN_PROGRESS 卡死恢复阈值 |
| `events.capacity.max-pending` | 10000 | 积压上限（超出拒绝新提交） |

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/events` | 幂等提交 `{idempotencyKey, payload}`；冲突 409，容量满 429 |
| GET | `/api/events` | 列出全部事件及状态 |
| GET | `/api/events/{id}` | 查询单事件（含 deadReason/lastError/attempts/nextAttemptAt） |
| POST | `/api/events/{id}/replay` | 受控重放 DEAD 事件 |
| GET | `/api/events/stats` | 运维汇总（各状态计数 / 最久等待 / 重试分布 / 容量） |

## 本地验证

```bash
mvn test
```

- `DuplicateSubmissionTest`：重复提交幂等、冲突拒绝、成功后重复提交不产生新投递。
- `RetryExhaustionTest`：重试耗尽进入 DEAD(RETRY_EXHAUSTED)、不可重试立即 DEAD(NON_RETRYABLE)、
  重放后成功、DEAD 不阻塞其他事件。
- `ConcurrentConsumptionTest`：8 线程并发消费同一事件副作用仅执行一次、成功后重复投递被抑制、
  中途崩溃后状态可恢复。
- `DispatcherAutoRunTest`：提交后自动投递、失败按退避自动重试且有界、等待重试不阻塞其他事件。
- `DispatcherToggleTest`：`events.dispatcher.enabled=false` 可关闭自动运行。
- `StuckRecoveryTest`：卡死的 IN_PROGRESS 自动恢复、进度不丢、副作用不重复、未到期不误恢复。
- `CapacityBackpressureTest`：容量满拒绝新提交、与业务冲突可区分、已知 key 仍幂等、缓和后自动恢复。
- `EventStatsTest` / `OpsEndpointsTest`：运维汇总内容与 429/409 信号可区分。
- `EventControllerTest`：HTTP 层 409/422 原因可区分。

所有单测日志均打印事件标识与状态迁移轨迹，例如：

```
event=596cef5c-... transition PENDING->IN_PROGRESS attempts=0/3
event=596cef5c-... transition IN_PROGRESS->PENDING attempts=0/3 error=recovered from interrupted consumer
event=596cef5c-... recovered IN_PROGRESS->PENDING after being stuck since 2026-10-07T20:19:59Z
event=596cef5c-... transition IN_PROGRESS->SUCCEEDED attempts=0/3
```

手动演示（关闭自动运行，逐步观察状态变化）：

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--events.dispatcher.enabled=false --server.port=8080"
curl -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
     -d '{"idempotencyKey":"demo-1","payload":"hello"}'
curl localhost:8080/api/events/stats
```
