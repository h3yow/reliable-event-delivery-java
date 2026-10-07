# reliable-event-delivery-java

一套完全在本地运行的可靠事件投递能力（不依赖外部消息中间件 / 数据库 / 真实凭据）。
业务动作被接受后产生的事件不会丢失：系统自己持续投递，失败按有界节奏自动重试，
中断后自动恢复，积压时给出明确背压信号，并提供运维汇总视图。

## 架构

```
业务动作 ──submit(key, payload)──▶ EventService ──▶ EventStore (本地线程安全存储)
                     │                                  │  PENDING（容量超限 → 429 OVERLOADED）
                     │                                  ▼
                     │              EventDispatcher ◀── 定时 sweep（可开关）
                     │                  │  ① reclaimStale：IN_PROGRESS 超时 → PENDING
                     │                  │  ② findDuePending：取出退避已到期的 PENDING
                     │                  ▼
                     │              EventProcessor.process(eventId, handler)
                     │                  │  claim: PENDING→IN_PROGRESS（原子，仅一个消费者成功）
                     │                  │  handler.handle(event)   ← 副作用，applied-set 保证 exactly-once
                     │                  ├─ 成功 ─▶ SUCCEEDED（终止）
                     │                  ├─ 可重试失败 ─▶ PENDING（attempts+1，nextAttemptAt=now+retryDelay）
                     │                  │                耗尽 ─▶ DEAD(RETRY_EXHAUSTED)
                     │                  └─ NonRetryableException ─▶ DEAD(NON_RETRYABLE)
                     └── DEAD ──replay()──▶ PENDING（受控重放，attempts 归零）
```

- `event/EventStore`：事件与进度的唯一事实来源；所有状态迁移原子完成并打印日志。
- `event/EventDispatcher`：自动投递循环；每轮先做中断恢复再投递到期事件。
- `event/EventProcessor`：投递与重试策略；并发/重复消费的幂等防线。
- `event/EventService` + `api/EventController`：提交、查询、重放、运维汇总入口。

## 投递语义

- **接受即不丢**：`submit` 返回前事件已落库（PENDING），之后任何消费者崩溃都不会丢失记录。
- **幂等提交**：同一 `idempotencyKey` + 相同 payload 重复提交返回同一条记录，不产生额外投递。
- **冲突拒绝**：同一 `idempotencyKey` + 不同 payload 抛出 `EventConflictException`，
  HTTP 层返回 `409 {"reason":"DUPLICATE_CONFLICT"}`。
- **at-least-once 投递 + exactly-once 副作用**：投递可能重复，但副作用只应用一次（见并发边界）。

## 自动运行（本轮新增）

- 提交后无需人工或外部触发：`EventDispatcher` 按固定间隔自动 sweep，把待投递事件处理掉。
- **开关**：`events.dispatcher.enabled=false` 可整体关掉自动循环（测试/演示用），
  此时仍可手动调用 `EventDispatcher.runOnce()` 单步推进。
- **节奏**：`events.dispatcher.interval-ms`（默认 1000）为两次 sweep 的间隔。
- **有界重试节奏**：可重试失败回到 PENDING 时携带 `nextAttemptAt = now + events.retry-delay-ms`
  （默认 2000），到期才会被再次拾取；尝试次数上限仍为 `events.max-attempts`。
- **互不阻塞**：某条事件在退避等待期间只是被 sweep 跳过，其他到期事件照常投递，
  不会被它一起卡住。

## 中断恢复（本轮新增）

- 消费者处理到一半崩溃（或某次处理迟迟未收尾）时，事件停留在 IN_PROGRESS，记录完整保留。
- 每轮 sweep 先执行恢复：IN_PROGRESS 持续时间超过 `events.recovery.stale-timeout-ms`
  （默认 30000）的事件被自动回收为 PENDING 并立即可投递，无需人工介入。
- **进度不丢**：回收不消耗重试预算（attempts 保持不变）——被中断的尝试没有上报结果，
  不计入失败次数；此前已累计的失败次数、lastError 全部保留。
- **副作用不重复**：恢复后重新投递仍走 claim 互斥 + applied-set 抑制，同一 JVM 内
  已成功的事件绝不会重复执行副作用。
- **语义边界**：stale-timeout 必须明显大于 handler 的最大执行时长。若原消费者其实
  没死（只是慢），超时回收后新消费者可能与旧消费者并发执行副作用——这是
  at-least-once 的固有边界，`EventHandler` 契约要求 handler 以事件 id 做幂等键、可安全重入。

## 容量与背压（本轮新增）

- 待处理量（PENDING + IN_PROGRESS）达到 `events.capacity.max-pending`（默认 1000）时，
  新提交被明确拒绝：抛出 `EventOverloadException`，HTTP 层返回
  `429 {"reason":"OVERLOADED"}`。
- 信号可区分：429 OVERLOADED（暂时过载，可稍后重试）≠ 409 DUPLICATE_CONFLICT（业务冲突）
  ≠ 500（内部故障）；被拒绝的事件不会悄悄落库。
- 已存在 key 的幂等重提交在满载时仍正常应答（不产生新积压）；冲突检测也不受影响。
- **自动恢复**：积压被消费降到上限以下后，新提交自动恢复接收，无需人工干预。
- 容量检查与入库在同一临界区完成，并发提交下上限严格成立。

## 运维汇总（本轮新增）

`GET /api/events/stats` 返回当前管线的可解释快照：

```json
{
  "countsByStatus": {"PENDING": 3, "IN_PROGRESS": 1, "SUCCEEDED": 40, "DEAD": 2},
  "backlog": 4,
  "maxPending": 1000,
  "oldestPendingWaitMs": 8123,
  "retryDistribution": {"1": 5, "2": 2, "3": 1},
  "generatedAt": "2026-10-08T04:00:00Z"
}
```

- `countsByStatus`：各状态事件数，判断积压卡在哪个阶段。
- `backlog` / `maxPending`：当前积压与容量上限（背压阈值）。
- `oldestPendingWaitMs`：最久的 PENDING 事件已等待毫秒数（自最近一次进入 PENDING 起算）；
  无待处理事件时为 null。
- `retryDistribution`：重试次数 → 事件数，看失败大致分布在哪个尝试区间。

## 重试与终止规则

- 每次投递失败计 1 次 `attempts`，上限 `maxAttempts`（默认 3，`events.max-attempts` 配置）。
- 可重试失败（普通异常）：未耗尽回到 PENDING（带退避）；耗尽进入 `DEAD`，`deadReason=RETRY_EXHAUSTED`。
- 不可重试失败（`NonRetryableException`）：立即进入 `DEAD`，`deadReason=NON_RETRYABLE`。
- `DEAD` 是**可查询**的终止状态，不阻塞其他事件的投递。
- **受控重放**：仅 DEAD 事件可 `POST /api/events/{id}/replay` 回到 PENDING（attempts 归零）；
  其他状态重放返回 `422 ILLEGAL_STATE`。

## 并发与幂等边界

- **认领互斥**：`claim` 原子地将 PENDING→IN_PROGRESS，并发消费者中只有一个进入 handler。
- **重复投递抑制**：已成功（或已在 applied-set 中）的事件再次被 process 时直接跳过 handler。
- **边界说明**：applied-set 与事件存储均为进程内本地状态；进程重启后 applied-set 清空，
  但 SUCCEEDED 状态仍阻止重复投递，IN_PROGRESS 记录由恢复流程回收。
  若副作用落在外部系统，handler 应以事件 id 做幂等键。

## 配置一览

| 配置项 | 默认 | 说明 |
|---|---|---|
| `events.max-attempts` | 3 | 每条事件的失败尝试上限 |
| `events.retry-delay-ms` | 2000 | 可重试失败后的退避时长（到期才再投递） |
| `events.dispatcher.enabled` | true | 自动投递循环开关 |
| `events.dispatcher.interval-ms` | 1000 | 自动 sweep 间隔 |
| `events.recovery.stale-timeout-ms` | 30000 | IN_PROGRESS 超过该时长被回收 |
| `events.capacity.max-pending` | 1000 | 积压上限，达到后新提交返回 429 |

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/events` | 幂等提交 `{idempotencyKey, payload}`；冲突 409，过载 429 |
| GET | `/api/events` | 列出全部事件及状态 |
| GET | `/api/events/{id}` | 查询单事件（含 deadReason/lastError/attempts/nextAttemptAt） |
| POST | `/api/events/{id}/replay` | 受控重放 DEAD 事件 |
| GET | `/api/events/stats` | 运维汇总：状态计数、积压、最久等待、重试分布 |

## 本地验证

```bash
mvn test
```

- `DuplicateSubmissionTest`：重复提交幂等、冲突拒绝、成功后重复提交不产生新投递。
- `RetryExhaustionTest`：重试耗尽 DEAD(RETRY_EXHAUSTED)、不可重试立即 DEAD(NON_RETRYABLE)、
  重放后成功、DEAD 不阻塞其他事件。
- `ConcurrentConsumptionTest`：并发消费副作用仅一次、成功后重复投递被抑制、崩溃后状态可恢复。
- `DispatcherAutoRunTest`：提交后自动投递无需触发、退避事件不阻塞其他事件、调度开关生效。
- `DispatcherSchedulingTest`：Spring 调度循环端到端自动投递（awaitility 等待 SUCCEEDED）。
- `StaleRecoveryTest`：崩溃事件超时自动回收、恢复后副作用恰好一次、重试进度保留、
  新鲜 IN_PROGRESS 不被误回收。
- `CapacityBackpressureTest`：超限给出专属过载信号、不静默丢弃、幂等重提交不受限、
  积压排空后自动恢复接收、IN_PROGRESS 计入积压。
- `OpsSummaryTest`：状态计数、最久等待时长、重试分布、空库零值。
- `EventControllerTest` / `EventControllerBackpressureTest`：HTTP 层 409/422/429 原因可区分、
  stats 端点字段正确。

所有单测日志均打印事件标识与状态迁移轨迹，例如：

```
event=cd3975a4-... transition null->PENDING attempts=0/3 backlog=1/1000
event=cd3975a4-... transition PENDING->IN_PROGRESS attempts=0/3
event=cd3975a4-... transition IN_PROGRESS->PENDING attempts=0/3 error=recovered from stale IN_PROGRESS
event=cd3975a4-... recovery=stale-reclaim attempts=0/3 kept, due immediately
event=73c6a8a9-... transition IN_PROGRESS->PENDING attempts=2/3 error=transient downstream error nextAttemptAt=2026-10-08T04:00:02Z
event=73c6a8a9-... transition IN_PROGRESS->DEAD attempts=3/3 deadReason=RETRY_EXHAUSTED
```

手动体验自动运行与背压：

```bash
mvn spring-boot:run                                        # 自动投递默认开启
curl -X POST localhost:8080/api/events \
  -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"demo-1","payload":"hello"}'       # 约 1s 内自动 SUCCEEDED
curl localhost:8080/api/events/stats                       # 查看汇总
mvn spring-boot:run -Dspring-boot.run.arguments="--events.dispatcher.enabled=false \
  --events.capacity.max-pending=2"                         # 关自动循环 + 小容量观察 429
```
