# reliable-event-delivery-java

一套完全在本地运行的可靠事件投递能力（不依赖外部消息中间件 / 数据库 / 真实凭据）。
业务动作被接受后产生的事件不会丢失，重复提交、处理失败与并发消费都能得到确定且可解释的结果。

## 架构

```
业务动作 ──submit(key, payload)──▶ EventService ──▶ EventStore (本地线程安全存储)
                                                        │  PENDING
EventProcessor.process(eventId, handler) ◀──────────────┘
        │  claim: PENDING→IN_PROGRESS（原子，仅一个消费者成功）
        │  handler.handle(event)   ← 副作用，applied-set 保证 exactly-once
        ├─ 成功 ─▶ SUCCEEDED（终止）
        ├─ 可重试失败 ─▶ PENDING（attempts+1，未耗尽）/ DEAD(RETRY_EXHAUSTED)
        └─ NonRetryableException ─▶ DEAD(NON_RETRYABLE)
DEAD ──replay()──▶ PENDING（受控重放，attempts 归零）
```

- `event/EventStore`：事件与进度的唯一事实来源；所有状态迁移原子完成并打印日志。
- `event/EventProcessor`：投递与重试策略；并发/重复消费的幂等防线。
- `event/EventService` + `api/EventController`：提交、查询、重放入口。

## 投递语义

- **接受即不丢**：`submit` 返回前事件已落库（PENDING），之后任何消费者崩溃都不会丢失记录。
- **幂等提交**：同一 `idempotencyKey` + 相同 payload 重复提交返回同一条记录，不产生额外投递。
- **冲突拒绝**：同一 `idempotencyKey` + 不同 payload 抛出 `EventConflictException`，
  HTTP 层返回 `409 {"reason":"DUPLICATE_CONFLICT"}`，与 `NOT_FOUND` / `ILLEGAL_STATE` 可区分。
- **at-least-once 投递 + exactly-once 副作用**：投递可能重复，但副作用只应用一次（见并发边界）。

## 重试与终止规则

- 每次投递尝试计 1 次 `attempts`，上限 `maxAttempts`（默认 3，可用 `events.max-attempts` 配置）。
- 可重试失败（普通异常）：未耗尽回到 PENDING 等待下次投递；耗尽进入 `DEAD`，`deadReason=RETRY_EXHAUSTED`。
- 不可重试失败（`NonRetryableException`）：立即进入 `DEAD`，`deadReason=NON_RETRYABLE`。
- `DEAD` 是**可查询**的终止状态（`GET /api/events/{id}` 可见 `status/deadReason/lastError`），
  不阻塞其他事件的投递。
- **受控重放**：仅 DEAD 事件可 `POST /api/events/{id}/replay` 回到 PENDING（attempts 归零）；
  其他状态重放返回 `422 ILLEGAL_STATE`。

## 并发与幂等边界

- **认领互斥**：`claim` 原子地将 PENDING→IN_PROGRESS，并发消费者中只有一个进入 handler，
  其余立即返回当前状态，不会重复执行副作用。
- **重复投递抑制**：已成功（或已在 applied-set 中）的事件再次被 process 时直接跳过 handler。
- **中断可恢复**：消费者在 IN_PROGRESS 中途崩溃时记录完整保留；通过 `markFailed` 失败回 PENDING
  后即可由新消费者继续，进度不丢失、副作用不重复。
- **边界说明**：applied-set 与事件存储均为进程内本地状态；进程重启后 applied-set 清空，
  但 SUCCEEDED 状态仍阻止重复投递，IN_PROGRESS 记录可被恢复流程重新认领。
  若副作用本身有外部系统，应让 handler 以事件 id 做幂等键（`EventHandler` 契约要求可安全重入）。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/events` | 幂等提交 `{idempotencyKey, payload}`；冲突返回 409 |
| GET | `/api/events` | 列出全部事件及状态 |
| GET | `/api/events/{id}` | 查询单事件（含 deadReason/lastError/attempts） |
| POST | `/api/events/{id}/replay` | 受控重放 DEAD 事件 |

## 验证方法

```bash
mvn test
```

- `DuplicateSubmissionTest`：重复提交幂等、冲突拒绝、成功后重复提交不产生新投递。
- `RetryExhaustionTest`：重试耗尽进入 DEAD(RETRY_EXHAUSTED)、不可重试立即 DEAD(NON_RETRYABLE)、
  重放后成功、DEAD 不阻塞其他事件。
- `ConcurrentConsumptionTest`：8 线程并发消费同一事件副作用仅执行一次、成功后重复投递被抑制、
  中途崩溃后状态可恢复。
- `EventControllerTest`：HTTP 层 409/422 原因可区分。

所有单测日志均打印事件标识与状态迁移轨迹，例如：

```
event=73c6a8a9-... transition PENDING->IN_PROGRESS attempts=2/3
event=73c6a8a9-... transition IN_PROGRESS->DEAD attempts=3/3 deadReason=RETRY_EXHAUSTED error=transient downstream error
event=bae90749-... transition DEAD->PENDING replay attempts=0/3
```
