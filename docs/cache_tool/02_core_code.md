# DuckDB Cache API、Transaction 与 Capacity

> **Status: Implemented design.** 本文定义当前 `DuckDbOhlcvCache` 的应用边界和原子流程。

## 1. Public API

```python
class DuckDbOhlcvCache:
    def read_best_prefix(
        self,
        series_key: str,
        since: int,
        max_rows: int | None,
    ) -> list[OhlcvRow]: ...

    def write_segment(
        self,
        series_key: str,
        result: OhlcvResult,
        verified_covered_from: int | None,
    ) -> None: ...
```

不公开 `clear`、`prune`、`compact` 或通用 SQL/DSL。Capacity eviction 是 `write_segment()` 内部 transaction step，不是 Route 可调用能力。

## 2. Read candidate

对带明确 `since` 的请求，candidate 基本条件是：

```text
segment.covered_from <= since <= segment.last_time
```

三种安全命中：

```text
Exact       : 存在 time == since
Bracketed   : 同一 segment 存在 time < since 和 time > since
Leading gap : covered_from <= since < first_time
```

读取统一返回：

```sql
SELECT time, open, high, low, close, volume
FROM ohlcv_rows
WHERE segment_id = ?
  AND time >= ?
ORDER BY time
LIMIT ?;
```

`?` 是 parameter binding，不拼接用户 SQL。小于 since 的 predecessor 只用于证明 Bracketed coverage，绝不返回。

## 3. Best-prefix selection

多个 candidate 时：

1. 选从 `since` 起可复用 rows 最多的 segment；
2. 相同时选 `updated_at` 更新的 segment；
3. 仍相同时按 `segment_id ASC`；
4. 只读这一个 segment，不再读第二个。

Candidate selection 和 row read 在一条 SQL 或同一 read transaction 中完成，避免并发 write 导致 metadata/rows 来自不同 snapshot。

`max_rows` 是内存/response budget：

- `SinceLimit` 最多读 `limit`；
- `SinceLatest` 最多读 100,001 根用于识别超限；
- `LatestLimit` 不读 cache。

## 4. Cacheable row selection

Cache 不自己判断尾根，只消费 Client 已给出的证据：

```python
if not result.rows:
    return

cache_rows = result.rows if result.last_bar_completion_confirmed else result.rows[:-1]
```

`false` 是 unknown，不是 confirmed-open。`null` 只对应空 rows。去尾后无 rows 时整个 write no-op，不创建空 segment。

## 5. Write transaction

```text
acquire per-database write lock
→ BEGIN
→ validate canonical cache_rows/coverage
→ 查找与 incoming 有 exact timestamp 交集的所有 segments
→ 无 overlap：新建 segment_id
→ 有 overlap：选 canonical segment
→ merge incoming + all overlap segments
→ valid incoming row 在同 timestamp 冲突时胜出
→ 删除被吸收的 rows/metadata
→ 重算 canonical first/last/count/covered_from
→ enforce per-series/global capacity
→ 重算所有被 eviction 影响的 metadata
→ COMMIT
→ release lock
```

任一步失败都 rollback。Network 请求必须在锁外完成。

## 6. Overlap 与 coverage merge

Overlap 定义：

```text
incoming timestamp set ∩ segment timestamp set != ∅
```

不能只用 `[first_time,last_time]` 相交。

Canonical segment 优先保留 `row_count` 最大者；相同按 `segment_id ASC`。合并后：

```text
first_time = MIN(actual row time)
last_time  = MAX(actual row time)
row_count  = COUNT(actual rows)
covered_from = earliest still-valid coverage lower bound
```

当更早 verified request 实际连接到该 chain：

```python
covered_from = min(existing_covered_from, verified_covered_from)
```

## 7. History update policy

```text
valid incoming same timestamp → atomic replace
timestamp absent from network   → keep existing
incoming batch 有 invalid/NULL  → 整次 cache write no-op; keep existing
new timestamp                   → insert
```

Cache 不根据“network 没有返回”生成 tombstone，因为自然休盘和 Provider 历史删除无法仅从缺行区分。

## 8. Capacity definitions

```text
max_response_rows          = 100,000
max_cache_rows_per_series  = 2,000,000（默认）
max_cache_rows_total       = 20,000,000（默认）
```

Cache 计数：

```text
series_count = 该 series 的 distinct time
total_count  = 全部 live (series_key,time) identities
```

Global 不得 `COUNT(DISTINCT time)`，因为各 symbol/timeframe 会共享 timestamp。配置应在启动时满足：

```text
100,000 < per_series_limit <= total_limit
```

## 9. Automatic eviction

合并后在同一 transaction 中按顺序执行：

1. Current series 超限：按 `(time,segment_id)` 删除最旧 rows，直到 `floor(per_series_limit * 0.9)`。
2. Global 超限：按同一优先级跨 series 删除最旧 rows，直到 `floor(total_limit * 0.9)`。
3. 删空的 segment 删 metadata；只删部分的 segment 重算 metadata。

只能删 segment 前缀，不制造中间缺口。被截断的 segment 必须：

```text
covered_from = new first_time
```

因为被删区间原本确实包含 rows，不能继续宣称为 leading empty gap。

Incoming 不特殊保护。请求过旧历史时，新写 rows 可能当场被年龄优先级淘汰；这避免为保留旧数据而驱逐新数据。不记录 `last_accessed_at`，不实现 LRU。

Eviction 成功不影响 Route response。Eviction SQL/transaction 失败或处理后仍超限时 rollback，由全局 handler 返回 HTTP 507 `CACHE_CAPACITY_EXCEEDED`；服务进程继续。

## 10. Connection/lock lifecycle

- 一个 Uvicorn process read-write 一个 native DuckDB file。
- 不在 FastAPI threads 共享同一 Python connection。
- Cache 使用 thread-local connections。
- 每个 database path 共享一把 process-local `threading.Lock`。多个 Cache object 不得各自创建无关锁。
- Read 不加 application write lock，依赖 DuckDB snapshot isolation。
- Write 持锁并使用 transaction。
- 不使用 `FileLock`规避 DuckDB 的 single-writer-process 约束。
- Cache 跟踪本实例创建的全部 thread-local connections。Public read 进入 reader lifecycle scope；`close()` 先阻止新 reader，再等待 active readers 退出，最后关闭 connections。Write/close 继续由同一 per-database write lock 串行。`close()` 幂等，并在 application shutdown 或 `ExchangeManager` reinitialize 时执行；关闭后的 Cache object 不得重新使用。

## 11. Failure policy

- Cache read failure：warning，当作 miss，走完整 network path。
- 普通 cache write failure：error，不影响已成功的 network response。
- Overlap mismatch：放弃 prefix，从原始 query 完整重拉一次。
- Transaction conflict：rollback，按普通 write failure 处理。
- Eviction/capacity failure：rollback 并返回 507。

Capacity 只限制 logical rows，不是严格 database byte 上限。不增加 RSS 监控、JSON byte 估算或自动物理 compact；需要完全重置时在停止进程后删除精确 cache DB。
