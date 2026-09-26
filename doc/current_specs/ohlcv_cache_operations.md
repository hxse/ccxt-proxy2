# OHLCV 缓存 API、事务与容量

## 公开 API

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

    def write_tq_segment(self, series: TqOhlcvSeries, batch: TqOhlcvBatch) -> None: ...

    def read_contiguous_before(self, series_key: str, end_time: int, max_rows: int) -> CachedHistory: ...

    def read_connected_history(self, series_key: str, batch: OhlcvResult | TqOhlcvBatch, max_rows: int) -> CachedHistory: ...

    def read_latest_summary(self, series_key: str) -> SeriesSummary: ...

    def close(self) -> None: ...
```

不公开 `clear`、`prune`、`compact` 或通用 SQL/DSL。Capacity eviction 是 `write_segment()` 内部 transaction step，不是 Route 可调用能力。

## 可读候选片段

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

## 最佳前缀选择

多个 candidate 时：

1. 选从 `since` 起可复用 rows 最多的 segment；
2. 相同时选 `updated_at` 更新的 segment；
3. 仍相同时按 `segment_id ASC`；
4. 只读这一个 segment，不再读第二个。

Candidate selection 和 row read 在一条 SQL 或同一 read transaction 中完成，避免并发 write 导致 metadata/rows 来自不同 snapshot。

`max_rows` 是内存/response budget：

- `SinceLimit` 最多读 `limit`；
- `SinceLatest` 最多读 100,001 根用于识别超限；
- `LatestLimit` 固定周期复用快照查询的前缀读取，候选裁到最初的 S；周线以上不读写缓存。

## 可缓存行选择

Cache 不自己判断尾根，只消费 Client 已给出的证据：

```python
if not result.rows:
    return

cache_rows = result.rows if result.last_bar_completion_confirmed else result.rows[:-1]
```

`false` 是 unknown，不是 confirmed-open。`null` 只对应空 rows。去尾后无 rows 时整个 write no-op，不创建空 segment。调用方提交完整结果，持久化去尾只在缓存内部执行；已从数据库读出的末行不再去尾。

## 写入事务

```text
选择并验证可缓存 rows 与 coverage
→ acquire per-database write lock
→ BEGIN
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

## 重叠与覆盖范围合并

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

## 历史数据更新

```text
valid incoming same timestamp → atomic replace
timestamp absent from network   → keep existing
incoming batch 有 invalid/NULL  → 整次 cache write no-op; keep existing
new timestamp                   → insert
```

Cache 不根据“network 没有返回”生成 tombstone，因为自然休盘和 Provider 历史删除无法仅从缺行区分。

非空合法可缓存批次继续进入事务与 upsert。当前没有内容相同即零写入的保证；不能在调用方用“没有新时间戳”跳过片段桥接、价格修订或覆盖起点扩展。完整缓存命中只读返回，不因读取而重复写回。

## 容量定义

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

## 自动淘汰

合并后在同一 transaction 中按顺序执行：

1. Current series 超限：按 `(time,segment_id)` 删除最旧 rows，直到 `floor(per_series_limit * 0.9)`。
2. Global 超限：毫秒先转 HUGEINT 再乘一百万，纳秒转 HUGEINT 原值，按精确时间与 segment_id 跨 series 删除最旧 rows，直到 `floor(total_limit * 0.9)`。
3. 删空的 segment 删 metadata；只删部分的 segment 重算 metadata。

只能删 segment 前缀，不制造中间缺口。被截断的 segment 必须：

```text
covered_from = new first_time
```

因为被删区间原本确实包含 rows，不能继续宣称为 leading empty gap。

Incoming 不特殊保护。请求过旧历史时，新写 rows 可能当场被年龄优先级淘汰；这避免为保留旧数据而驱逐新数据。不记录 `last_accessed_at`，不实现 LRU。

Eviction 成功不影响 Route response。Eviction SQL/transaction 失败或处理后仍超限时 rollback，由全局 handler 返回 HTTP 507 `CACHE_CAPACITY_EXCEEDED`；服务进程继续。

## 连接与锁生命周期

- 一个 Uvicorn process read-write 一个 native DuckDB file。
- 不在 FastAPI threads 共享同一 Python connection。
- Cache 使用 thread-local connections。
- 每个 database path 共享一把 process-local `threading.Lock`。多个 Cache object 不得各自创建无关锁。
- Read 不加 application write lock，依赖 DuckDB snapshot isolation。
- Write 持锁并使用 transaction。
- 不使用 `FileLock`规避 DuckDB 的 single-writer-process 约束。
- Cache 跟踪本实例创建的全部 thread-local connections。Public read 进入 reader lifecycle scope；`close()` 先阻止新 reader，再等待 active readers 退出，最后关闭 connections。Write/close 继续由同一 per-database write lock 串行。`close()` 幂等，由应用持有的 CacheResource 在所有消费者关闭后执行；ExchangeManager reinitialize 只关闭其客户端；关闭后的 Cache object 不得重新使用。

## 失败规则

- Cache read failure：warning，当作 miss，走完整 network path。
- 普通 cache write failure：error，不影响已成功的 network response。
- Overlap mismatch：放弃 prefix，从原始 query 完整重拉一次。
- Transaction conflict：rollback，按普通 write failure 处理。
- Eviction/capacity failure：rollback 并返回 507。

Capacity 只限制 logical rows，不是严格 database byte 上限。不增加 RSS 监控、JSON byte 估算或自动物理 compact；需要完全重置时在停止进程后删除精确 cache DB。

## 向前历史、连接历史与概况

read_contiguous_before 在包含式 end_time 之前选择最新实际行所在片段，取其末 max_rows 根并升序返回；不证明缓存已经追上 end_time。read_connected_history 仅用本批次可持久化行的真实时间戳交集选一个可向前复用最多的片段，平局按更新时间、ID；未知尾根不能独自建立连接。读取裁至网络批次上界，无连接返回空，不跨独立片段拼接。

新增读取的 max_rows 为 1..100000 整数，不改变 read_best_prefix 原有 100001 根溢出检测能力。CachedHistory.rows 保持 CCXT tuple 或 TQ records 类型；TQ 读取不再次去尾。read_latest_summary 单快照返回 start/end/count、total_count、segment_count、time_unit；端点为原生单位，start 为实际首行，空库为 null/null/0；不暴露片段 ID。所有查询在同一 SQL 快照及 reader scope 内完成。

## 日期高级接口

read_calendar_range(start,end) 返回 CalendarSourceResult 或 miss；submit_calendar(result) 原子提交完整自然日表与源事实。read_mapping_range(symbol,dates,max_date) 按经日历核实的交易日和本次 D 返回 CachedMapping 或 miss；submit_mapping(result) 原子保存日表、必要节点、独立当前核验行与事实。read_metadata_facts(kind,symbol=None) 只读相应类型事实，不接受任意 SQL/事实键写入。

日期读取在同一 reader scope 和只读事务中选择片段、读取数据及上下文；写入使用与普通行情相同的路径写锁与事务。完整规则见 [TQ 元数据](tq_metadata.md)。

read_matching_mapping(result) 接收本次有效 MappingSourceResult，在同一读事务中核对源摘要、D 和目标范围的完整日记录及真实节点/前驱。仅完整相同返回 CachedMapping，否则 miss；全局摘要相同不能替其他未修订的旧片段背书。业务通过该高级入口复用映射，不在路由另造连续性判断。

## 过渡窗口接口

read_transition_prefix(identity,count) 返回已确认窗口前 N 根或 miss；submit_transition_window(identity,target_count,batch) 接受同次目标 N＋1 根、unknown 末根，验证整批后共用 eligible_rows 保存 N 根。写锁内比较实际 K，只允许同长修订或完整扩长；不与普通行情或其他来源逐行补长。完整资格见 [TQ 换月过渡](tq_transition.md)。
