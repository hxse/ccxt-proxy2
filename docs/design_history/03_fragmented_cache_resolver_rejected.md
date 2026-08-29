# 通用缓存系统

> **Status: Rejected design archive. Do not implement.** 本文从早期 `todo/` 完整迁入正式文档区，只保留历史讨论；现行规范以 [`docs/README.md`](../README.md) 标记的 Implemented design 为准。


> **文档状态：Ideas archive，非实施计划。** 这是一个未来可能考虑、但由于复杂度过高而更可能不会实施的 TODO；本文仅保留设计思路，不代表当前架构决策或开发排期。

## 1. 三个公开入口，一个共享引擎

建议公开三个 strongly typed 入口：

```python
class OhlcvCacheEngine:
    def resolve_since_limit(
        self,
        intent: SinceLimitIntent,
        source: OhlcvSource,
    ) -> pl.DataFrame: ...

    def resolve_latest_limit(
        self,
        intent: LatestLimitIntent,
        source: OhlcvSource,
    ) -> pl.DataFrame: ...

    def resolve_since_latest(
        self,
        intent: SinceLatestIntent,
        source: OhlcvSource,
    ) -> pl.DataFrame: ...
```

三个入口共享：

```text
ProofSpanIndex
ForwardWalker
BackwardWalker
SegmentPlanner
CacheEstimator
CacheStore
MergeValidator
```

不是三套独立缓存实现。也可以保留一个内部 `_resolve(intent, source)`，由三个公开入口调用。

不能只传 callback 而不传 RouteIntent，因为 CacheEngine 需要知道最终遍历方向、终止条件和结果裁剪方式。

---

## 2. CacheStore

CacheStore 只提供本地原语：

```python
class OhlcvCacheStore:
    def get_proven_spans(series_key) -> list[ProofSpan]: ...

    def read_rows(
        series_key,
        start=None,
        end=None,
        order="asc",
        limit=None,
    ) -> pl.DataFrame: ...

    def write_rows(series_key, data): ...
    def append_proof(series_key, proof): ...
    def compact_proof(series_key): ...
```

不再使用面向单一前缀的 `find_span_end` 作为主要规划接口。Resolver 需要完整、排序、互不重叠的 proven span list。

`read_tail` 可以作为内部性能优化，但不承担 LatestLimit 的用户语义；也可以统一表示为：

```python
read_rows(order="desc", limit=N)
```

CacheStore 不允许：

- import CCXT/TQSDK/FastAPI；
- 持有 Provider Client；
- 决定 page limit；
- 执行 retry；
- 根据 provider 分支；
- 调用原始网络 API。

---

## 3. 多段 proven spans

假设：

```text
ProofSpan A：[20,30]
ProofSpan B：[40,50]
```

`30→40` 是 unproven gap，不能假设它是节假日，也不能假设它一定有数据。

如果一个 proven span 内的实际 timestamps 为：

```text
[20,21,22,26,30]
```

其中时间跳跃已由 fetch chain 证明，因此是合法的非连续数据。

proof compaction：

- overlap/包含的 spans 合并；
- 未证明缺口保持分离；
- 不使用 interval adjacency 合并。

---

## 4. RouteIntent × CacheTopology

| RouteIntent | 零缓存 | 多段缓存 | 看似覆盖完整 |
| --- | --- | --- | --- |
| SinceLimit | FullQuery | ForwardWalker + AfterCount | 不请求网络 |
| LatestLimit | FullQuery | BackwardWalker + BeforeCount | 仍需 latest probe |
| SinceLatest | FullQuery | latest probe + ForwardWalker | 仍需 latest probe |

Latest 相关请求必须确定真实 latest anchor。仅凭本地最大 timestamp 不能证明缓存仍然最新。

---

## 5. ForwardWalker

用于：

- SinceLimit；
- SinceLatest 固定 latest anchor 之后的正向遍历。

核心循环：

```text
从 cursor 开始

如果 cursor 落入 proven span：
    读取并复用实际 rows

如果 cursor 落入缺口：
    生成 AfterCount
    根据实际结果推进 cursor

如果结果接上下一 proven span：
    继续复用该 span

满足 count 或到达 latest anchor：
    停止
```

### 5.1 SinceLimit 终止条件

```text
最终实际 rows 数量达到 intent.limit
```

### 5.2 SinceLatest 终止条件

先执行：

```python
BeforeCount(anchor=None, count=1)
```

固定 `latest_anchor`，然后 ForwardWalker 直到实际 callback result 包含该 anchor。新产生的更晚 K 线不属于本次请求。

---

## 6. BackwardWalker

用于 LatestLimit。

```text
从真实 latest anchor 向 timestamp 减小方向遍历

读取当前 proven span
遇到缺口时生成 BeforeCount
接上更早 proven span 后继续复用
收集到 intent.limit 根后停止
最终恢复升序
```

不要先从缓存最大 timestamp 一直抓到 latest，只为计算二者之间是否超过 N 根。正确策略是从 latest 反向收集，达到 N 根立即停止。

如果缓存尾部非常旧，LatestLimit 可能完全由 Provider 返回的最新 N 根满足，旧缓存无需访问。

---

## 7. 中间缺口处理

缓存：

```text
[20------30]      [40------50]
```

正向遍历时：

```python
AfterCount(
    anchor=30,
    count=estimated_count,
)
```

判断：

- 实际结果包含 40：缺口桥接成功；
- 未包含 40 且 Query 尚未满足：从 callback 最后一根继续补；
- 去重后无新 timestamp：NO_PROGRESS；
- Provider 返回 UNAVAILABLE：不能建立 proof；
- Provider 返回 EXHAUSTED，但右侧已有缓存 40：数据源与缓存冲突，不能静默合并。

反向遍历可以对称使用 BeforeCount。

完成证明只来自实际 anchor overlap，不来自 interval 估算。

---

## 8. Network data 与持久化

一次 resolve 可能调用多个 FetchOperation。CacheEngine 统一累计：

```python
network_data = merge(network_data, fetch_result.data)
```

所有片段处理完成后：

```python
cache_data = network_data[:-1]
```

只执行一次，不对每个 FetchOperation 单独删除尾部。

最终响应：

```python
response = merged_result

if not intent.include_last:
    response = response[:-1]
```

proof 记录的是经过实际 overlap 验证的 fetch chain。对于中间缺口，右侧 anchor 可能已经存在于稳定缓存，因此 proof 范围不必等同于“本轮新写入行”的 min/max；proof 生成应与物理新增行数解耦。

---

## 9. Lock boundary

网络操作不得在 CacheStore 文件锁内执行。

```text
加锁：读取 proof、rows、统计并生成 plan
解锁
执行 FetchOperation
加锁：重新检查状态、merge、commit
解锁
```

commit 以 `time` 主键 upsert，保持幂等。后续可增加 per-series singleflight，但不得重新用文件锁包住长网络请求。

---

## 10. FetchResult 校验

CacheEngine 必须校验：

1. canonical schema 包含 time/OHLCV；
2. time 为 UTC milliseconds Int64；
3. time 严格升序且唯一；
4. high/low/open/close 关系合法；
5. volume 非负；
6. 价格允许为负；
7. AfterCount/BeforeCount 返回 inclusive anchor 或明确状态；
8. partial bridge 实际包含目标 cache anchor；
9. UNAVAILABLE 不生成 proof；
10. 空结果不生成虚假 proof；
11. 去重后无进展时停止。

---

## 11. 数据结构保留与修改

### 保留

- Parquet；
- 按时间分区；
- `time` 唯一键；
- `unique(keep="last")`；
- JSONL proof；
- overlap-based span compaction；
- FileLock，仅保护本地 IO。

### 修改

- `DataLocation` 泛化为 `SeriesKey`；
- 增加完整 proven span index；
- 增加方向化 `read_rows`；
- 移除 Cache 层 `MAX_PER_REQUEST`；
- timeframe 不再限制为 CCXT Literal；
- OHLC 允许负价格；
- proof 范围与本轮物理新增行解耦；
- 增加可重建的缓存统计能力。

未来如需 negative coverage，可再增加 `covered_start/covered_end`，不是第一阶段阻塞项。
