# 已否决：通用多片段 Cache Resolver

> **Status: Rejected design. Do not implement.** 本文概括旧方案的边界和否决理由；逐篇原始设计与测试矩阵保存在 [`design_history`](../design_history/README.md)。

> 本文中的 `include_last` 仅是旧 RouteIntent 档案；当前公开和内部 OHLCV API 均已删除该参数。

## 1. 原问题

旧蓝图试图同时解决：

- 三类用户请求；
- 任意多段离散 cache；
- 中间缺口只请求缺失部分；
- Binance、Kraken、TQ 不同上游能力；
- 休盘/节假日的非连续 rows；
- 不使用 fixed interval 的 completeness proof。

其核心抽象是：

```text
RouteIntent × CacheTopology
        ↓
FetchOperation sequence
        ↓
Provider-specific API translation
```

## 2. RouteIntent

```python
SinceLimitIntent(series, since, limit, include_last=True)
LatestLimitIntent(series, limit, include_last=True)
SinceLatestIntent(series, since, include_last=True)
```

RouteIntent 表达最终用户目标，不表达缓存内部当前要补的缺口。旧方案要求三个 strongly typed cache entry 共享同一 engine。

## 3. FetchOperation

```python
FullQuery(intent)
AfterCount(series, anchor, count)
BeforeCount(series, anchor_or_none, count)
```

语义：

```text
AfterCount(anchor=30,count=100)
= 从 30 含首向时间增大方向取最多 100 根

BeforeCount(anchor=40,count=100)
= 截止 40 含首向时间减小方向取最多 100 根

BeforeCount(anchor=None,count=100)
= 最新倒数 100 根
```

Operation 不包含 `include_last`，因为内部 bridge 必须保留 overlap anchor。

## 4. FetchResult/status

```python
FetchResult(data, status)

COMPLETE  # 数量/anchor 目标已满足
EXHAUSTED  # Provider 权威确认到达边界
UNAVAILABLE  # Provider API 无法访问该 anchor/range
```

`UNAVAILABLE` 绝不能解释为“区间没有 K 线”。`NO_PROGRESS` 由 CacheEngine 在去重后无新 timestamp 时检测。

## 5. Source/callback protocol

旧方案不传多个裸 callback，而传 capability object：

```python
class OhlcvSource(Protocol):
    def execute(self, operation: FetchOperation) -> FetchResult: ...
```

Provider 内部 dispatcher 将 `FullQuery/AfterCount/BeforeCount` 翻译成原始 API。CacheEngine 不关心这个 translation，Provider 也不读 CacheStore。

该协议的出发点正确：零 cache 时需要“完整 Route fetch”，有片段时需要“某方向补若干根”，两者未必是同一 Provider API 能力。

## 6. Zero-cache 与 fragmented-cache

```text
零 cache:
SinceLimit   → FullQuery
LatestLimit  → FullQuery
SinceLatest → FullQuery

有片段:
向后缺口 → AfterCount
向前缺口 → BeforeCount
latest probe → BeforeCount(anchor=None,count=1)
```

`FullQuery` 也可用于 `enable_cache=false` 或 cache 过于碎片时的整体 fallback。

## 7. Proven spans

假设：

```text
[20------30]      [40------50]
```

`30→40` 是 unproven gap，不得猜测是休盘或有数据。Span 内部可以是：

```text
20,21,22,26,30
```

因为同一 fetch chain 已证明这是完整序列。Proof compaction 只合并 overlap/contain/actual-anchor bridge，不用 interval adjacency。

## 8. ForwardWalker

用于 `SinceLimit` 以及已固定 latest anchor 的 `SinceLatest`：

```text
cursor 落在 proven span → 读取实际 rows
cursor 落在 gap         → AfterCount
实际结果包含右侧 anchor → bridge 成功，复用下一 span
未达 count/anchor       → 从新 tail 继续
```

`SinceLimit` 收集到 N 根即停。`SinceLatest` 先做 latest probe，然后走到实际包含 fixed anchor。

## 9. BackwardWalker

用于 `LatestLimit`：

```text
从真实 latest anchor 向过去收集
命中 span → 倒序读取
遇缺口 → BeforeCount
收集 N 根 → 停止并恢复升序
```

它不应从一个很旧 cache tail 一直抓到 latest，而是从 latest 向后计数，必要时完全忽略旧 cache。

## 10. Gap bridge 冲突

对 `[20,30]` 与 `[40,50]`，正向 bridge 请求从 30 含首开始：

- 结果实际包含 40：bridge 成功；
- 未包含 40 且未满足 Route：扩大并继续；
- 去重无进展：停止；
- `UNAVAILABLE`：不建 proof；
- `EXHAUSTED` 但右侧 cache 40 存在：Provider 与 cache 冲突，不静默合并。

反向 bridge 对称使用 `BeforeCount`。

## 11. Provider translation

### Binance

```text
AfterCount  → since + limit + 手动 overlap pagination
BeforeCount → endTime/until + limit + 反向 pagination
```

### TQ

```text
选 data_length
→ 读 realtime serial
→ 过滤 anchor/验证 count
→ 不足逐级扩大到 10000
→ 仍不包 anchor 则 UNAVAILABLE
```

### Range API

可以用 interval 估算第一个 time window，但必须以实际 rows/anchor 验证，估算不生成 proof。

## 12. Store 与落盘

旧 target `CacheStore` 只做 Parquet/proof/local lock，提供 asc/desc range read、write/upsert 和 proven-span index。Resolver 累积整次 resolve 的 network data，最终只排除整体 network tail，不在每个 operation 删尾。`include_last` 只在最终 response 机械执行一次。

网络不得在 Store FileLock 内运行：

```text
lock: read proof/rows and plan
unlock: execute Provider operation
lock: recheck + idempotent commit
```

## 13. 否决理由

1. RouteIntent、FetchOperation、FetchStatus 和 Provider adapter 组成了一套新 framework。
2. 任意多缺口需要方向化 read、bridge conflict、proof planning 和双向 walker。
3. TQ/Binance 为了适配逻辑 operation 还要各自建 estimator。
4. 实际业务主要是从起点向后读取，single-prefix 已覆盖高价值部分。
5. 个人项目无法合理承担通用金融时序 cache framework 的长期维护。

现行 target 保留有价值的原则：Provider/cache 解耦、inclusive overlap、不猜 interval、Provider capability 显式报错。但删除 callback protocol、walkers、多缺口规划与 estimator。
