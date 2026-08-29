# 多片段缓存与中位数预估算法

> **Status: Rejected design archive. Do not implement.** 本文从早期 `todo/` 完整迁入正式文档区，只保留历史讨论；现行规范以 [`docs/README.md`](../README.md) 标记的 Implemented design 为准。


> **文档状态：Ideas archive，非实施计划。** 这是一个未来可能考虑、但由于复杂度过高而更可能不会实施的 TODO；本文仅保留设计思路，不代表当前架构决策或开发排期。

## 1. 目标

CacheEngine 面向如下状态设计：

```text
缓存：[20------30]      [40------50]
缺口：10→20、30→40、50→latest
```

算法必须：

- 复用所有有价值的 proven spans；
- 只请求缺失部分；
- 不假设 K 线连续；
- 通过缓存统计改善第一次 FetchOperation 的 count；
- 估算不足时继续补拉；
- 只以实际 count、timestamp 和 overlap 判断完成。

---

## 2. 统计对象

统计“固定 wall-clock sampling window 内实际 K 线数量”的中位数。

不统计相邻 K 线时间差中位数。期货在交易时段内的相邻间隔可能一直是 1m，但并不表示市场 24×7 连续。

建议 sampling window：

```python
sampling_window = max(
    7 * DAY,
    32 * interval_hint,
)
```

这使分钟/小时数据至少覆盖完整周末，日线及更长周期拥有足够样本。

示例：

```text
最近多个完整 proven windows 的 row count：
[1180, 1125, 1142, 980, 1138]

median_count = 1138
```

估算缺口：

```python
estimated_count = ceil(gap_duration / sampling_window * median_count)
```

估算只用于决定第一次 AfterCount/BeforeCount 的 count。

---

## 3. 只能统计 proven windows

```text
ProofSpan A：[20,30]
Unproven gap：(30,40)
ProofSpan B：[40,50]
```

禁止将 `[20,50]` 作为完整统计样本，因为 `30→40` 可能只是从未缓存。

统计窗口必须：

- 完整位于一个 proven span 内；或
- 已通过 fetch chain 证明完整覆盖。

proven span 内部真实存在的周末、节假日、午休和停牌正常进入统计。

---

## 4. 统计回退层级

```text
1. 当前品种 + 当前周期
2. 同 cohort 的其他品种
3. 默认估算
4. 实际抓取后不足则自适应补拉
```

### 4.1 当前品种

建议至少需要：

```python
MIN_STAT_WINDOWS = 3
```

只使用近期完整 proven windows，取 row count median。

### 4.2 同 cohort 其他品种

cohort 至少包含：

```text
provider
environment
market
timeframe
variant/adjustment
```

不能把 Binance crypto 1m 与 TQ futures 1m 混为一组。

为避免历史较长品种占据过高权重，使用两级中位数：

```python
symbol_medians = [
    median(symbol_a_windows),
    median(symbol_b_windows),
    median(symbol_c_windows),
]

cohort_median = median(symbol_medians)
```

每个品种只贡献一个 median。

### 4.3 默认估算

有左右时间 anchor 且 interval 可换算时：

```python
estimated_count = ceil((right_anchor - left_anchor) / interval) + overlap
```

这是请求数量 hint，不是连续性证明。休盘只会使实际 rows 更少。

`1M` 等无法稳定换算时使用默认 probe：

```python
DEFAULT_PROBE_COUNT = 128
```

然后：

```text
128 → 256 → 512 → 1024 → ...
```

只有数量目标时直接请求 `remaining + overlap`，不需要密度估算。

---

## 5. Adaptive refill

统一反馈循环：

```text
Estimate
→ FetchOperation
→ Validate actual result
→ 达到 count/anchor？
    ├── Yes：完成
    └── No：更新估算并补拉
```

示例：

```python
class AdaptiveRefill:
    def next_count(
        self,
        previous_count: int,
        returned_count: int,
        target_reached: bool,
    ) -> int:
        if target_reached:
            return 0
        return max(
            previous_count * 2,
            returned_count * 2,
        )
```

完成证明只能来自：

- 实际收集到 RouteIntent 所需数量；
- 实际结果包含目标 cache/latest anchor；
- Provider 明确返回 EXHAUSTED。

UNAVAILABLE、时间估算或 interval slot 数都不能建立 proof。

---

## 6. 场景一：SinceLimit

```text
Route：从 10 开始取 100 根
缓存：[20,30]、[40,50]
```

### 10→20

```python
estimated_gap = estimator.estimate_between(10, 20)

request_count = min(
    remaining + overlap,
    estimated_gap + overlap,
)

AfterCount(anchor=10, count=request_count)
```

判断：

- 已收集 100 根：立即停止；
- 实际结果包含 20：复用 `[20,30]`；
- 未达到数量且未包含 20：扩大 count 继续补。

### 30→40

同样用中位数估算并调用 AfterCount，实际碰到 40 后复用 `[40,50]`。

### 50 之后

没有右侧缓存 anchor，只剩数量目标：

```python
AfterCount(
    anchor=50,
    count=remaining + overlap,
)
```

---

## 7. 场景二：LatestLimit

```text
Route：最新倒数 100 根
缓存尾：50
```

先确定真实 latest：

```python
BeforeCount(anchor=None, count=1)
```

假设 latest anchor 为 90，然后估算：

```python
estimated_tail = estimator.estimate_between(50, 90)
```

### 估计大于等于 100

缓存 50 大概率不属于最新 100 根：

```python
BeforeCount(anchor=None, count=100 + overlap)
```

实际得到 100 根后停止，无需强行连接旧缓存。

### 估计小于 100

尝试反向抓到 cache tail：

```python
BeforeCount(
    anchor=90,
    count=estimated_tail + overlap,
)
```

- 实际结果包含 50：接上缓存并继续反向复用；
- 已经收集 100 根：直接完成；
- 未到 50 且不足 100：扩大 count 继续补拉。

最终算法仍然从 latest 向过去计数，不会从很旧的 cache tail 一直向最新抓取。

---

## 8. 场景三：SinceLatest

```text
Route：从 10 抓到最新
缓存：[20,30]、[40,50]
```

先固定：

```python
latest_anchor = BeforeCount(
    anchor=None,
    count=1,
).data[-1]["time"]
```

随后 ForwardWalker 依次处理：

```text
估算并抓取 10→20
复用 20→30
估算并抓取 30→40
复用 40→50
估算并抓取 50→latest_anchor
```

最后一段若未实际包含 latest anchor，则扩大 AfterCount 继续补拉。抓取期间新产生的更晚 K 线被裁掉。

---

## 9. 两层 Estimator

### 9.1 Cache-level Estimator

输入：

- proven cache median；
- 左右 anchor；
- interval hint；
- remaining count；
- 上一次 FetchOperation 的实际结果。

输出：

```text
下一次 AfterCount/BeforeCount 的逻辑 count
```

### 9.2 Provider-level Estimator

负责将逻辑 count 翻译为具体 API：

- Binance：since/endTime + limit + 自分页；
- TQ：data_length 逐级扩大；
- range API：估算时间 window，不足后扩大。

Provider-level Estimator 不读取 CacheStore；Cache-level Estimator 不知道原始 API 参数。

两层都必须执行“估算 → 抓取 → 实际验证 → 补拉”。

---

## 10. 统计存储

第一版可以不新增持久文件：

- 从 proven spans 计算；
- 进程内 memoize；
- 当前 SeriesKey 写入新数据后使统计失效；
- 下次需要时重算。

如果扫描成本明显，再增加可重建的 `series_stats.json`。它只用于性能优化，不承担 proof，丢失后可安全重建。
