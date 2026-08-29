# 已否决：中位数密度估算器

> **Status: Rejected design. Do not implement.** 这是通用多片段 Resolver 的性能优化构想，不是当前 target 的正确性组件；完整早期草案见 [estimator design history](../design_history/06_median_estimator_rejected.md)。

## 1. 动机

在非连续市场中，时间区间长度不能直接推出 K 线数量。旧方案试图利用已证明 cache 的密度，预估第一次 `AfterCount/BeforeCount` 大小，然后不足再补拉。

估算只为减少 Provider round trips，从不证明连续性或 completeness。

## 2. 统计对象

统计“固定 wall-clock sampling window 内的实际 row count”中位数，而不是相邻 K 线时差中位数。

原候选：

```python
sampling_window = max(
    7 * DAY,
    32 * interval_hint,
)
```

它让分钟/小时数据至少覆盖一个周末。样本：

```text
[1180, 1125, 1142, 980, 1138]
median_count = 1138
```

对 gap duration 的首次估算：

```python
estimated_count = ceil(gap_duration / sampling_window * median_count)
```

## 3. 只能统计 proven windows

对：

```text
Proof A [20,30]
Gap     (30,40)
Proof B [40,50]
```

禁止将 `[20,50]` 作为完整统计窗口。样本只能完整位于单个 proven span/fetch chain 中。Span 内已经证明的周末、节假日、午休和停牌正常参与统计。

## 4. 回退层级

```text
1. 当前品种 + 当前周期
2. 同 cohort 其他品种
3. 默认估算/probe
4. 实际拉取后 adaptive refill
```

### 当前品种

原建议至少 3 个近期完整 windows，对 count 取 median。

### Cohort

Cohort identity 至少包含：

```text
provider / environment / market / timeframe / variant/adjustment
```

不得把 Binance crypto 1m 与 TQ futures 1m 混用。为避免长历史品种占过高权重，原方案用两级 median：先对每个 symbol 取 median，再对 symbol medians 取 cohort median。

### 默认估算

左右 anchor 与 interval 可换算时：

```python
ceil((right_anchor - left_anchor) / interval) + overlap
```

这仍只是 request-count hint。无法稳定换算的 `1M` 等周期使用 `DEFAULT_PROBE_COUNT=128`，然后 `128→256→512→...`。只有数量目标时可直接请求 `remaining + overlap`。

## 5. Adaptive refill

```text
Estimate
→ FetchOperation
→ validate actual rows/count/anchor
→ target reached?
    ├─ yes: stop
    └─ no : 扩大 count 后继续
```

原候选：

```python
next_count = max(previous_count * 2, returned_count * 2)
```

只有以下情况可完成：

- 实际收集到 Route 需要的 rows；
- 实际 result 包含目标 cache/latest anchor；
- Provider 权威返回 `EXHAUSTED`。

`UNAVAILABLE`、时间估算或 interval slot 数不能生成 proof。

## 6. 三类场景的原估算

### SinceLimit

对 `10→20`、`30→40` 等左右有 anchor 的 gap 估算首次 `AfterCount`；实际碰到右 anchor 后复用下一 span。最后无右 anchor 的部分直接用 `remaining+overlap`。

### LatestLimit

先用 `BeforeCount(None,1)` 固定 latest，再估算 cache tail 到 latest 的 rows。若估计已超 N，直接从 latest 反向取 N，不连旧 cache；否则尝试反向 bridge。

### SinceLatest

先固定 latest anchor，再用 ForwardWalker 依次处理 start gap、已有 spans、中间 gaps 和 cache-tail-to-latest。

## 7. 两层 estimator

- Cache-level：根据 proven medians、anchors、remaining 和上一次实际结果生成下一个逻辑 count。
- Provider-level：将逻辑 count 翻译为 Binance since/endTime、TQ data_length 或 range API time window。

两层都要“估算→实际拉取→验证→补拉”；Provider estimator 不读 CacheStore，Cache estimator 不理解原始 API params。

## 8. 统计存储构想

第一版原本计划从 proven spans 即时计算并做进程内 memoization；新数据写入使统计失效。只有性能证明必要时才建可重建 `series_stats.json`，它不承担 proof。

## 9. 否决理由

- 它必须依赖已被否决的多片段 Resolver/proven-span index。
- 估算错了仍要 adaptive refill，因此对正确性没有贡献。
- Cohort、sampling window、invalidations 和 Provider translation 会显著扩大测试矩阵。
- 当前 single-prefix 场景可以直接按 `remaining + overlap + successor` 拉取，不需要统计 framework。

目标方案中不存在 estimator module、stats sidecar 或 cohort config。
