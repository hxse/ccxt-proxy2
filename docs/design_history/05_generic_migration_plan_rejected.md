# 测试、迁移与验收

> **Status: Rejected design archive. Do not implement.** 本文从早期 `todo/` 完整迁入正式文档区，只保留历史讨论；现行规范以 [`docs/README.md`](../README.md) 标记的 Implemented design 为准。


> **文档状态：Ideas archive，非实施计划。** 这是一个未来可能考虑、但由于复杂度过高而更可能不会实施的 TODO；本文仅保留设计思路，不代表当前架构决策或开发排期。

## 1. Test strategy

### 1.1 Provider Client

使用 mock 原始 API response，验证：

- FullQuery 三种 RouteIntent；
- AfterCount inclusive anchor；
- BeforeCount inclusive anchor/latest；
- Provider 内部首尾重叠分页；
- duplicate time 保留新值；
- empty/no-progress 正确退出；
- Spot/Future page limit；
- TQ adaptive data_length；
- COMPLETE/EXHAUSTED/UNAVAILABLE 区分；
- read-only retry；
- 非只读操作不自动重试；
- 交易异常记录后向上抛出，服务继续运行。

### 1.2 CacheEngine 路由矩阵

每个 RouteIntent 覆盖：

- 零缓存；
- 完全缓存；
- 只有前缀；
- 只有后缀；
- 一个中间缺口；
- 多个中间缺口；
- 起点位于 unproven gap；
- cache tail 远离 latest；
- 缓存碎片过多触发 FullQuery fallback。

### 1.3 Walkers

- SinceLimit 使用 ForwardWalker，达到实际 N 根即停止；
- SinceLatest 先固定 latest anchor，再正向遍历；
- LatestLimit 从真实 latest 反向收集，达到 N 根即停止；
- 中间 gap 实际碰到右/左 anchor 后复用下一 span；
- 不使用 interval adjacency；
- 周末、节假日、午休和停牌跳跃保持合法；
- UNAVAILABLE 不生成 proof；
- EXHAUSTED 与已有远端 cache anchor 冲突时拒绝静默合并。

### 1.4 CacheEstimator

- 当前品种统计优先；
- 当前品种样本不足时使用同 cohort 品种；
- cohort 无样本时使用默认估算；
- unproven gap 不进入统计；
- proven span 内自然休盘进入统计；
- 中位数估算不足后自适应补拉；
- 估算不能直接生成 proof；
- stats 丢失时可重建。

### 1.5 CacheStore

- asc/desc read_rows；
- cross-partition reverse read；
- multiple proof spans；
- proof compaction 保留真实缺口；
- duplicate upsert；
- corrupted proof；
- concurrent commit；
- proof range 与新增 row range 解耦；
- 现有 Binance cache path migration。

### 1.6 Router

- 三条 route 构造正确 RouteIntent；
- 三条 route 调用正确 CacheEngine 入口；
- enable_cache=False 使用 FullQuery；
- include_last 最终只执行一次；
- capability error 正确映射；
- response serialization 兼容。

---

## 2. Migration plan

### Phase 1：Domain contract

1. 新增 SeriesKey；
2. 新增三种 RouteIntent；
3. 新增 FullQuery/AfterCount/BeforeCount；
4. 新增 FetchStatus/FetchResult；
5. 固定 include_last 的机械语义。

### Phase 2：Provider Client 收口

1. 创建 BinanceClient；
2. 实现三个 FullQuery route fetch；
3. 实现 AfterCount/BeforeCount；
4. 只为 read-only 实现 retry；
5. 创建对称 KrakenClient；
6. ProviderRegistry 返回 Client；
7. Route 切换后删除旧 utils/adapter。

### Phase 3：CacheStore

1. 将现有 Parquet/proof 提取为纯本地 Store；
2. 增加 get_proven_spans；
3. 增加方向化 read_rows；
4. proof 与物理新增行范围解耦；
5. 移除 Cache 层 Provider page limit。

### Phase 4：CacheEngine

1. 实现三个公开入口；
2. 实现 ForwardWalker；
3. 实现 BackwardWalker；
4. 支持任意多个缓存片段；
5. 零缓存使用 FullQuery；
6. 多片段使用 AfterCount/BeforeCount；
7. 实现最终统一 include_last 和 network tail 写入策略。

### Phase 5：Estimator

1. 从 proven spans 生成当前品种窗口中位数；
2. 增加同 cohort fallback；
3. 增加默认估算和 adaptive refill；
4. 首先使用进程内 memoization；
5. 性能需要时再增加可重建 stats sidecar。

### Phase 6：TQ

1. TqManager 收口为 TqClient；
2. canonical OHLCV normalization；
3. 实现 Latest FullQuery；
4. 实现 serial-window AfterCount/BeforeCount；
5. 超出窗口返回 UNAVAILABLE；
6. 保留 raw thin-forward routes。

### Phase 7：数据迁移

1. DataLocation 迁移为 SeriesKey；
2. TQ adj_type/Binance variant 纳入 identity；
3. 兼容或迁移现有 Parquet；
4. 增加 schema version。

---

## 3. Acceptance criteria

1. CacheStore 不 import CCXT/TQSDK/FastAPI；
2. Cache package 不包含 Provider page limit；
3. RouteIntent 和 FetchOperation 为不同类型；
4. CacheEngine 有三个明确公开入口，但共享一套实现；
5. 零缓存使用 FullQuery；
6. 片段缓存由 CacheEngine 选择 AfterCount/BeforeCount；
7. 支持任意多个 proven spans 和中间缺口；
8. 完全命中的 SinceLimit 不调用 Provider；
9. Latest 相关请求固定真实 latest anchor；
10. include_last 只在最终结果机械执行一次；
11. network data 只在整个 resolve 完成后统一去尾持久化；
12. 不使用固定 interval 证明连续性；
13. 统计只使用 proven windows；
14. 统计不足时按当前品种、同 cohort、默认估算回退；
15. 所有估算结果都通过实际 count/timestamp/anchor 验证；
16. UNAVAILABLE 不被解释为无数据；
17. Binance/Kraken Route 不再经过旧 utils/adapter；
18. 非只读操作第一阶段不自动重试；
19. TQ 不将超出 serial window 的部分数据伪装成完整结果；
20. 旧实现迁移后彻底删除。

---

## 4. 最终结论

```text
RouteIntent
    × CacheTopology
        ↓
FetchOperation sequence
        ↓
Provider-specific API translation / estimation
        ↓
实际 rows、count 和 overlap validation
```

三个完整 Route fetch 能力服务于冷缓存和 bypass；AfterCount/BeforeCount 服务于片段缓存。CacheEstimator 只提高第一次请求的准确度，不能参与 proof。系统正确性最终只依赖实际数据和实际 anchor overlap。
