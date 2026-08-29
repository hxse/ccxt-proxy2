# OHLCV Cache 方案对比与决策记录

> **Status: Implemented design + rejected-design archive.** DuckDB single-prefix 方案已经落地；其他方案只用于解释决策。

## 1. 最终选择

```text
CcxtClient
└── DuckDbOhlcvCache
    └── embedded DuckDB native file
```

核心特征：

- Cache 无 network、无 callback、无 Provider page。
- Read 最多复用一个最佳 prefix segment，不解多个中间缺口。
- Write 可原子合并所有实际 timestamp overlap 的 segments。
- Cache 只信任 Client 已验证的 fetch chain/overlap；Binance/Kraken Futures Client 额外使用 fixed interval 对异常 page fail-fast。
- 尾根只在观察到严格更晚 successor 时落盘。
- 物理存储、WAL、checkpoint、row group 交给 DuckDB。

## 2. 方案总表

| 方案 | 状态 | 决策 |
| --- | --- | --- |
| DuckDB + logical segment + single prefix | **Adopted target** | 实用语义与可维护性的平衡 |
| Parquet 时间分区 + JSONL proof log | Legacy, 已删除 | IO/proof/corruption 路径过多，与 network 耦合 |
| Parquet 按行数分片 `.1/.2` | Rejected | 需要手动分片、合并、rename 和 cleanup |
| 单一大 Parquet | Rejected | 反复重写，又无法单靠 rows 证明 fetch chain |
| `batch_id` 内嵌到 OHLCV | Rejected | 动态合并 identity，动态列污染 canonical schema |
| Callback-driven generic CacheEngine | Rejected | Route intent 与缺口 operation 协议过于复杂 |
| 多片段 Forward/Backward Walker | Rejected | 分支、proof 和 Provider 能力组合成本过高 |
| 中位数密度估算/adaptive refill | Rejected | 只减少请求次数，不提供正确性，维护不划算 |
| TQ 对称 cached Route | Rejected | TqSdk 自有 serial cache，当前外层重复缓存无必要 |

## 3. 为什么不继续 Parquet/proof log

旧设计将以下问题交给应用：

- 时间分区路径与大文件重写；
- Parquet 读写与 Polars execution-plan 性能依赖；
- JSONL proof 丢失/损坏/压缩；
- 文件锁、atomic rename 和数据/证明一致性；
- cache entry 同时决定 Provider page limit 和 network pagination。

DuckDB 用 transaction 和关系表替代物理文件编排。它不自动理解 segment/successor，但让应用只剩业务规则。

## 4. 为什么不做通用多片段 Resolver

旧蓝图将用户意图分为 `SinceLimit/LatestLimit/SinceLatest`，再由 CacheEngine 产生 `FullQuery/AfterCount/BeforeCount`，通过 callback/capability object 向 Binance、Kraken、TQ 投射。它还需要：

- 任意多 proven spans；
- ForwardWalker/BackwardWalker；
- 缺口 bridge 和 anchor conflict；
- Provider `COMPLETE/EXHAUSTED/UNAVAILABLE` 状态；
- 密度 estimator 和自适应补拉；
- proof 与物理新增 rows 解耦。

方案理论通用，但对当前实际请求成本过高。目标方案只在请求起点选一个最佳 prefix，进入 network 后不再读第二个 segment。详细历史保留在 [已否决 Resolver](05_cache_algorithm_deprecated.md) 和 [已否决 Estimator](06_estimator_deprecated.md)。

## 5. 关键否决项

- 不使用 CCXT automatic pagination；Client 手动做 inclusive overlap。
- 不以本机时间/timeframe 判断最后一根是否完成。
- 不在每个 Provider page 落盘 pending tail。
- 不建通用 `DuckDbStore`/DSL；parameterized SQL 封装在 cache class。
- 不开 DuckDB server；使用 embedded native file。
- 不关闭 WAL；WAL 是 crash recovery，不是历史备份。
- 不使用 `FileLock` 保护 CCXT/DuckDB；它们使用不同的 process-local lock。
- 不实现 LRU/last-access write amplification；capacity 按 row time 淘汰最旧前缀。
- 不保留旧 cache 兼容和旧 public facade。

## 6. 权威顺序

1. [目标架构](../architecture/01_target_architecture.md)
2. [OHLCV Route contract](../ccxt/02_ohlcv_routes.md)
3. [DuckDB cache 模型](01_design.md)
4. 本文的 rejected-design 记录

任何旧文档中的 Parquet/proof/callback 建议如与上述 target 冲突，均视为已否决。
