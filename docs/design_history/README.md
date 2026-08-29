# 已否决设计档案

> **Status: Rejected design archive. Do not implement.** 本目录用于保证删除 `todo/` 后仍能追溯早期讨论，不属于当前 roadmap。

这些文档记录了一个更通用、也更复杂的设想：用 `RouteIntent × CacheTopology` 产生 `FetchOperation`，再通过 Provider callback/capability object 填补任意 cache 缺口。该方向后来因实现与维护成本过高而被明确否决。

档案包括：

1. [通用 Provider 架构](01_generic_provider_architecture_rejected.md)
2. [RouteIntent、FetchOperation 与 callback protocol](02_query_callback_protocol_rejected.md)
3. [多片段 cache resolver](03_fragmented_cache_resolver_rejected.md)
4. [TQ 对称 cached Route 与代码收口](04_tq_cached_routes_rejected.md)
5. [旧测试、迁移与验收计划](05_generic_migration_plan_rejected.md)
6. [中位数密度 estimator](06_median_estimator_rejected.md)

当前实施依据是：

- [目标架构](../architecture/01_target_architecture.md)
- [CCXT OHLCV Route contract](../ccxt/02_ohlcv_routes.md)
- [DuckDB cache 数据模型](../cache_tool/01_design.md)
- [简化 single-prefix 算法](../cache_tool/04_cache_algorithm_simple.md)

如档案与 Implemented design 冲突，Implemented design 无条件优先。档案中的 Parquet、proof log、callback、walker、estimator 与 TQ cache 均不得被误当成待实施组件。

档案中出现的 `include_last` 也是已删除的历史 RouteIntent 字段。当前 Route 始终返回完整 rows，只通过 `last_bar_completion_confirmed` 描述尾根证据。
