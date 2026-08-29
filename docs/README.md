# ccxt-proxy2 文档索引

## 文档标记

- **Implemented**：已实现且应与当前代码一致。
- **Implemented design**：已经落地并应与当前代码一致。
- **Target design**：仅用于尚未落地的后续规范；当前 CCXT/DuckDB 主方案已完成。
- **Research snapshot**：人工或沙盒实测记录，不是稳定 API contract。
- **Rejected design**：仅保留决策背景，不得作为实施依据。

## 当前架构

当前正式支持面是 Binance USDⓈ-M linear Futures 和 Kraken Futures。Spot 只做 best-effort 暴露；Binance COIN-M/inverse 与 Kraken Spot sandbox 不支持。

1. [总体目标架构](architecture/01_target_architecture.md)
2. [破坏性迁移、测试与验收](architecture/02_migration_and_acceptance.md)
3. [CcxtClient 与 Provider capability](ccxt/01_client_architecture.md)
4. [CCXT OHLCV 三类 Route 与 network contract](ccxt/02_ohlcv_routes.md)

## DuckDB OHLCV cache

1. [方案对比与决策记录](cache_tool/00_comparison.md)
2. [DuckDB cache 数据模型](cache_tool/01_design.md)
3. [Cache API、transaction、capacity 与 concurrency](cache_tool/02_core_code.md)
4. [测试规范](cache_tool/03_test_code.md)
5. [三类 Route 的简化缓存算法](cache_tool/04_cache_algorithm_simple.md)
6. [已否决：通用多片段 Resolver](cache_tool/05_cache_algorithm_deprecated.md)
7. [已否决：中位数密度估算器](cache_tool/06_estimator_deprecated.md)
8. [已删除：旧 Parquet/proof-log 实现档案](cache_tool/07_legacy_parquet_cache.md)

## 其他能力

- [TQ 路由与 lifecycle](tq_data_source/01_design.md)
- [TQ Pandas 数据处理与测试](tq_data_source/02_data_processing_and_tests.md)
- [Telegram 消息转发](telegram/01_design.md)

## CCXT 研究记录

- [Binance/Kraken 市场数据对比](ccxt_research/market_data_comparison.md)
- [Binance/Kraken 订单行为对比](ccxt_research/order_behavior_comparison.md)
- [CCXT 下单规则](ccxt_research/order_rules.md)

## 已否决设计档案

- [档案索引与权威顺序](design_history/README.md)
- [通用 Provider 架构](design_history/01_generic_provider_architecture_rejected.md)
- [RouteIntent、FetchOperation 与 callback protocol](design_history/02_query_callback_protocol_rejected.md)
- [多片段 cache resolver](design_history/03_fragmented_cache_resolver_rejected.md)
- [TQ 对称 cached Route](design_history/04_tq_cached_routes_rejected.md)
- [旧测试与迁移计划](design_history/05_generic_migration_plan_rejected.md)
- [中位数密度 estimator](design_history/06_median_estimator_rejected.md)

## 草稿迁移覆盖

仓库内旧 `todo/` 草稿已经删除；以下映射记录其正式归档位置：

| 草稿内容 | 正式去向 |
| --- | --- |
| 最新 DuckDB 草稿 1 | target architecture、方案对比 |
| 最新 DuckDB 草稿 2 | CCXT OHLCV Route contract |
| 最新 DuckDB 草稿 3–4 | CcxtClient 与 Provider capability |
| 最新 DuckDB 草稿 5–6 | canonical OHLCV、DuckDB cache design |
| 最新 DuckDB 草稿 7–11 | cache public API、schema、segment 与命中语义 |
| 最新 DuckDB 草稿 12–15 | 三类简化算法、完整 response 与 completion metadata |
| 最新 DuckDB 草稿 16–18 | write transaction、concurrency、capacity/eviction |
| 最新 DuckDB 草稿 19 | TQ 两份正式文档 |
| 最新 DuckDB 草稿 20 | Route/cache error contract |
| 最新 DuckDB 草稿 21–23 | migration、tests、acceptance 与已固定实现项 |
| 旧草稿 Provider/Route/FetchOperation | `design_history/01–02` + target CcxtClient |
| 旧草稿多片段 CacheEngine | `design_history/03` + rejected resolver 摘要 |
| 旧草稿 TQ cached adapter | `design_history/04` + TQ 正式边界 |
| 旧草稿旧迁移与验收矩阵 | `design_history/05`；仅作历史记录 |
| 旧草稿中位数估算 | `design_history/06` + rejected estimator 摘要 |
| 旧 Parquet/proof 正式文档 | legacy Parquet archive |

如果正式文档与历史草稿冲突，以标记为 **Implemented design** 的正式文档为准。

`~/Downloads/todo/ccxt-proxy2草稿文档/` 中的 discussion draft 只是仓库外备份，不再同步、不属于项目文档或实施依据；可在确认不再需要后手工删除。
