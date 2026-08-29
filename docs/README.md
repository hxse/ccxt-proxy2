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
