# ccxt-proxy2 重构方案（草稿）

> **状态：Ideas archive / Future TODO（未排期）**
>
> 本目录仅用于备份此前讨论过的重构思路，不代表已经批准的实施方案，也不属于当前 roadmap。由于该方案涉及跨 Provider 查询语义、多片段缓存、非连续市场数据证明、分页和一致性校验，设计与长期维护成本都很高，**未来更可能不会实施，而不是会实施**。
>
> 保留这些文档只是为了避免讨论成果丢失。任何后续开发都不应默认按本方案执行；如果未来重新考虑，必须先重新评估现成 library、各 Provider 的原生缓存能力、实际业务需求和可接受的 scope，再决定是局部采用、显著删减还是彻底放弃。

本目录将重构方案按职责拆分为以下专题文档：

1. [总体架构与 Provider Client](01_architecture_and_provider_clients.md)
2. [RouteIntent、FetchOperation 与 Callback 协议](02_ohlcv_query_and_callback_protocol.md)
3. [通用缓存系统](03_generic_cache_system.md)
4. [TQ、Route 与代码收口](04_tq_routes_and_code_consolidation.md)
5. [测试、迁移与验收](05_testing_migration_and_acceptance.md)
6. [多片段缓存与中位数预估算法](06_fragmented_cache_and_median_estimation.md)

核心结论：`RouteIntent` 描述用户最终需求，`CacheEngine` 根据 RouteIntent 和缓存拓扑生成 `FetchOperation`，Provider Client 负责把 FetchOperation 转换成具体 API 请求。CacheStore 只负责本地文件 IO 和 proof，不包含 Provider 或网络知识。
