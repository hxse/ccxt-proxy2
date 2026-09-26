# 20260926A 行情缓存复用与定期维护

- 概括：统一行情缓存契约，完善现行规范，规划 CCXT 查询复用、TQ 历史积累、元数据、过渡与后台维护。
- 级别：三星；涉及连续性证明、公开接口、共享数据库所有权与跨模块迁移。
- 影响范围：行情查询、缓存、TQ 元数据、应用生命周期、后台配置、相关文档与验证。
- 主任务：无。
- 前置任务：无；基于现有订单价格规范之后的项目能力建立独立任务组。
- 相关 current spec：doc/current_specs/architecture.md、market_data_contract.md、ccxt_ohlcv.md、ohlcv_cache_storage.md、ohlcv_cache_operations.md、ohlcv_cache_resolution.md、tq_data.md、tq_processing.md、configuration.md、system_time.md、verification.md。
