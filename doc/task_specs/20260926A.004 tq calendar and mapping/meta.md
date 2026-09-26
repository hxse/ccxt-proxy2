# 20260926A.004 TQ 日历与主连映射迁移

- 概括：提取官方元数据转换，自主管理源刷新与可信时间，缓存日期序列并返回真实换月节点。
- 级别：三星；涉及源等价性、日期覆盖、历史上下文和旧入口硬禁。
- 影响范围：TQ 元数据获取、缓存辅助类型、路由与响应、SDK 工厂、架构和等价性测试。
- 主任务：20260926A。
- 前置任务：20260926A.001、20260926A.003。
- 相关 current spec：doc/current_specs/tq_data.md、tq_processing.md、system_time.md、ohlcv_cache_storage.md、ohlcv_cache_operations.md、verification.md。
