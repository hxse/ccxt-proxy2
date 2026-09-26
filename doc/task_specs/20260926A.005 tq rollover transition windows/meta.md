# 20260926A.005 换月过渡窗口

- 概括：在真实换月节点附带指定周期的旧合约价格，按单次完整窗口保存最长可信前缀。
- 级别：三星；涉及窗口起点、单次证明、可变长度和并发替换。
- 影响范围：TQ 映射路由、旧合约 SDK 获取、过渡缓存类型及相关验证。
- 主任务：20260926A。
- 前置任务：20260926A.001、20260926A.003、20260926A.004。
- 相关 current spec：doc/current_specs/tq_data.md、tq_processing.md、ohlcv_cache_storage.md、ohlcv_cache_operations.md、verification.md。
