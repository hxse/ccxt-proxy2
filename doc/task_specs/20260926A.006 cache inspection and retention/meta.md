# 20260926A.006 缓存概况与保留清理

- 概括：公开本地连续片段概况与按请求规则维护的清理入口，原子维护全部缓存类型。
- 级别：三星；涉及删除、覆盖证明、辅助上下文及并发。
- 影响范围：缓存高级维护 API、HTTP 查询/清理、响应类型与相关测试。
- 主任务：20260926A。
- 前置任务：20260926A.001、20260926A.003、20260926A.004、20260926A.005。
- 相关 current spec：doc/current_specs/ohlcv_cache_storage.md、ohlcv_cache_operations.md、configuration.md、system_time.md、verification.md。
