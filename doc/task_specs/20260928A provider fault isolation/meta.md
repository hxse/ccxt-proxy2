# 20260928A SDK 故障隔离

- 概括：按服务身份独立初始化和判定可用性，单个 SDK 故障不阻断 HTTP 或关闭其他实例。
- 级别：三星；涉及并发生命周期、资源所有权、统一错误及就绪契约。
- 影响范围：ServiceRuntime、各 Provider 入口、健康检查、后台调度、相关规范与离线测试。
- 主任务：无。
- 前置任务：20260927B。
- 相关 current spec：doc/current_specs/configuration.md、doc/current_specs/architecture.md、doc/current_specs/verification.md。
