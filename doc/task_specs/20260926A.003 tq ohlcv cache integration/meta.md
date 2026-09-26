# 20260926A.003 TQ 普通行情接入缓存

- 概括：单合约最新窗口复用共享缓存，补充更长连续历史，并实现真实休市状态兜底。
- 级别：三星；涉及 SDK 序列、数据资格、状态分支及等待退出。
- 影响范围：TQ 行情路由、请求类型、业务编排、SDK 获取与清洗、相关测试。
- 主任务：20260926A。
- 前置任务：20260926A.001；版本栈继承已统一的 CCXT 查询契约。
- 相关 current spec：doc/current_specs/tq_data.md、tq_processing.md、market_data_contract.md、verification.md。
