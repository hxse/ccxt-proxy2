# 20260927A CCXT 订单请求与回执修复

- 概括：修复 Kraken Futures IOC/FOK 编码、未知订单字段响应和布尔值数量/杠杆输入。
- 级别：二星；涉及交易请求、真实 SDK 编解码与 HTTP 响应的回归验证。
- 影响范围：CcxtClient 交易边界、CCXT 请求与响应类型、离线测试、现行接口规范。
- 主任务：无。
- 前置任务：20260924B、20260926A.008；基于当前集成版本修复，不改写已发布价格任务。
- 相关 current spec：doc/current_specs/ccxt_client.md、doc/current_specs/verification.md。
