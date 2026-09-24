# 20260924B 委托价格精度与边界

- 概括：由后端统一处理 CCXT 与 CTP 委托报价的步长、价格边界和拒单原因。
- 级别：三星；改变交易报价处理契约，涉及多个交易所及 CTP 原生回调。
- 影响范围：CCXT/CTP 客户端、价格响应、错误映射、离线测试和接口文档。
- 主任务：无。
- 前置任务：无。
- 相关 current spec：doc/current_specs/ccxt_client.md、doc/current_specs/ctp_trading.md。
