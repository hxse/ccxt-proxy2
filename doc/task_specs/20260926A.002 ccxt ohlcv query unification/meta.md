# 20260926A.002 CCXT OHLCV 查询统一

- 概括：复用单前缀与 since＋limit 分页，固定最新边界，统一普通行情尾根规则。
- 级别：三星；改变核心分页抽象及 latest-limit 的缓存行为。
- 影响范围：CcxtClient、OhlcvNetworkFetcher、相关路由契约、测试和示例。
- 主任务：20260926A。
- 前置任务：20260926A.001。
- 相关 current spec：doc/current_specs/ccxt_client.md、ccxt_ohlcv.md、ohlcv_cache_resolution.md、verification.md。
