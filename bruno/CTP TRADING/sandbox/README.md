# CTP 模拟盘 Bruno 场景测试

25 个请求，全部显式使用 `mode=sandbox`，读取服务端 `ctp.test`（SimNow/期货公司仿真账户）。原有 CTP 示例仍保留在父目录。

在 Bruno 打开项目的 `bruno` 集合，选择 `ccxt-proxy2` 环境，展开 **CTP TRADING → 模拟盘场景测试 (sandbox)**。
`user/password` 是本服务的登录账号；CTP 账号、密码、前置和认证码沿用服务端配置，不填进 Bruno。

| 目录 | 数量 | 内容 |
| --- | ---: | --- |
| 01_queries | 7 | 资金、全部/指定合约订单、成交、持仓 |
| 02_limit_orders | 8 | 开多、开空、平多/平空今仓、平多/平空昨仓、IOC、FOK |
| 03_market_orders | 4 | 开多、开空、平多、平空 |
| 04_cancel | 2 | OrderSysID 与原订单会话标识两种撤单 |
| 05_validation | 4 | 市价带价格、限价缺价格、非整数手数、混用撤单标识，预期 422 |

每个请求包含中文 Docs、状态码断言和 Tests 脚本。成功用例检查 sandbox、CTP 请求号、交易日和原生字段类型；查询允许空数组。验证失败用例检查 422 及对应字段错误，不会把上游拒单误当作参数校验通过。

## 使用顺序

1. 填写服务端 `ctp.test` 并启动服务；先查看 `01_queries`。
2. 在请求 Body/Params 中替换合约与价格。限价样例为 SHFE.rb2610 / 3500，市价样例为 DCE.m2701，都是格式示例。
3. 按需单独执行一个开仓请求，再查询订单、成交和持仓。HTTP 200 下单回报不代表已经成交。
4. 撤单时从响应复制原订单标识，保留 OrderSysID 前导空格。两种撤单用例任选其一。
5. 有对应可平持仓后再执行平仓用例；按交易所规则区分今昨仓。

下单和撤单会修改模拟账户状态，不要将整个 sandbox 目录当作自动开平仓流程一键执行。
原生市价是否支持由品种和前置决定；上游不支持时 200 成功断言应失败，保留错误供查看。

仅批量运行查询：

```bash
just bru-run 'CTP TRADING/sandbox/01_queries'
```

单独执行参数校验用例或某一个请求：

```bash
just bru-run 'CTP TRADING/sandbox/05_validation'
just bru-run 'CTP TRADING/sandbox/02_limit_orders/open_long.bru'
```

本批只生成用例并离线校验，未连接 SimNow 或提交交易。完整接口说明见 [CTP 文档](../../../doc/current_specs/ctp_trading.md)。
