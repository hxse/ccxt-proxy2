# CCXT 订单请求与回执修复

## 任务边界

修复 Kraken Futures 普通限价单 IOC/FOK、CCXT 订单回执 type/side 未知，以及下单 amount 和 set_leverage 的布尔值输入；同步 current spec 与 OpenAPI 描述并补离线 HTTP 回归。

保留现有 URI、参数名称、sandbox/live 默认、价格校验、数量步长处理、Binance normal/conditional 回退条件和写操作不重试。不扩展 CTP、CFB、TQ，不升级 CCXT，不重新设计市价/触发单有效期，不新增真实交易测试或兼容链路。

停止线：真实 SDK 最终请求的订单类型与 IOC/FOK 意图一致，成功查询/撤单可返回未知字段，布尔值在进入客户端前拒绝，相关离线与静态检查通过。

## 任务规范

### 有效期编码

翻译位于 CcxtClient 的现有交易边界，不在 HTTP 路由增加交易所分支。仅 Kraken Futures 的普通 limit 且 timeInForce 为 IOC/FOK 时，把参数明确转换为原生 orderType=ioc/fok，同时提供相应小写 timeInForce；客户端直接调用的小写写法也正确处理。保留限价和其他无冲突参数，不修改调用方原参数字典。

postOnly=true 与 IOC/FOK 不兼容；同时提供不同的 orderType 或附带 stopLossPrice/takeProfitPrice 时，提交前返回 422 INVALID_PROVIDER_REQUEST。不能静默选择一个语义，也不能让新增原生 orderType 覆盖触发单语义。GTC、无有效期、其他市场/Provider 和既有独立原生参数保持原处理。

锁定版本对原生 fok 回执还可能解析出 timeInForce=gtc。仅在 Kraken Futures 成功创建回执的已解析 type 明确为 fok 时，将 timeInForce 修正为 fok；不据请求参数伪造订单类型、方向、状态或成交量，不追加查询。

### 未知回执字段

OrderStructure.type/side 允许显式 null 和字段缺失，缺失时输出 null；订单 ID、symbol、原始 info 及已知字段保留。不把未知类型/方向补成空字符串、market 或 buy。单笔与订单列表共用此模型；其他必需字段和状态语义不变。

Binance 条件单撤单仍仅在普通撤单明确 OrderNotFound 后切换 conditional 入口；网络失败不能触发新撤单或重试。SDK 已收到成功回执时，不因 type/side 未提供而变成 HTTP 500。

### 数值输入

在 Pydantic 数值转换之前拒绝 bool。四类下单请求的 amount 与 SetLeverageRequest.leverage 使用和价格字段相同的 BeforeValidator 机制；true/false 均为 HTTP 422，尚未调用 exchange_manager.get_client。

保留已有正整数、合法浮点和可解析数字字符串；正数、有限值及整数约束继续由原字段执行。客户端原有 bool 拒绝仍保留，不能只依赖路由。

## 公开接口与用户写法

现有限价请求写法不变，例如：

```http
POST /ccxt/create_limit_order
Content-Type: application/json

{"exchange_name":"kraken","market":"future","mode":"sandbox","symbol":"BTC/USD:USD","side":"buy","amount":1,"price":50000,"timeInForce":"IOC"}
```

最终 Kraken 请求必须带 orderType=ioc 与 limitPrice=50000；FOK 对应 fok。增加 postOnly=true 或冲突 orderType=lmt 时明确返回 422，不发出交易写请求。

订单未知字段示例（其余可选字段省略）：

```json
{"order":{"id":"2146760","symbol":"BTC/USDT:USDT","status":null,"type":null,"side":null,"info":{"algoId":2146760,"clientAlgoId":"offline-conditional","code":"200","msg":"success"}}}
```

amount=true/false 与 leverage=true/false 在参数阶段拒绝；amount="0.0015" 和 leverage="2" 继续按原规则转换。此任务不改变数量精度或自动舍入行为。

官方契约依据：[Kraken sendorder](https://docs.kraken.com/api/docs/futures-api/trading/send-order/)、[Kraken order status](https://docs.kraken.com/api/docs/futures-api/trading/get-order-status/)、[Binance 条件单撤单](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade#cancel-algo-order)。

## 测试、验证与阶段过渡

全部使用离线入口。真实锁定 CCXT 实例装载固定 markets，拦截 fetch 网络边界，使用官方协议形状的固定回执；经隔离 FastAPI HTTP 入口验证，不只断言中间 mock 参数。

覆盖 IOC/FOK 两侧最终发送的 orderType、限价和数量；GTC/默认/PostOnly 不回退；冲突组合在写入前失败；原参数不变；FOK 回执有效期校正仅有明确类型证据时生效。保留其他 Provider 的处理。

覆盖 Kraken Futures orders/status 原始嵌套订单解析后 type=null；Binance 普通撤单明确不存在后条件单成功回执 type/side=null；HTTP 200、ID、原始 info 保留，网络错误不重试。另覆盖订单列表与 OpenAPI 的 nullable 字段。

四类下单及杠杆 HTTP 布尔输入均在客户端查找前返回准确字段的 422；合法数值和数字字符串继续成功进入正确参数。保留直接客户端反向校验。

运行 just test-file 的对应离线测试，再通过 just test、just lint、just check。不执行线上下单、撤单或账户写操作，不新增在线测试。实现、fixture、接口描述与 current spec 同阶段切换，不保留旧错误行为。
