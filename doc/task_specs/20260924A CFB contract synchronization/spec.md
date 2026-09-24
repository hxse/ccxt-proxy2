# CFB 上游契约同步

## 任务边界

交付最新 CFB 业务 OpenAPI 快照、对应 Bruno 使用说明、受影响 current spec 和离线回归。

上游来源为 `http://127.0.0.1:45173/openapi.json`；通过既有 `just sync-cfb-docs` 更新 `src/openapi/cfb.json`。沿用 `src/cfb_contract.py` 定义的八条业务路径及 HTTP 方法，无新增或删除业务路径。

保持 Bearer 鉴权、白名单、单次请求转发、超时、不自动重试和字节级透传。健康、管理及截图路径不纳入代理业务入口；不增加本地业务模型、参数解释、订单确认或账户切换，不调用真实下单、撤单接口。

停止线为：快照与上游八条业务操作及组件一致，新增参数和响应在代理 OpenAPI 可见，示例表达正确，指定离线验证通过。上游终端实际交易执行不属于本任务验收范围。

## 任务规范

上游 OpenAPI 是 CFB 参数、响应字段、模式能力和失败语义的唯一来源。继续由既有同步器检查八条操作完整性并原子替换快照；文档缺失时保留旧文件。

同步订单查询的 `trading_day/front_id/session_id/order_ref` 参数，以及 `OrderIdentity`、`OrderExecution`、`Verification`、查询一致性等结构。保留同一快照中的组件；代理文档统一为组件及引用增加 `Cfb_` 前缀，包括 discriminator 映射。

订单完整引用查询、参数组互斥、模式与启动环境匹配等规则由上游校验。代理不补默认参数，不将订单编号转为数字，不丢弃负数会话编号、前导零或上游错误响应中的提交事实。当前上游默认 `mode=sandbox`，实盘请求显式使用 `live`，参数不会切换上游账户。

## 公开接口与用户写法

本任务不改变代理 URI、HTTP 方法及鉴权方式。已有查询直接支持新字段；Bruno 订单查询示例提供默认关闭的可选参数，编号和交易日须替换为实际响应值。

订单编号查询示例：

```text
GET /cfb/fetch_orders?mode=sandbox&exchange_id=DCE&instrument_id=m2701&order_sys_id=648294
```

完整引用查询示例：

```text
GET /cfb/fetch_orders?mode=sandbox&exchange_id=DCE&instrument_id=m2701&trading_day=20260924&front_id=3&session_id=-123&order_ref=000018
```

完整引用包含交易所、合约、交易日、前置、会话及报单引用六个字段，另带原 `mode`。两种定位方式不混用；只传 `order_ref` 等不完整引用，由上游返回 422；查询其他交易日由上游返回 501；模式不匹配启动环境由上游返回 409，代理原样保留这些结果。

调用方将提交响应的 `order_id` 传给 `order_sys_id`，不使用 `request_id` 替代。`order_id=null` 时可以使用完整 `identity` 复查。HTTP 202、`submitted`、`verification.status=observed` 和 `consistency=stable` 都不单独表示全部成交；查看订单 `status/filled_volume/remaining_volume`。非成功响应也可能含已提交事实，查询最新结果使用 GET。

## 测试、验证与阶段过渡

使用正式入口：

```bash
just sync-cfb-docs http://127.0.0.1:45173/openapi.json
CCXT_PROXY_CONFIG_PATH=Test/fixtures/config.toml just test-file Test/test_cfb_proxy.py Test/test_cfb_openapi.py Test/test_cfb_lifecycle.py Test/test_repository_contract.py -q
```

同步只在线读取 OpenAPI，不运行上游业务请求。测试使用本地 ASGI 和 `MockTransport`，验证八条路由、参数与原始正文透传、新的完整引用查询、模式默认值文档、响应组件引用及代理自身鉴权和网络错误。

正向验证包括原样保留新增提交/核验字段、订单编号及完整引用查询参数。反向验证包括上游 409/422/501 等错误透传、代理超时不重试、未授权或未启用时不转发、同步文档不完整时旧快照不被覆盖。Bruno 活跃查询参数必须与 URL 一致。

更新后按同步器提取规则比较本地快照与上游文档，核对所有指定验证的结果、警告和跳过。无需兼容桥接或入口停用；应用重启后加载新快照，新参数的实际转发能力由既有薄转发提供。
