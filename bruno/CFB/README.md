# CFB Bruno 示例

八条同名业务路由均经过 ccxt-proxy2 鉴权；默认 `mode=sandbox`。`mode` 必须匹配 CFB 的启动环境，不匹配时上游返回 409；上游以实盘启动时显式使用 `live`，请求参数不会切换账户。本服务先配置 `[cfb]` 与 `service="cfb"` 白名单，然后重启。

```bash
just bru-cfb-readonly
just bru-run 'CFB/fetch_trading_status.bru'
```

`bru-cfb-readonly` 只运行五个 GET。下单和撤单示例标记 `[STATEFUL]`，替换实际参数后单独执行；不要整文件夹批量执行。POST 示例预期正常提交返回 202，上游未支持或未就绪时原样返回其错误。

## 提交后查询订单

使用 [fetch_orders.bru](fetch_orders.bru) 复查，保持与原请求相同的 `mode`、交易所和合约。可选查询参数默认关闭，启用时同步修改 URL 与 `params:query`。

- 已取得 `order_id`：启用 `order_sys_id` 并填写该值，保持字符串原样；`request_id` 不能作为订单编号。
- `order_id=null` 且返回 `identity`：将其六个字段完整传入，启用 `trading_day/front_id/session_id/order_ref`，并保留对应 `exchange_id/instrument_id`；此时关闭 `order_sys_id`。示例中的日期和编号须替换为响应原值，保留负数会话编号和报单引用前导零。

查询仅支持当前终端交易日。HTTP 202 和 `submitted` 只表示提交；`verification.status=observed` 或 `consistency=stable` 也不等于全部成交，需要查看订单状态及成交手数。非成功响应仍可能带 `identity/execution/verification`，应通过 GET 查询确认，不能据此更换幂等键重新下单。

接口说明直接同步 CFB `/openapi.json`：`just sync-cfb-docs`，重启后在本项目 `/docs` 查看。Bruno 登录用户名和密码沿用现有配置加载方式，不填写期货账户密码。
