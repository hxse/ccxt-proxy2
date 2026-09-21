# CFB Bruno 示例

八条同名业务路由均经过 ccxt-proxy2 鉴权；默认 `mode=sandbox`，实盘将请求里的 `mode` 改为 `live`。本服务先配置 `[cfb]` 与 `service="cfb"` 白名单，然后重启。

```bash
just bru-cfb-readonly
just bru-run 'CFB/fetch_trading_status.bru'
```

`bru-cfb-readonly` 只运行五个 GET。下单和撤单示例标记 `[STATEFUL]`，替换实际参数后单独执行；不要整文件夹批量执行。POST 示例预期正常提交返回 202，上游未支持或未就绪时原样返回其错误。

接口说明直接同步 CFB `/openapi.json`：`just sync-cfb-docs`，重启后在本项目 `/docs` 查看。Bruno 登录用户名和密码沿用现有配置加载方式，不填写期货账户密码。
