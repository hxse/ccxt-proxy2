# 公共时间转发

`GET /system/fetch_time` 复用本项目 Bearer 鉴权，不接受 query 参数。每次通过 CCXT 异步 Binance 的 `fetch_time()` 调用 live 现货公共时间接口，不需要币安账号、API key 或服务白名单。

成功返回原始 JSON，例如：

```json
{"serverTime":1789689600123}
```

serverTime 为整数 Unix 毫秒时间戳，与显示时区无关；对应上游生成响应的时刻。客户端收到结果时仍有传输延迟，服务不做延迟补偿。

HTTP 鉴权、公共取时和业务日期转换是不同职责。唯一公共函数位于 [public_time.py](../../src/tools/public_time.py)，每次持有独立 CCXT 异步客户端，结束时关闭客户端和 HTTP 会话；不加载市场、不等待交易注册表就绪，不占用交易实例的锁。是否使用结果决定某个业务范围，由对应业务规范定义。该接口不会自动替换 TQ SDK 内部的本机时间，也不构成客户端任意日期的可信证明。

代理复用启动时合并后的 `binance.enable_proxy` 和 `proxy.effective_http`，遵循 `overrides.remote`；未配置 Binance 或开关关闭时明确直连，不继承环境代理。开启时缺少代理地址在配置阶段拒绝，即使没有启用 Binance 交易身份。固定访问 live `https://api.binance.com/api/v3/time`，不使用 sandbox、不按交易实例的 market/mode 改变时间来源。

## 请求与失败语义

整次上游请求等待限时 5 秒，异步等待网络，不占用交易或行情 SDK 队列。无自动重试、重定向、时间缓存、后台校时或本机时间回退，不修改操作系统时间。响应携带 `Cache-Control: no-store`。

通过 CCXT 响应钩子在 SDK 数值转换前严格验证原始 serverTime 为非负 int64，拒绝字符串、布尔值和浮点数，保留上游扩展字段。HTTP 错误保留状态码，响应不带上游正文或代理凭据；外部取消继续传播，关闭本次请求资源。

| HTTP | detail.code 或响应 | 含义 |
| --- | --- | --- |
| 401 | 既有鉴权错误 | 缺少或无效的项目 token |
| 422 | 参数校验错误 | 路由不接受 query 参数 |
| 502 | PUBLIC_TIME_NETWORK_ERROR | 上游网络失败 |
| 502 | PUBLIC_TIME_UPSTREAM_ERROR | 上游 HTTP 错误，detail.upstream_status 保留其状态码 |
| 502 | PUBLIC_TIME_INVALID_RESPONSE | JSON 或 serverTime 类型不符合响应模型 |
| 504 | PUBLIC_TIME_TIMEOUT | 等待上游超时 |

## 文档与验证入口

路由和响应模型见 [system_router.py](../../src/router/system_router.py) 与 [responses_time.py](../../src/responses_time.py)。[Bruno 示例](../../bruno/SYSTEM/fetch_time.bru)通过 `just bru-public-time` 手动运行。

`just test` 中使用真实 CCXT `fetch_time/sign/fetch` 与离线传输替身，验证场景代理、环境代理隔离、无需账户或已启用交易身份、原值保留、参数与鉴权、错误映射、取消、整次等待时限，以及每次调用不重试或跟随重定向。

本地镜像手动验证入口为 `just test-public-time-online`，先执行 `just deploy --target=local --build`。验证临时容器使用 local 场景、只读挂载原配置，经本项目鉴权和真实路由只获取一次 live 公共时间；不启动生产实例、交易 SDK 或后台计划，不挂业务数据库，不上传。真实公共时间来自[币安时间接口](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/general#time)，部署环境需要能够访问该地址。

## 元数据业务使用

TQ 日历/历史映射复用 fetch_public_time 公共函数，每个请求固定一次可信时间；不经本机 HTTP 回调，不接受客户端 now，不回退本机日期。历史映射的已生效日还需实际主连行情时间，不能仅将周末在线日期推到下周。仅当前主连 items 查询也固定一次在线时间和实际参考行情日期。源规则见 [TQ 元数据](tq_metadata.md)。单调时钟和本机时间用于 SDK/HTTP 等待 deadline，不用于判断交易历史生效。
