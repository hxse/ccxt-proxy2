# 公共时间转发

`GET /system/fetch_time` 复用本项目 Bearer 鉴权，不接受 query 参数。每次请求调用币安公共 `GET https://api.binance.com/api/v3/time`，不需要币安账号、API key 或服务白名单。

成功返回原始 JSON，例如：

```json
{"serverTime":1789689600123}
```

serverTime 为整数 Unix 毫秒时间戳，与显示时区无关；对应上游生成响应的时刻。客户端收到结果时仍有传输延迟，服务不做延迟补偿。

HTTP 鉴权、公共取时和业务日期转换是不同职责。当前公共函数位于 [public_time.py](../../src/tools/public_time.py)，不初始化 CCXT 交易所实例；是否使用该结果决定某个业务范围，由对应业务规范定义。该接口不会自动替换 TQ SDK 内部的本机时间，也不构成客户端任意日期的可信证明。

## 请求与失败语义

整次上游请求等待限时 5 秒，异步等待网络，不占用交易或行情 SDK 队列。无自动重试、时间缓存、后台校时或本机时间回退，不修改操作系统时间。响应携带 `Cache-Control: no-store`。

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

离线测试模拟 HTTP，验证原值保留、参数与鉴权、错误映射、整次等待时限，以及每次调用不重试。真实公共时间来自[币安时间接口](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/general#time)，部署环境需要能够访问该地址。

## 元数据业务使用

TQ 日历/历史映射复用 fetch_public_time 公共函数，每个请求固定一次可信时间；不经本机 HTTP 回调，不接受客户端 now，不回退本机日期。历史映射的已生效日还需实际主连行情时间，不能仅将周末在线日期推到下周。仅当前主连 items 查询也固定一次在线时间和实际参考行情日期。源规则见 [TQ 元数据](tq_metadata.md)。单调时钟和本机时间用于 SDK/HTTP 等待 deadline，不用于判断交易历史生效。
