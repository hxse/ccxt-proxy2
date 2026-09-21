# 公共时间转发

`GET /system/fetch_time` 复用本项目 Bearer 鉴权，不接受 query 参数。每次请求调用币安公共 `GET https://api.binance.com/api/v3/time`，不需要币安账号、API key 或服务白名单。

成功返回原始 JSON，例如：

```json
{"serverTime":1789689600123}
```

serverTime 为整数 Unix 毫秒时间戳，与显示时区无关；对应上游生成响应的时刻。客户端收到结果时仍有传输延迟，服务不做延迟补偿。

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
