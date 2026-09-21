# CFB HTTP 薄转发

ccxt-proxy2 使用现有 Bearer 鉴权，将固定的八条 `/cfb` 业务路由转发到独立的 cn-futures-bridge 服务。模拟盘 `mode=sandbox`、实盘 `mode=live` 及其他请求字段均原样传递，上游决定实际支持范围。

## 配置与启动

在本项目 `config.toml` 中增加：

```toml
[cfb]
base_url = "http://127.0.0.1:45173"
request_timeout_seconds = 300

[[service_whitelist]]
service = "cfb"
```

`base_url` 是上游根地址，不需要加 `/cfb`。它支持 HTTP/HTTPS，可带部署路径前缀，不接受账号、密码、query 或 fragment。期货账户继续在 CFB 项目配置；本项目的 JWT 仅用于入口鉴权，不传递给 CFB。

配置仅在进程启动时读取。白名单启用 `cfb` 后，启动时创建可复用的 HTTP 客户端，退出时关闭；未启用时请求返回 `503 SERVICE_NOT_ENABLED`。启动不调用上游业务接口，不触发登录或管理 CFB 容器。`/readyz` 中的 `cfb` 表示代理客户端初始化完成，CFB 自身的终端就绪由上游负责。

如果 ccxt-proxy2 在宿主机运行，可使用上述本机地址。两个服务分别运行在容器内时，将它们接入同一个启用 DNS 的 Podman 网络，并使用 `http://cn-futures-bridge:45173` 等容器地址。CFB 无鉴权端口保持仅本机或受控容器网络可访问。

## 转发行为

| 方法 | 路由 |
| --- | --- |
| POST | `/cfb/create_market_order` |
| POST | `/cfb/create_limit_order` |
| POST | `/cfb/cancel_order` |
| GET | `/cfb/fetch_orders` |
| GET | `/cfb/fetch_trades` |
| GET | `/cfb/fetch_positions` |
| GET | `/cfb/fetch_balance` |
| GET | `/cfb/fetch_trading_status` |

查询字符串、请求体字节及 `Idempotency-Key` 原样传递，不由代理解析、补默认值或执行交易判断。CFB 的 HTTP 状态码、正文、请求编号和业务响应头原样保留。HTTP 连接专用头不跨连接传递；HTTPX 解压正文后同步移除编码头并重算长度。

每次请求只发送一次，不自动重试、不跟随重定向、不缓存结果、不增加业务队列。上游返回的错误和 `202` 等状态也直接转发。仅代理自身网络失败返回 `502 {"detail":{"code":"CFB_PROXY_NETWORK_ERROR"}}`，超过配置的整次上游请求等待时间返回 `504 {"detail":{"code":"CFB_PROXY_TIMEOUT"}}`。

## 自动文档同步

本项目 `/docs`、`/redoc` 和 `/scalar` 中的 CFB 请求参数、JSON Schema、返回定义及业务说明，直接复制自上游 `/openapi.json`，保存在 [src/openapi/cfb.json](../../src/openapi/cfb.json)。运行时不请求上游文档，不需要安装或导入 CFB 项目的 Python 模块。

更新 CFB 接口后执行：

```bash
just sync-cfb-docs
# 或指定其他文档地址
just sync-cfb-docs http://127.0.0.1:45173/openapi.json
```

同步只读取自动文档，固定保留八条业务路由；不会调用查询、下单或撤单接口。缺少预期路由时保留旧快照。重启代理后加载新文档。代理展示时给组件引用增加 `Cfb_` 前缀避免名称冲突，并补上本项目鉴权及代理网络错误，其余业务定义沿用上游。

上游文档中的能力限制同样来自该快照；代理自身不根据文档拦截请求。CFB 的模式能力由上游实现，代理持续透传 `mode=sandbox/live`；上游能力变化后，重新同步文档即可更新展示的说明。

Bruno 示例位于 [bruno/CFB](../../bruno/CFB)，默认均为 `sandbox`，可按需修改为 `live`。`just bru-cfb-readonly` 只执行五个 GET；下单和撤单示例标记 `[STATEFUL]`，按需单独运行。
