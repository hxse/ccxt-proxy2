# CFB 模块与独立执行容器

CFB 代码属于本项目 src/cfb，原生资产位于 containers/cfb；原 cn-futures-bridge 项目保持不变。唯一 HTTP 入口是主 FastAPI，八条业务路由及模式/幂等/订单语义由 [CFB API](cfb_api.md) 定义。请求模型直接生成 OpenAPI，不保留旧 HTTP 转发、上游文档快照或 sync-cfb-docs 命令。

## 所有权与协议

主服务只负责鉴权、模型校验、HTTP 等待及格式转换；独立 ccxt-proxy2-cfb 容器持有原 BridgeService、Dispatcher、WorkerProcess、Journal、Wine 和桌面。只有一个业务 FIFO 和 SQLite 写入者，主服务不访问业务数据库，不获得 Podman socket。

双方共享宿主 data/cfb/run/bridge.sock，执行器内路径为 /data/run/bridge.sock，主容器内为 /app/data/cfb/run/bridge.sock。目录私有、socket 权限 0600；同一 rootless Podman 用户持有数据，执行容器内 root 对应宿主该普通用户。没有第二个 FastAPI、CFB HTTP 端口或容器 DNS 依赖。

内部协议 version=1，一连接一请求一响应，UTF-8 JSON 换行分帧，请求上限 64 KiB、响应 8 MiB；只接受 execute/status/health/pause/resume/clean/screenshot。execute 使用原 Operation，带 request_id、配置身份和可选幂等键；服务端再次验证模型和能力，身份不匹配拒绝，不能向旧账户执行。没有任意命令或 pickle。

客户端连接/等待失败不重试；可能发送的写请求返回 submission_status=unknown，不能据此重发。断连取消未开始请求；已开始请求仍由原执行器收尾和持久化。HTTP 超时不会释放 GUI 执行权。准入继续优先重放已保存幂等结果，再检查交易就绪。

响应保留原状态码、正文、提交事实及白名单头：X-Request-ID、Cache-Control、X-Content-Type-Options、Retry-After。CFB 参数及业务错误由局部适配处理，不改变其他路由；鉴权与白名单错误保持主项目格式。单次 CFB 失败不关闭其他 SDK。

## 配置与启动

config.toml 的 cfb 子表承接原配置；完整示例见 config.example.toml，账户只在私有文件填写。cfb.base_url、cfb.api、旧独立加载器退出。启用白名单的正式 serve/deploy 入口管理执行容器；直接绕过 Just 运行 Uvicorn 不负责准备容器。

```toml
[[service_whitelist]]
service = "cfb" # 正式启动入口自动准备执行容器。
[cfb]
enable_proxy = false # 本地默认直连。
vnc_enabled = true # 仅在回环发布 VNC/noVNC。
request_timeout_seconds = 300 # 等待超时不表示交易已取消。
[cfb.bridge]
mode = "sandbox" # 请求必须匹配所选账户环境。
data_dir = "/data" # 执行容器专用目录，宿主为 data/cfb。
[overrides.remote.cfb]
enable_proxy = true # 使用公共 proxy.effective_http。
```

主服务生命周期只创建轻量 socket 客户端；全局 /readyz 中 cfb=ready 表示客户端初始化完成。终端详情看 /cfb/status，交易就绪看 /cfb/readyz，执行进程存活看 /cfb/healthz。容器启动失败会记录原因并允许主服务及其他 SDK 运行；CFB 路由明确报错。

## 代理与故障边界

开启代理时使用公共 HTTP CONNECT 地址，通过镜像内 proxychains-ng 的 32/64 位动态库作用于 Wine TCP；关闭时清除继承代理环境。只支持 http:// 地址和可表达的可选 Basic 凭据，不支持 HTTPS 代理端点或 UDP；不合规配置在启动前报错，不静默直连。

生成的代理配置仅保存在私有运行目录，严格单代理链，无直连回退；回环 X11/控制连接留在本机。是否开启独立于 Binance/Kraken/TQ。容器生命周期、文件治理及原生版本约束见 [CFB 运行规范](cfb_bootstrap.md)。

## 订单结果与复查

提交响应中的 `order_id` 是交易所订单编号，`request_id` 是 CFB 请求追踪编号。复查优先把 `order_id` 原样传给 `/cfb/fetch_orders` 的 `order_sys_id`，并带原 `mode`、交易所及合约。编号保持字符串，不转成数字。

`order_id=null` 且返回 `identity` 时，完整传入 `exchange_id/instrument_id/trading_day/front_id/session_id/order_ref` 六个字段，另带原 `mode`，不混入 `order_sys_id`。完整引用中的负数会话编号及报单引用前导零也原样保留。以下编号仅展示格式，使用时替换为实际响应值：

```text
GET /cfb/fetch_orders?mode=sandbox&exchange_id=DCE&instrument_id=m2701&order_sys_id=648294
GET /cfb/fetch_orders?mode=sandbox&exchange_id=DCE&instrument_id=m2701&trading_day=20260924&front_id=3&session_id=-123&order_ref=000018
```

执行器只查询终端当前交易日；完整引用缺字段或与订单编号混用时返回 422，其他交易日返回 501。主服务与执行器共用同一组业务模型和能力规则。

响应中的 `identity/execution/verification`、查询 `consistency` 及错误详情全部透传。HTTP 202、`submitted`、`verification.status=observed`、`consistency=stable` 都不能单独证明全部成交；读取订单 `status/filled_volume/remaining_volume`。非成功响应也可能保留提交事实及订单标识，后续确认使用订单查询，不根据报错重新下单。

## 验证及来源

Test/fixtures/cfb_source_contract.json 来自原项目当前代码的离线 OpenAPI，作为八条业务接口等价样本；不用于运行时生成路由。Test/cfb 保留原业务回归，Test/test_cfb_* 覆盖真实 socket、断连、身份和统一 HTTP。原文文档及任务材料保存在 doc/archive/cfb_source，仅为来源资料，现行规则以 current spec 为准。

Bruno 示例位于 bruno/CFB，默认 sandbox，请求须匹配启动模式；下单/撤单标记 STATEFUL，不能用作自动离线验证。迁移测试不发送真实订单。
