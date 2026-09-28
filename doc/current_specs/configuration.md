# 配置、鉴权与服务生命周期

本规范定义项目公共配置入口、服务启用、启动快照及部署约束。字段模型位于 [config_types.py](../../src/tools/config_types.py)，完整配置示例为 [config.example.toml](../../config.example.toml)。账户信息只保存在本地配置，业务请求不接受连接密码。

## TOML 配置入口

默认文件为项目根目录 `config.toml`。`CCXT_PROXY_CONFIG_PATH` 可选择其他 TOML 文件；相对路径按项目根目录解析，绝对路径直接使用。CCXT_PROXY_PROFILE 必须明确选择 dev、local 或 remote；缺失或非法时报错，不按容器环境猜测。环境变量仅选择文件和场景，不覆盖账号字段。应用、Bruno 和调试脚本共用同一个配置加载入口。

配置在进程启动时读取一次；运行中编辑文件不会改变白名单、鉴权或账户信息，重启后生效。连接重建继续使用内存配置。加载失败只报告必要字段范围，隐藏原始配置值、异常链及用户自定义敏感键。

最小配置示例：

```toml
SECRET = "replace-with-a-random-secret"

[users.admin]
password = "replace-with-your-password"

# 默认公开计划启用后台清理，需要引用已有 HTTP 登录账号。
[market_data_client]
user = "admin"
```

`SECRET` 是必填根字段，放在所有分组之前。用户名含点时使用 `[users."alice.dev"]`。字符串采用 TOML 引号和转义；密码中的美元符号及带美元符号的变量表达式原样保留。TOML 没有 null，未配置的可选分组直接省略或保持注释。

配置模型拒绝未知字段。账号分组存在但未填写必填值，仍会导致配置校验失败；“暂不启用”通过省略整组或保留注释表达。`config.toml`、`config.toml.*`、旧 .env 及数据目录被 Git 和镜像构建上下文排除，真实值不得写入示例。

## 运行场景覆盖

公共字段用于所有场景，顶层 overrides.dev / overrides.local / overrides.remote 只声明差异。统一加载器在内存中递归合并表，普通值和数组整体替换；false、0、空字符串也参与覆盖。原文件保持不变，合并后的结果交给既有模型和业务依赖校验。

场景表只能使用上述三个名称，必须为表；覆盖字段和结构在所有场景中校验，不能藏入未选场景的未知字段。必需字段和跨字段依赖按选中场景的最终配置验证。不得嵌套第二层 overrides。错误只说明范围，不输出原始配置值。内部预检可显式指定 profile，优先于环境，仍复用同一加载器。

Just 的源码、测试和辅助入口注入 dev；本地生产预检和运行注入 local；远端上传预检、镜像预检和运行注入 remote。后台子进程继承父进程场景。直接启动应用必须设置 CCXT_PROXY_PROFILE，例如 `CCXT_PROXY_PROFILE=dev uv run --no-sync uvicorn src.main:app --host 127.0.0.1 --port 5123`。

公共配置中三个 enable_proxy 默认 false，CFB 为 http://127.0.0.1:45173。远端差异使用以下写法，上传文件原样保留，最终生效值由 remote 场景决定：

```toml
[overrides.remote.binance]
enable_proxy = true
[overrides.remote.kraken]
enable_proxy = true
[overrides.remote.tq]
enable_proxy = true
[overrides.remote.cfb]
base_url = "http://cn-futures-bridge:45173"
```

保留共享 proxy 地址；覆盖不会自行启用未列入 service_whitelist 的服务。三个场景的地址/凭据选择不改变路由里的 live/sandbox 语义。上传端不再修改代理字段或做第二次配置合并。

## HTTP 鉴权

业务接口使用 `/auth/token` 的 OAuth2 Password Grant 和 Bearer JWT，登录账号来自 `[users.<username>]`，与交易所和期货账号独立。

`POST /auth/token` 接受表单用户名、密码，成功返回 `access_token`、`token_type="bearer"` 和 `expires_in=3600`，并写入登录 Cookie。没有 refresh token；过期后重新登录。鉴权读取内存配置，不占用同步 SDK 线程池。

Bruno collection 共享变量仅为 `baseUrl` 与 secret `user/password`；请求专用参数属于具体请求。Bruno 的自动登录和运行约束见[验证规范](verification.md)。

## 统一服务白名单

只有 `[[service_whitelist]]` 列出的身份会初始化。填写账户或上游地址本身不会启用服务；缺省白名单为空。所有路由仍保留在 OpenAPI 中，未启用服务返回 `503 SERVICE_NOT_ENABLED`；已启用但初始化中、失败、关闭或已知 SDK 不可用时返回 `503 SERVICE_NOT_READY`，detail 同时包含 service 身份。

| service | 白名单字段 | 身份 | 必需配置与初始化 |
| --- | --- | --- | --- |
| ccxt | exchange、market、mode | ccxt/交易所/市场/模式 | 对应 test/live 凭证，创建客户端并加载 markets |
| tq | 仅 service | tq | [tq]，在专用线程创建并初始化 TqApi |
| ctp | mode | ctp/sandbox 或 ctp/live | [ctp.test] 或 [ctp.live]，认证、登录、确认结算 |
| cfb | 仅 service | cfb | [cfb]，创建复用的 HTTP 客户端 |

ccxt 的 exchange 为 binance/kraken，market 为 spot/future，mode 为 sandbox/live；Kraken spot sandbox 身份在配置阶段拒绝。CCXT 的 test 对应 sandbox，live 对应实盘。开启交易所代理时必须同时存在可用的 [proxy] 地址。Binance、Kraken Futures/Spot 的同步客户端通过同一个 proxies 字典将该地址映射到 HTTP 和 HTTPS 目标；关闭时 proxies 为 None。地址优先级保持 proxy.effective_http 的既有规则，不同时设置 SDK 的 httpProxy/httpsProxy；代理地址自身可用 http:// 或 https://，不会按目标协议改写。

公共取时使用独立 CCXT 异步 Binance 客户端，读取同一 binance.enable_proxy 和 proxy.effective_http，通过 SDK aiohttp_proxy 传递；关闭时禁用环境代理。该能力不依赖交易白名单或账号，因此即使没有启用 Binance 交易身份，开启币安代理也必须提供地址。详见[公共时间](system_time.md)。

TQ 和 CFB 的白名单项不接受 exchange/market/mode。CFB 请求中的 mode 原样转发，由上游决定支持范围。Telegram 与公共时间不属于这份服务白名单，分别遵循自己的模块规范。

Binance、Kraken、TQ 的 enable_proxy 均默认 false。TQ 配置 `[tq] enable_proxy=true` 时使用同一 proxy.effective_http 地址；启用的 TQ 服务缺少代理地址会在配置阶段拒绝。该开关同时覆盖 SDK 认证/查询 HTTP、行情/交易状态 WebSocket 和自有日历/主连源下载，关闭时显式直连，不继承环境代理。代理绑定启动快照和 TQ 调用上下文，不修改进程环境或其他 SDK 的配置；详细传输边界见 [TQ 规范](tq_data.md)。

CFB 最小启用示例：

```toml
[cfb]
base_url = "http://127.0.0.1:45173"
request_timeout_seconds = 300

[[service_whitelist]]
service = "cfb"
```

其他服务的白名单写法如下；使用前需填写对应账号分组：

```toml
[[service_whitelist]]
service = "ccxt"
exchange = "binance"
market = "future"
mode = "sandbox"

[[service_whitelist]]
service = "tq"

[[service_whitelist]]
service = "ctp"
mode = "sandbox"
```

同一身份不得重复，白名单不得引用缺失的配置。身份和账号字段校验在启动时完成；上游初始化失败保留为 failed 身份，不从白名单静默删除，也不切换到另一个模式。

## 启动、关闭与健康检查

配置与后台计划校验通过后，每个身份使用独立后台线程初始化，HTTP 不等待全体 SDK。TQ、CFB、Binance、Kraken 和 CTP 各自失败或等待网络时，其他身份继续初始化和服务；CCXT 的市场、模式也分别隔离。配置本身非法仍阻止应用启动。全部 SDK 失败或空白名单时，鉴权、文档、健康及不依赖这些 SDK 的路由仍可用。

ServiceRuntime 统一持有 initializing/ready/failed/stopped 状态；只有 ready 身份可进入业务。初始化失败不触发其他身份清理，请求也不重建失败 SDK。TQ 工作线程退出后本地可用性检查立即拒绝新请求。首次发生但尚未被 SDK 识别的断网，仍需正常调用或原有超时发现；不增加每请求健康探测、自动重建或全局熔断。

明确的 CCXT 只读网络失败、旧客户端关闭、CFB 代理网络失败/超时和 TQ 网络/线程不可用统一为 503 SERVICE_NOT_READY。数据完整性、权限、业务拒绝及 OPERATION_STATUS_UNKNOWN 保留原错误；运行中请求失败不代表写操作未执行，不自动重发。CFB 已收到的上游业务响应继续原样转发，TQ 休市缓存兜底先按原逻辑处理。

退出先禁止新业务和新初始化、停止后台任务，再等待已发起的 SDK 初始化结束，关闭元数据任务和 Provider，最后关闭共享缓存；晚到的初始化成功不能恢复已关闭状态。不能强行中断 SDK；其当前调用仍受 SDK 自身等待规则约束。关闭重复调用须安全；TQ 在所属线程关闭 SDK，CTP 释放原生连接，CCXT 关闭会话，CFB 在应用事件循环关闭 HTTP 客户端，Telegram 关闭自己持有的客户端。

`GET /healthz` 只表示应用进程能处理请求，返回 `{"status":"ok"}`。`GET /readyz` 的 200 表示 HTTP 应用已就绪，不要求全体 SDK 成功；initialized 是当前 ready 身份按白名单顺序生成的视图，services 是逐身份状态，例如：

```json
{"status":"ready","initialized":["cfb"],"services":{"tq":"failed","cfb":"ready"}}
```

进入或退出基础生命周期期间返回 503/not_ready，仍带 initialized 和 services。readyz 不做实时网络探测，也不保证某个合约已经订阅。CFB ready 只表示代理客户端已创建，不等于上游终端已登录或交易就绪。访问上述失败的 TQ 路由返回 `{"detail":{"code":"SERVICE_NOT_READY","service":"tq"}}`；健康的 CFB 路由照常执行。

## 本地缓存配置

```toml
[ohlcv_cache]
database_path = "./data/cache/ohlcv.duckdb"
max_rows_per_series = 2000000
max_rows_total = 20000000
```

省略时使用上述默认值。容量要求 `100000 < max_rows_per_series <= max_rows_total`；它与单次响应预算分开，具体事务与淘汰规则见[缓存操作](ohlcv_cache_operations.md)。

缓存由 ServiceRuntime 持有的 CacheResource 延迟创建并统一关闭；CCXT 只接收注入资源，关闭单个 Provider 不关闭共享库。仅 TQ 或空白名单也可取得同一缓存资源，取缓存不初始化 Provider。上述容量是写入事务中的大容量保护，不是每小时保留目标。

公开 market_data.toml 默认启用 TQ 固定窗口采集和缓存清理；私有 market_data_client 指定地址、已有 HTTP 用户和超时。需要在原配置中补齐 user，或显式关闭公开计划中的采集、清理两个 enabled。启动前验证，HTTP 可用后才执行后台首轮；退出先停止后台再关闭 SDK/缓存。参数作用域、默认值、单轮入口及失败规则见[后台任务](market_data_jobs.md)。

## 部署入口

本地先复制并填写配置，再执行 `just sync` 和 `just serve`；需要 CTP 时同步加 `--extra=ctp`。serve 支持 --config、--host、--port，固定 no-sync，配置选择仍沿用本规范。Windows 开发优先使用 127.0.0.1，避免 localhost 与 reload 带来的额外延迟。CTP 的可选依赖和独立 GUI 联调见对应模块规范。

容器采用 Podman，本地优先 rootless，远端沿用现有登录账号，开发和测试仍在宿主运行。按目标明确选择动作：

```bash
just deploy --target=local --build
just deploy --target=local --start
just deploy --target=remote --upload --build --start
```

两份运行配置均不进入镜像。本地生产直接只读挂所选原 config.toml 与项目 market_data.toml，不复制第二套；远端默认原样完整上传两份配置到待发布快照，运行时采用 remote 场景。--keep-remote-config 不读取或上传本地运行配置，复用远端完整快照。源码按白名单和摘要增量同步。数据库独立保留在宿主 data 目录，宿主只绑定 `127.0.0.1:5123`，后台自身 base_url 同为 `http://127.0.0.1:5123`，保持单 Uvicorn 进程。

可选 `[deployment]` 分组定义 `ssh_host` 和 `remote_dir`，只由显式远端操作使用。配置模块对只构建/控制远端实例或保留远端配置上传的命令，按 remote 场景合并后仅校验这一分组，无需本机应用账号或白名单有效；应用启动和上传仍进行完整配置校验。完整字段、示例、快照、防重复及恢复契约见 [Podman 部署](container_deployment.md)。CFB 上游地址见 [CFB 代理](cfb_proxy.md)。

## 已提供的一次性配置迁移工具

运行时只读取 TOML，旧格式仅由独立工具处理，不存在自动兼容读取。

- 旧 .env：`uv run --no-sync python scripts/migrate_config_to_toml.py`。
- 旧 JSON：上述命令加 `--source data/config.json`。
- 旧 exchange_whitelist：`uv run --no-sync python scripts/migrate_service_whitelist.py`。

工具核对后保留原文件备份；配置和备份权限为 0600，既有目标或备份不覆盖。白名单迁移保留原 CCXT 身份，并显式列出原已配置的 TQ/CTP 模式。新旧白名单不能混用；旧 CCXT_PROXY_ENV_FILE 应改为 CCXT_PROXY_CONFIG_PATH。迁移后核对内容并重启应用。

TQ 自有元数据源沿用集中读取的 TQ_CHINESE_HOLIDAY_URL、TQ_CONT_TABLE_URL。不新增登录配置或 refresh_source 开关；下载头取自已初始化 SDK，应用关闭时先停止元数据任务并释放 HTTP 客户端。
