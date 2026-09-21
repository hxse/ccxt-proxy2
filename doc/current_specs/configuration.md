# 配置、鉴权与服务生命周期

本规范定义项目公共配置入口、服务启用、启动快照及部署约束。字段模型位于 [config_types.py](../../src/tools/config_types.py)，完整配置示例为 [config.example.toml](../../config.example.toml)。账户信息只保存在本地配置，业务请求不接受连接密码。

## TOML 配置入口

默认文件为项目根目录 `config.toml`。`CCXT_PROXY_CONFIG_PATH` 可选择其他 TOML 文件；相对路径按项目根目录解析，绝对路径直接使用。环境变量只选择文件，不覆盖账号字段。应用、Bruno 和调试脚本共用同一个配置加载入口。

配置在进程启动时读取一次；运行中编辑文件不会改变白名单、鉴权或账户信息，重启后生效。连接重建继续使用内存配置。加载失败只报告必要字段范围，隐藏原始配置值、异常链及用户自定义敏感键。

最小配置示例：

```toml
SECRET = "replace-with-a-random-secret"

[users.admin]
password = "replace-with-your-password"
```

`SECRET` 是必填根字段，放在所有分组之前。用户名含点时使用 `[users."alice.dev"]`。字符串采用 TOML 引号和转义；密码中的美元符号及带美元符号的变量表达式原样保留。TOML 没有 null，未配置的可选分组直接省略或保持注释。

配置模型拒绝未知字段。账号分组存在但未填写必填值，仍会导致配置校验失败；“暂不启用”通过省略整组或保留注释表达。`config.toml`、`config.toml.*`、旧 .env 及数据目录被 Git 和 Docker 构建上下文排除，真实值不得写入示例。

## HTTP 鉴权

业务接口使用 `/auth/token` 的 OAuth2 Password Grant 和 Bearer JWT，登录账号来自 `[users.<username>]`，与交易所和期货账号独立。

`POST /auth/token` 接受表单用户名、密码，成功返回 `access_token`、`token_type="bearer"` 和 `expires_in=3600`，并写入登录 Cookie。没有 refresh token；过期后重新登录。鉴权读取内存配置，不占用同步 SDK 线程池。

Bruno collection 共享变量仅为 `baseUrl` 与 secret `user/password`；请求专用参数属于具体请求。Bruno 的自动登录和运行约束见[验证规范](verification.md)。

## 统一服务白名单

只有 `[[service_whitelist]]` 列出的身份会初始化。填写账户或上游地址本身不会启用服务；缺省白名单为空。所有路由仍保留在 OpenAPI 中，未启用服务返回 `503 SERVICE_NOT_ENABLED`，应用尚未就绪时返回 `503 SERVICE_NOT_READY`。

| service | 白名单字段 | 身份 | 必需配置与初始化 |
| --- | --- | --- | --- |
| ccxt | exchange、market、mode | ccxt/交易所/市场/模式 | 对应 test/live 凭证，创建客户端并加载 markets |
| tq | 仅 service | tq | [tq]，在专用线程创建并初始化 TqApi |
| ctp | mode | ctp/sandbox 或 ctp/live | [ctp.test] 或 [ctp.live]，认证、登录、确认结算 |
| cfb | 仅 service | cfb | [cfb]，创建复用的 HTTP 客户端 |

ccxt 的 exchange 为 binance/kraken，market 为 spot/future，mode 为 sandbox/live；Kraken spot sandbox 身份在配置阶段拒绝。CCXT 的 test 对应 sandbox，live 对应实盘。开启交易所代理时必须同时存在可用的 [proxy] 地址。

TQ 和 CFB 的白名单项不接受 exchange/market/mode。CFB 请求中的 mode 原样转发，由上游决定支持范围。Telegram 与公共时间不属于这份服务白名单，分别遵循自己的模块规范。

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

同一身份不得重复，白名单不得引用缺失的配置。身份和账号校验在启动时完成，不能静默跳过不可用项或切换到另一个模式。

## 启动、关闭与健康检查

启动协调器按白名单顺序初始化，全部完成后才接受 HTTP 请求。任一初始化失败，清理已建立的资源并退出。空白名单仍可运行鉴权、文档、健康检查及不依赖该白名单的接口。

启动取消时停止初始化后续实例，等待当前 SDK 初始化结束后统一清理，不强行中断 SDK。关闭重复调用须安全；TQ 在所属线程关闭 SDK，CTP 释放原生连接，CCXT 关闭会话与缓存，CFB 在应用事件循环关闭 HTTP 客户端，Telegram 关闭自己持有的客户端。

`GET /healthz` 只表示应用进程能处理请求，返回 `{"status":"ok"}`。`GET /readyz` 返回启动就绪及已初始化身份，例如：

```json
{"status":"ready","initialized":["cfb"]}
```

进入或退出生命周期期间返回 503/not_ready。readyz 不做实时网络探测，也不保证某个合约已经订阅。CFB 初始化不调用上游业务接口，不启动容器或登录账户，因此其代理就绪不等于终端交易就绪。

## 本地缓存配置

```toml
[ohlcv_cache]
database_path = "./data/cache/ohlcv.duckdb"
max_rows_per_series = 2000000
max_rows_total = 20000000
```

省略时使用上述默认值。容量要求 `100000 < max_rows_per_series <= max_rows_total`；它与单次响应预算分开，具体事务与淘汰规则见[缓存操作](ohlcv_cache_operations.md)。

## 部署入口

本地先复制并填写配置，再执行 `uv sync --locked` 和 `just serve`。Windows 开发优先使用 127.0.0.1，避免 localhost 与 reload 带来的额外延迟。CTP 的可选依赖和独立 GUI 联调见对应模块规范。

`docker compose up -d --build` 将本地 config.toml 只读挂载到容器，不写入镜像。直接运行镜像可采用：

```bash
docker run -d -p 5123:8000 \
  -v ~/ccxt-proxy2:/app/data \
  -v "$PWD/config.toml:/app/config.toml:ro" \
  -e PYTHONUNBUFFERED=1 \
  --name ccxt-proxy2 \
  --restart=always \
  hxse/ccxt-proxy2:latest
```

当前部署保持单个 Uvicorn 进程。CFB 独立容器的连接地址见 [CFB 代理](cfb_proxy.md)。

## 已提供的一次性配置迁移工具

运行时只读取 TOML，旧格式仅由独立工具处理，不存在自动兼容读取。

- 旧 .env：`uv run --no-sync python scripts/migrate_config_to_toml.py`。
- 旧 JSON：上述命令加 `--source data/config.json`。
- 旧 exchange_whitelist：`uv run --no-sync python scripts/migrate_service_whitelist.py`。

工具核对后保留原文件备份；配置和备份权限为 0600，既有目标或备份不覆盖。白名单迁移保留原 CCXT 身份，并显式列出原已配置的 TQ/CTP 模式。新旧白名单不能混用；旧 CCXT_PROXY_ENV_FILE 应改为 CCXT_PROXY_CONFIG_PATH。迁移后核对内容并重启应用。
