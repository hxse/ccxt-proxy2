# ccxt-proxy2

`ccxt-proxy2` 是一个带鉴权的 FastAPI 代理服务，提供 CCXT 交易所、CTP 期货交易、CFB HTTP、TQ 行情与 Telegram 转发路由。

## 文档状态

CCXT/OHLCV 的破坏性重构已经完成。当前生产路径是 `Route → CcxtClient → DuckDbOhlcvCache`；旧 Parquet/proof-log/callback cache 与平行 CCXT utils/adapter 已移除。

正式保证的 Provider 范围是 Binance USDⓈ-M linear Futures 和 Kraken Futures。Binance/Kraken Spot 路由只作 best-effort 暴露，不承诺完整可用性；Binance COIN-M/inverse 与 Kraken Spot sandbox 不支持。

从 [文档索引](docs/README.md) 开始阅读。重构的总体边界见 [目标架构](docs/architecture/01_target_architecture.md)，代码切换顺序见 [迁移与验收](docs/architecture/02_migration_and_acceptance.md)。

## 本地开发

首次使用时将 [config.example.toml](config.example.toml) 复制为项目根目录 `config.toml`，填写账号配置后启动；已有 `config.toml` 时直接编辑。实际配置文件已被 Git 和 Docker 构建上下文忽略。

```bash
uv sync --locked
uv run uvicorn src.main:app --host 127.0.0.1 --port 5123 --reload
```

Windows 本地开发优先使用 `127.0.0.1`，避免 `localhost + --reload` 带来额外延迟。

服务启动后可访问自动生成的 API 文档：

```text
Swagger UI  http://127.0.0.1:5123/docs
Scalar UI   http://127.0.0.1:5123/scalar
ReDoc       http://127.0.0.1:5123/redoc
OpenAPI     http://127.0.0.1:5123/openapi.json
```

本地定义的公开 operation 提供 summary、行为/副作用说明、tag 和成功响应说明；CFB 文档直接同步上游，离线测试核对快照一致性。

本地业务路由采用 strict query contract：未声明或拼错的 query 参数返回 422。CFB 路由原样转发参数，由上游校验。OpenAPI 页面展示 query 参数以及 JSON/binary 成功响应 schema。

CFB 接入独立运行的 `cn-futures-bridge`：启用 `[cfb]` 与 `service="cfb"` 白名单后，八条同名 `/cfb` 路由复用本项目鉴权，将 `sandbox/live` 参数、请求体和上游响应原样转发；只限制等待时间，不自动重试。`just sync-cfb-docs` 直接从上游 `/openapi.json` 同步接口文档，`just bru-cfb-readonly` 运行默认模拟盘查询示例。配置与容器地址见 [CFB 薄转发](docs/cfb/01_proxy.md)。

CTP 使用可选依赖：`uv sync --locked --extra ctp`，启动时使用 `uv run --no-sync uvicorn src.main:app --host 127.0.0.1 --port 5123`（或 `just serve-ctp`）。在 `config.toml` 中填写 `[ctp.test]`（SimNow 等模拟盘）和/或 `[ctp.live]`（实盘），通过 `mode=sandbox/live` 选择。Docker 镜像默认安装 CTP。市价与限价路由分开，查询返回详细 CTP 原生字段；配置、返回语义和示例见 [CTP 交易薄转发](docs/ctp/01_design.md)。

CTP extra 使用 VeighNa 官方 `vnpy_ctp` 源码构建的固定补丁版本，只编译、安装和加载交易扩展，不安装 `vnpy`、Qt 或 CTP 行情二进制。账户配置继续使用原来的 `[ctp.test]`、`[ctp.live]` 和服务白名单。补丁修复退出线程等待、积压回调内存释放和中文回报解码；原生交易库保持上游原样。本地与 Docker 使用相同依赖，来源和重建方式见 [依赖说明](vendor/vnpy_ctp/README.md)。

执行 `just update-ctp` 可跟随 VeighNa 官方最新稳定版升级，也可用 `just update-ctp 6.7.11.4` 指定版本：校验官方源码、应用补丁、核对 HTTP 回报字段、更新锁文件、安装并运行离线回归。上游结构或字段变化时会停止并要求适配。升级通过后再构建新镜像；后台进程使用镜像中锁定的版本。

期货公司联调使用 [script/ctp_assessment.py](script/ctp_assessment.py)：`just ctp-assessment`。通过 `uv run --with` 在独立临时环境中安装完整 VeighNa Trader、官方 CTP 网关及风控模块，打开图形界面；不改变后台依赖。默认复用 `config.toml` 的 `ctp.test`，传 `--mode live` 读取 `ctp.live`，在界面点击“系统 → 连接CTP”开始。评测的 `production_mode=false` 与 SimNow 的 `true` 要区分，详见 [联调说明](docs/ctp/02_assessment.md)。

当前交易状态可查询 `GET /tq/fetch_trading_status?symbol=SHFE.rb2610`（需要 TQ 交易状态权限）或 `GET /ctp/fetch_trading_status?exchange_id=SHFE&product_id=rb`（默认模拟盘 `sandbox`）。两条状态路由直接读取最新快照，不等待网络或其他 SDK 查询；TQ 新合约首次登记后台订阅并先返回未知。两者均返回 `is_open/raw_status/reason`：仅连续交易为 true，明确的其他阶段为 false，断线、超时或未收到状态为 null；CTP 还返回完整原生通知。参数及类型见 `/docs`，Bruno 已提供对应示例。

公共时间可查询 `GET /system/fetch_time`：沿用 Bearer 鉴权，无参数，每次直接请求[币安公共时间接口](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/general#time)，返回原始 `{"serverTime": 1789689600123}`（Unix 毫秒）。不需要币安账号或交易服务白名单，容器只需能访问 `https://api.binance.com/api/v3/time`。上游请求等待限时 5 秒，无重试、时间缓存或本机时间回退；超时返回 504，网络、上游 HTTP 或数据错误返回 502。该值对应上游生成响应的时刻，没有补偿网络延迟，也不会修改系统时间。Bruno 示例见 [SYSTEM/fetch_time.bru](bruno/SYSTEM/fetch_time.bru)。

可选的 DuckDB cache 配置（省略时使用以下默认值）：

```toml
[ohlcv_cache]
database_path = "./data/cache/ohlcv.duckdb"
max_rows_per_series = 2000000
max_rows_total = 20000000
```

## 配置

配置使用 TOML 原生分组表达层级，例如 `[tq]`、`[ctp.test]`、`[cfb]`、`[users.admin]`；CCXT/TQ/CTP/CFB 统一由 `[[service_whitelist]]` 数组表启用。填写配置不会自动启用服务。完整字段和注释见 [config.example.toml](config.example.toml)。

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

启动时按白名单顺序初始化：CCXT 加载 markets，TQ 建立连接并启动持续消息循环，CTP 完成认证、登录、结算确认，CFB 创建 HTTP 客户端。全部完成后才接受 HTTP 请求；任一失败则释放资源并退出。未列入白名单的实例不初始化，请求返回 `503 SERVICE_NOT_ENABLED`。空白名单可以正常运行鉴权、文档和健康检查。

启动被取消时，停止初始化后续服务，等待正在执行的 SDK 初始化结束后统一释放资源；取消不会强行中断 SDK 调用。

配置只在进程启动时读取一次，包括白名单和账号密码；请求和重连都复用内存快照，中途修改文件不会生效。`/readyz` 返回初始化结果，如 `ccxt/binance/future/sandbox`、`tq`、`ctp/sandbox`、`cfb`。这是启动就绪检查，不发起实时网络探测；CFB 客户端就绪不代表上游终端已经登录。具体合约的首次订阅仍然按需进行。

现有 TOML 白名单一次性迁移：`uv run --no-sync python scripts/migrate_service_whitelist.py`。迁移保留原 CCXT 身份，并将原来已配置的 TQ/CTP 模式显式列入白名单；核对所有配置后生成 `config.toml.before-service-whitelist.bak` 备份，原配置和备份均为 `0600`。新旧白名单不能混用，迁移后重启服务。

Bruno 的启动就绪和禁用服务检查见 [SERVICE LIFECYCLE](bruno/SERVICE%20LIFECYCLE/README.md)，其中提供没有外部账号的测试配置。

`SECRET` 是根字段，必须放在所有分组之前；用户名含点时用 `[users."alice.dev"]`。字符串使用 TOML 引号与转义规则；密码中的 `$`、`${...}` 原样保留，不展开环境变量。TOML 没有 `null`，可选字段不填时直接省略。

默认只读取根目录 `config.toml`，可通过 `CCXT_PROXY_CONFIG_PATH=/absolute/path/settings.toml` 指定其他文件，相对路径按项目根目录解析。环境变量仅用于选择文件，不覆盖账号字段。应用、Bruno 和调试脚本共用同一个配置加载入口。修改文件后重启服务。

已有部署从旧 `.env` 一次性迁移：

```bash
uv sync --locked
uv run --no-sync python scripts/migrate_config_to_toml.py
```

迁移会生成权限为 `0600` 的 `config.toml`，逐项核对账号和配置，再将原文件保留为 `.env.bak`（同为 `0600`）。已存在目标文件或备份时拒绝覆盖。仍使用旧 JSON 的部署可加 `--source data/config.json`，备份为 `data/config.json.bak`。应用不再读取旧 JSON 或 `.env`；旧 `CCXT_PROXY_ENV_FILE` 需改为 `CCXT_PROXY_CONFIG_PATH`。

## 测试

```bash
just test
```

`Test/online` 只访问 live 服务并且只执行读取；sandbox 和其他有状态操作只属于 `just debug*`，都不进入默认离线测试。

Offline suite 通过 `CCXT_PROXY_CONFIG_PATH=Test/fixtures/config.toml` 使用仓库内的无凭证配置。Live online recipe 显式切回 `./config.toml`；Bruno 仅在执行 `just bru-*` 时按需通过相同加载器读取登录用户，普通 `just test/lint/check` 不加载 Bruno 凭证。

聚合运行 CCXT 与 TQ 只读 live online tests：

```bash
just test-online
```

仅运行 CCXT Futures 只读 live online tests：

```bash
just test-ccxt-online
```

这些 online 入口只验证 public live market data，不初始化 sandbox identity，不检查 private account credential，不调用下单、撤单、平仓、设置类 API，也不发送 Telegram 消息。

## Docker

```bash
docker compose up -d --build
```

或运行已构建镜像：

```bash
docker run -d -p 5123:8000 \
  -v ~/ccxt-proxy2:/app/data \
  -v "$PWD/config.toml:/app/config.toml:ro" \
  -e PYTHONUNBUFFERED=1 \
  --name ccxt-proxy2 \
  --restart=always \
  hxse/ccxt-proxy2:latest
```

DuckDB cache 第一版只支持一个 Uvicorn process 对同一 database file 读写，不要通过 `--workers` 扩展写进程。

Compose 会把项目根目录 `config.toml` 只读挂载到 `/app/config.toml`；启动前需要创建该文件。配置文件不会写入镜像。
