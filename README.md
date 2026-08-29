# ccxt-proxy2

`ccxt-proxy2` 是一个带鉴权的 FastAPI 代理服务，当前提供 CCXT 交易所、TQ 行情与 Telegram 转发路由。

## 文档状态

CCXT/OHLCV 的破坏性重构已经完成。当前生产路径是 `Route → CcxtClient → DuckDbOhlcvCache`；旧 Parquet/proof-log/callback cache 与平行 CCXT utils/adapter 已移除。

正式保证的 Provider 范围是 Binance USDⓈ-M linear Futures 和 Kraken Futures。Binance/Kraken Spot 路由只作 best-effort 暴露，不承诺完整可用性；Binance COIN-M/inverse 与 Kraken Spot sandbox 不支持。

从 [文档索引](docs/README.md) 开始阅读。重构的总体边界见 [目标架构](docs/architecture/01_target_architecture.md)，代码切换顺序见 [迁移与验收](docs/architecture/02_migration_and_acceptance.md)。

## 本地开发

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

每个公开 operation 都必须提供 summary、行为/副作用说明、tag 和成功响应说明；该约束由离线 OpenAPI contract test 固化。

业务路由采用 strict query contract：未声明或拼错的 query 参数返回 422，不会被静默忽略。OpenAPI 页面展示每个 query 参数的说明以及 JSON/binary 成功响应 schema。

可选的 DuckDB cache 配置（省略时使用以下默认值）：

```json
{
  "ohlcv_cache": {
    "database_path": "./data/cache/ohlcv.duckdb",
    "max_rows_per_series": 2000000,
    "max_rows_total": 20000000
  }
}
```

## 测试

```bash
just test
```

`Test/online` 只访问 live 服务并且只执行读取；sandbox 和其他有状态操作只属于 `just debug*`，都不进入默认离线测试。

Offline suite 通过 `CCXT_PROXY_CONFIG_PATH=Test/fixtures/config.json` 使用仓库内的无凭证配置，不读取 `data/config.json`。Live online recipe 显式切回 `./data/config.json`；Bruno 仅在执行 `just bru-*` 时由 `scripts/run_bruno.py` 按需读取登录用户，普通 `just test/lint/check` 不加载 Bruno 凭证。

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
  -e PYTHONUNBUFFERED=1 \
  --name ccxt-proxy2 \
  --restart=always \
  hxse/ccxt-proxy2:latest
```

DuckDB cache 第一版只支持一个 Uvicorn process 对同一 database file 读写，不要通过 `--workers` 扩展写进程。
