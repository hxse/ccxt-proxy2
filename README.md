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

`Test/online` 和 `debug/route_tests` 会访问外部服务或沙盒账户，不属于默认离线测试。

仅运行 Futures OHLCV 只读 online smoke test：

```bash
just test-ccxt-online
```

该入口不调用下单、撤单、平仓或设置类 API。

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
