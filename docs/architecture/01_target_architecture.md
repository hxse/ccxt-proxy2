# CCXT/OHLCV 目标架构

> **Status: Implemented design.** 本文是当前 CCXT/OHLCV 生产架构的权威边界。

## 1. 目标

- 将 Provider network 与本地 cache 完全解耦。
- 三类 CCXT OHLCV 用户语义由三个明确 Route 表达。
- Binance/Kraken Futures 共用自分页、retry、overlap 和固定周期连续性校验，Provider 差异收口到 `CcxtClient`。
- DuckDB cache 只做计算与本地 IO，绝不请求网络。
- TQ 继续作为独立 thin-forward，不接入 CCXT cache。
- 删除旧新并行实现，降低长期维护成本。

## 2. 顶层数据流

```text
FastAPI Route
    │
    ├── CCXT account/order/market/OHLCV
    │       └── ExchangeManager → CcxtClient
    │                               ├── provider network methods
    │                               └── DuckDbOhlcvCache
    │
    ├── TQ Route → TqManager → TqSdk/Pandas
    └── Telegram Route → TelegramManager → httpx
```

目标只引入两个 CCXT/OHLCV 核心 class：

```text
CcxtClient
└── DuckDbOhlcvCache
    └── embedded DuckDB
```

`Route` 只调用 `CcxtClient`。`CcxtClient` 负责编排 cache read、network fetch、cache write 与 response transform；Cache 不接受 callback，也看不到 Provider page。

## 3. 责任边界

| 层 | 负责 | 禁止 |
| --- | --- | --- |
| Route | HTTP schema、鉴权、业务错误映射 | Provider 分页、SQL、cache plan |
| ExchangeManager | Client lifecycle/capability registry | 向 Route 返回裸 CCXT instance |
| CcxtClient | CCXT API、自分页、retry、canonical rows、cache orchestration | DuckDB SQL 散落在 Client |
| DuckDbOhlcvCache | segment 命中、merge、transaction、capacity | CCXT/TQSDK/FastAPI、network、Provider params |
| TqManager | TqApi lifecycle、FileLock、thin-forward | CCXT RouteIntent、DuckDB cache |

## 4. 运行模型

- 第一版只允许一个 Uvicorn process 读写同一 DuckDB native file。
- 每个 exchange/market/mode/credential identity 只有一个长期存活的 `CcxtClient`/CCXT instance。
- 每个 CCXT instance 有独立 `threading.Lock`，只锁单次底层 HTTP attempt。
- 每个 DuckDB file 有独立 process-local write lock；读取依赖 snapshot isolation。
- CCXT 与 DuckDB 不使用 `FileLock`；TQ 保持现有 `FileLock`。
- 配置或 Exchange registry 初始化失败时 application fail-fast，由单实例 Podman restart policy 处理；`/readyz` 不承担启动失败后的 degraded 服务。
- FastAPI application 不注册 gzip/response-compression middleware；部署需要压缩时由前置 Nginx/Caddy 按 MIME type 统一处理。
- Network 不在 DuckDB write lock 内运行。
- Application shutdown/reinitialize 会幂等关闭 CCXT sessions、DuckDB connections、TqApi 和 Telegram HTTP client；CCXT close 与 attempt 串行，DuckDB close 等待 active readers，关闭后的旧对象拒绝新操作，不得只依赖进程退出回收。

## 5. Provider 能力不强行对称

正式保证的生产范围只有：

- Binance USDⓈ-M linear Futures，不支持 COIN-M/inverse；
- Kraken Futures。

两者支持三类 OHLCV Route、共用自分页和 DuckDB cache。完整分页的支持域是正常、固定周期连续的 crypto OHLCV；异常 gap 或满页 no-progress 必须 fail-fast，不建设自适应补拉系统。

Binance Spot 和 Kraken Spot 继续暴露已有 Route，但只属于 best-effort，不作正式可用性承诺。Kraken Spot 只 thin-forward，不自分页、不读写 cache、不支持 `SinceLatest`；Kraken Spot sandbox 明确不支持。所有 Provider 均在方法入口检查 method、timeframe 和 symbol market-scope capability；Binance inverse 统一返回稳定 `NOT_SUPPORTED`，其他未知 symbol 返回 `INVALID_PROVIDER_REQUEST`。

## 6. 不变量

1. `timeframe` 只用于完整 crypto Provider 的固定周期 fail-fast validation；不用于构造下一根 cursor，也不与本机时间一起推断尾根完成状态。
2. Provider 分页始终使用 inclusive overlap，合并后按 `time` 去重。
3. Response 和 cacheability 是两个维度：用户默认收到尾根，cache 可以拒绝保存它。
4. Cache 中的每一根 row 都曾获得严格更晚 successor 证据。
5. Cache 只复用一个最佳 prefix segment，不建设通用多缺口 Resolver。
6. 当前 target 不保证 Provider 历史删除/回填能被主动发现。

## 7. 非目标

- 旧 cache 格式迁移、兼容读或双轨运行。
- 通用 callback-driven CacheEngine、Forward/Backward Walker 和中位数密度估算。
- Parquet online cache、手动文件分片、JSON proof log 或物理文件 cleanup。
- DuckDB 后台服务或多进程 writer。
- TQ 的三类 CCXT-style cached Route。
- streaming response、后台长任务、LRU 或精确磁盘 byte 上限。

## 8. 目标代码形状

```text
src/
  tools/
    exchange_manager.py
    ccxt_client.py
    ccxt_trading.py     # private account/order mixin
    ccxt_ohlcv.py       # private manual pagination
    ccxt_transport.py   # private lock/retry boundary
    ccxt_errors.py      # private stable Provider error taxonomy
    tq_manager.py
    tq_data_source.py
  cache_tool/
    duckdb_ohlcv_cache.py
    duckdb_schema.py       # private schema/version initialization
    models.py
  router/
    trader_router.py
    tq_router.py
```

实际命名可在实施时微调，但不得重新建立 `CcxtClient` 之外的平行 utils/adapter public facade。
