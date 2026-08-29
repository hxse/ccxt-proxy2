# TQ 行情转发、Route 与 Lifecycle

> **Status: Implemented.** TQ 三个 Route 的语义保持不变，内部直接处理 TqSdk 返回的 Pandas DataFrame。

## 1. 边界

TQ 是独立 thin-forward data source，不是 CCXT OHLCV Provider adapter。

- 不支持 `since`。
- 不支持 CCXT `limit/enable_cache` 语义。
- 不接入 `DuckDbOhlcvCache`。
- 不做 window estimator、snapshot anchor、successor proof 或外层磁盘 cache。
- 复用 TqSdk 同一 `TqApi` 实例中的 realtime serial。
- 只做 HTTP validation、placeholder trim、time-axis validation 和 JSON serialization。

旧草稿曾考虑将 TQ 适配为 `LatestLimit/SinceLimit/SinceLatest` 以及 `AfterCount/BeforeCount`，并通过逐级扩大 `data_length` 读取 cache gaps。该方案已否决，见 [TQ cached Route 历史档案](../design_history/04_tq_cached_routes_rejected.md)。

## 2. Routes

```text
GET /tq/fetch_ohlcv
GET /tq/fetch_tick
GET /tq/fetch_underlying_symbol
```

三个 Route 都复用项目已有 `/auth/token` Bearer authentication，不增加 TQ 专用 HTTP token、query token 或 Basic Auth。

## 3. TqSdk 能力

第一版只使用 realtime/free-account 能力：

```python
api.get_kline_serial(symbol, duration_seconds, data_length, adj_type=None)
api.get_tick_serial(symbol, data_length, adj_type=None)
api.query_symbol_info(symbol)
api.query_his_cont_quotes(symbol, n=n)
```

`data_length` 范围是 `1..10000`，默认 10000。它是固定宽度滚动窗口的上限，不保证响应一定有该数量。服务运行更久不会让同一 serial 返回超过 `data_length` 的 rows。

专业版历史接口 `get_kline_data_series/get_tick_data_series` 不属于第一版能力，也不依赖其 `~/.tqsdk/data_series_1` 磁盘 cache。

## 4. TqApi serial reuse

同一 `TqApi` 中，serial identity 包含 request length：

```text
kline: (tuple(symbol), duration_seconds, data_length, adj_type)
tick : (symbol, data_length, adj_type)
```

相同 key 复用已有 serial；频繁改变同一 symbol/period/adj-type 的 `data_length` 会创建新 serial/chart。不同 symbol 或 timeframe 使用不同长度是允许的。TQ 不会为从未请求的 symbol/period 预热数据。

Route/OpenAPI description 必须说明此 cache key，建议调用方对同一 key 使用稳定 `data_length`。

## 5. Symbol contract

TQ symbol 本身表达数据类型，不增加 `data_type`：

- 具体合约：`SHFE.rb2505`；
- 主连：`KQ.m@SHFE.rb`；
- 指数/加权：`KQ.i@SHFE.rb`。

调用方传完整 symbol，服务端不拼接。`symbol: str | list[str]` 的 HTTP 表达使用重复同名 query parameter：

```text
?symbol=SHFE.au2508&symbol=CFFEX.IF2506
```

服务只聚合 `symbol`，不提供平行 `symbols` 参数。

## 6. `/tq/fetch_ohlcv`

| 参数 | 类型 | 必填 | 语义 |
| --- | --- | --- | --- |
| `symbol` | `str | list[str]` | 是 | 透传 TQ symbol |
| `duration_seconds` | positive `int` | 是 | K 线周期，秒 |
| `data_length` | `1..10000` | 否 | 默认 10000 |
| `adj_type` | supported string/`None` | 否 | `F/B/FORWARD/BACK` 或空 |

响应保留 TQ 字段名，不转成 CCXT six-column rows。单 symbol 常见字段：

```text
id, datetime(ns), open, high, low, close, volume,
open_oi, close_oi, symbol, duration
```

多 symbol 保留 TQ 原始 `open1/close1/symbol1...` 字段。

## 7. `/tq/fetch_tick`

| 参数 | 类型 | 必填 | 语义 |
| --- | --- | --- | --- |
| `symbol` | `str` | 是 | TQ 合约 |
| `data_length` | `1..10000` | 否 | 默认 10000 |
| `adj_type` | supported string/`None` | 否 | TQ 复权参数 |

返回 TQ Tick 时间序列，而 `/ccxt/fetch_tickers` 是当前快照，两者不对齐。常见字段包括 `last_price/average/highest/lowest/bid_price1/ask_price1/volume/amount/open_interest`。

## 8. `/tq/fetch_underlying_symbol`

用于将 `CONT` 主连 symbol 解析为当前实际合约，并可选返回最近 N 个交易日的历史映射。

| 参数 | 类型 | 语义 |
| --- | --- | --- |
| `symbol` | `str | list[str]` | 一个或多个主连 symbol |
| `n` | positive `int | None` | 不传只返当前；传入时增加历史 |

主路径：

```python
info = api.query_symbol_info(symbol)
underlying_symbol = info["underlying_symbol"]
```

单 symbol 且 metadata 缺字段时可 fallback `api.get_quote(symbol).underlying_symbol`。`n` 显式传入时才调 `query_his_cont_quotes`。

响应 envelope：

```json
{
  "items": [{"symbol": "KQ.m@DCE.i", "underlying_symbol": "DCE.i2509"}],
  "history": [{"date": "2026-06-10", "symbol": "KQ.m@DCE.i", "underlying_symbol": "DCE.i2509"}]
}
```

History 原始 Pandas 宽表必须转为长表，不将 symbol 作为动态 JSON key。非 `CONT` 返回 422；`CONT` 缺 underlying 也返回 422。

## 9. `TqManager` lifecycle 与 lock

一个 process 使用一个 singleton `TqManager`，惰性持有一个 `TqApi`。每次创建新 API 会丢失 serial reuse，因此禁止 per-request initialization。

Application lifespan shutdown 调用幂等 `TqManager.close()`，在持有同一 FileLock 时关闭并清空 `TqApi`；不只依赖进程退出回收状态客户端。

`TqApi` 是状态客户端，所有访问继续通过 TQ 自己的 `FileLock`。这是独立于 CCXT `threading.Lock` 和 DuckDB write lock 的锁域。

多 Uvicorn worker 会产生多个 `TqApi` 和多份进程内 serial cache；第一版部署保持 single process。

## 10. Config/auth

TQ config 只用于服务连接 TqSdk，不是 HTTP 入口鉴权：

```json
{
  "tq": {
    "username": "...",
    "password": ""
  }
}
```

`tq` 可选；未配置时首次访问返回 `TQ_NOT_CONFIGURED`，不影响其他路由启动。未登录的 `/tq/*` 仍由项目统一认证层返回 401。

## 11. Dependencies

当前实现显式声明：

```toml
"tqsdk"
"pandas"
"filelock"
```

Pandas 是项目 direct dependency；TQ 数据路径不再 import Polars。

数据处理、错误码和测试见 [TQ Pandas 数据规范](02_data_processing_and_tests.md)。
