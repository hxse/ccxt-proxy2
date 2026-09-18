# TQ 行情转发、Route 与 Lifecycle

> **Status: Implemented.** TQ realtime Route 保持 thin-forward，并公开中国期货交易日历和合约交易状态查询。

## 1. 边界

TQ 是独立 thin-forward data source，不是 CCXT OHLCV Provider adapter。

- 不支持 `since`。
- 不支持 CCXT `limit/enable_cache` 语义。
- 不接入 `DuckDbOhlcvCache`。
- 不做 window estimator、snapshot anchor、successor proof 或外层磁盘 cache。
- 复用 TqSdk 同一 `TqApi` 实例中的 realtime serial。
- 只做 HTTP validation、placeholder trim、time-axis validation 和 JSON serialization。
- 未声明的 query 参数返回 422，不静默忽略拼写错误。

已否决方案曾考虑将 TQ 适配为 `LatestLimit/SinceLimit/SinceLatest` 以及 `AfterCount/BeforeCount`，并通过逐级扩大 `data_length` 读取 cache gaps。决策背景见 [TQ cached Route 历史档案](../design_history/04_tq_cached_routes_rejected.md)。

## 2. Routes

```text
GET /tq/fetch_ohlcv
GET /tq/fetch_tick
GET /tq/fetch_underlying_symbol
GET /tq/fetch_trading_calendar
GET /tq/fetch_trading_status
```

所有 Route 都复用项目已有 `/auth/token` Bearer authentication，不增加 TQ 专用 HTTP token、query token 或 Basic Auth。

## 3. TqSdk 能力

现有行情、主连和日历能力：

```python
api.get_kline_serial(symbol, duration_seconds, data_length, adj_type=None)
api.get_tick_serial(symbol, data_length, adj_type=None)
api.query_symbol_info(symbol)
api.query_his_cont_quotes(symbol, n=n)
api.get_trading_calendar(start_dt, end_dt)
```

`data_length` 范围是 `1..10000`，默认 10000。它是固定宽度滚动窗口的上限，不保证响应一定有该数量。服务运行更久不会让同一 serial 返回超过 `data_length` 的 rows。

因此现有 `/tq/fetch_ohlcv` 不能证明“某个指定历史起点到终点是否完整覆盖”：请求本身没有日期边界，短于 `data_length` 也是已定义的正常结果。若未来必须对任意久远旧合约窗口提供覆盖证明，需要单独增加带 `start_dt/end_dt` 的历史数据能力；不能把短 serial 一律改成错误，也不能由交易日历替代 K 线 coverage proof。

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

## 9. `/tq/fetch_trading_calendar`

薄转发 `TqApi.get_trading_calendar(start_dt, end_dt)`：

| 参数 | 类型 | 语义 |
| --- | --- | --- |
| `start_date` | ISO `date` | 中国期货日历的起始自然日（`Asia/Shanghai`），包含 |
| `end_date` | ISO `date` | 中国期货日历的结束自然日（`Asia/Shanghai`），包含 |

这两个参数是只表示 calendar date 的 `YYYY-MM-DD` 字符串，不是 UTC 毫秒时间戳，也不表示零点 instant。服务不会根据客户端、服务器或 UTC 时区换算日期；例如 `2026-09-01` 始终指中国期货日历中的 2026-09-01。响应 `date` 也使用这一语义，不表达夜盘时刻应归属的 trading day。

响应是闭区间内逐自然日记录：

```json
[
  {"date": "2026-09-11", "trading": true},
  {"date": "2026-09-12", "trading": false}
]
```

它表达中国期货通用交易日/休息日，不区分交易所或 symbol，也不负责夜盘到交易日的映射、交易时段、节前禁入或换月策略。请求超出 TqSdk 当前节假日数据覆盖范围时返回 422 `TQ_CALENDAR_RANGE_UNAVAILABLE`；成功结果必须完整覆盖每日闭区间，否则返回 502 `TQ_CALENDAR_INCOMPLETE`。不增加外层 cache。

## 10. `TqManager` lifecycle 与 lock

只有 `service_whitelist` 中包含 `service="tq"` 时，启动协调器才调用 `TqManager.initialize()`。它等待专用 SDK 线程创建 `TqApi` 并完成初始化，然后开始提供 HTTP 服务。请求始终复用实例，不触发初始化。

专用线程串行执行 TQ SDK 调用，空闲时持续调用 `wait_update()` 处理网络消息；每个业务任务结束后，也以立即到期的 deadline 推进一次订阅和消息处理，避免持续排队的查询阻塞状态更新，且不额外等待网络。单个正在执行的 SDK 调用仍需完成后才能处理下一轮消息。关闭时停止接收新任务，等待在途调用完成，拒绝排队任务，并在同一线程关闭 `TqApi`。数据转换仍由 TqClient 薄转发。

`TqApi` 是状态客户端，所有访问继续通过 TQ 自己的 `FileLock`。这是独立于 CCXT `threading.Lock` 和 DuckDB write lock 的锁域。

多 Uvicorn worker 会产生多个 `TqApi` 和多份进程内 serial cache；第一版部署保持 single process。

## 11. Config/auth

`config.toml` 中的 TQ 配置只用于服务连接 TqSdk，不是 HTTP 入口鉴权：

```toml
[tq]
username = "..."
password = ""
```

`tq` 可选；未列入白名单时不创建 SDK/连接，请求返回 503 `SERVICE_NOT_ENABLED`；列入白名单但缺配置则启动失败。配置只在启动时读取，中途修改文件无效。未登录的 `/tq/*` 仍由项目统一认证层返回 401。

## 12. Dependencies

当前实现显式声明：

```toml
"tqsdk"
"pandas"
"filelock"
```

Pandas 是项目 direct dependency；TQ 数据路径不再 import Polars。

数据处理、错误码和测试见 [TQ Pandas 数据规范](02_data_processing_and_tests.md)。

## 13. `/tq/fetch_trading_status`

`GET /tq/fetch_trading_status?symbol=SHFE.rb2610` 读取后台订阅的最新状态快照，输入单个完整 TQ 合约代码，复用既有实例和合约订阅。

```json
{"symbol":"SHFE.rb2610","is_open":true,"raw_status":"CONTINOUS","reason":null}
```

`is_open=true` 只表示连续交易；`AUCTIONORDERING`（集合竞价报单）和 `NOTRADING`（非交易）返回 false。编码沿用 [TQ 官方 TradingStatus 定义](https://doc.shinnytech.com/tqsdk/latest/reference/tqsdk.objs.html#tqsdk.objs.TradingStatus)，包括 `CONTINOUS` 的原始拼写。

启动时保存账户的 `tq_trading_status` 权限状态，未开通时直接返回 HTTP 403 `TQ_TRADING_STATUS_PERMISSION_DENIED`。其余行情和日历接口不增加此权限要求。

无法确认时 HTTP 200，`is_open=null`；`reason` 为 `not_received`（尚未收到）、`disconnected`（状态连接断线）、`unavailable`（连接/服务不可用）、`unrecognized_status`（未知编码）。未知编码保留 raw_status，其余未知结果不携带旧状态。未配置 TQ 仍返回原有配置错误。

TQ 交易状态使用独立的 `ts` 连接。服务只保存这个连接收到的最新状态，收到连接切换通知就清除旧状态，重连后等待新状态。由于 SDK 会去掉值未变化的 diff，`TradingStatusTqApi` 在 `_fetch_msg` 合并去重前观察通知；不改 SDK 订阅、缓存和重连策略。离线测试直接运行已安装 SDK 的消息循环验证这一兼容点。

连接初始化等待发生在启动阶段。HTTP 读取不调用 SDK、不经过行情操作队列，也不等待网络；使用独立短锁读取快照。首次查询只登记一次订阅意向，立即返回 null/not_received；SDK 线程在业务任务之间或空闲时通过协程发起订阅，避开同步 get_trading_status 的 30 秒等待分支。订阅完成前的重复查询不重复登记，收到推送后查询返回新状态。后台订阅错误在后续查询中明确返回。没有新状态变化不代表休市，也不设置状态年龄阈值。节假日或断网时仅报告未知，不推算交易日/品种时段，不保证固定 HTTP 响应时限。TQ SDK 尚未检测到的网络故障也无法提前识别。

状态路由使用异步 HTTP 入口；内存鉴权及参数校验也不占用同步线程池，因此不会排在耗时行情查询后。订单、持仓、资金及其他行情查询仍走各自原有 SDK 队列。
