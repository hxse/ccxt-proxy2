# TQ 行情接口与生命周期

## 边界

TQ 获取算法独立于 CCXT；普通 K 线只使用最新数量窗口，Tick 不落项目缓存。OHLCV 单次请求 min(N,10000)，成功后复用共享缓存的原有时间戳交集与片段证明；不用 since、interval 推算或窗口试探。

普通行情详情见下文，片段身份和持久化规则见[共享缓存规范](ohlcv_cache_storage.md)。日历和历史映射走[自有元数据链](tq_metadata.md)。

## HTTP 路由

```text
GET /tq/fetch_ohlcv
GET /tq/fetch_tick
GET /tq/fetch_underlying_symbol
GET /tq/fetch_trading_calendar
GET /tq/fetch_trading_status
```

所有 Route 都复用项目已有 `/auth/token` Bearer authentication，不增加 TQ 专用 HTTP token、query token 或 Basic Auth。

## TqSdk 能力

现有行情、主连和日历能力：

```python
api.get_kline_serial(symbol, duration_seconds, data_length, adj_type=None)
api.get_tick_serial(symbol, data_length, adj_type=None)
```

SDK `data_length` 范围为 `1..10000`，默认 10000；项目 OHLCV 请求可指定最多 100000，超出的连续历史由项目缓存补充。它是固定宽度滚动窗口的上限，不保证响应一定有该数量。服务运行更久不会让同一 serial 返回超过 `data_length` 的 rows。

因此现有 `/tq/fetch_ohlcv` 不能证明“某个指定历史起点到终点是否完整覆盖”：请求本身没有日期边界，短于 `data_length` 也是已定义的正常结果。若未来必须对任意久远旧合约窗口提供覆盖证明，需要单独增加带 `start_dt/end_dt` 的历史数据能力；不能把短 serial 一律改成错误，也不能由交易日历替代 K 线 coverage proof。

专业版历史接口 `get_kline_data_series/get_tick_data_series` 不属于当前实现能力，也不依赖其 `~/.tqsdk/data_series_1` 磁盘 cache。

SDK 的静态日历/历史映射缓存入口及 query_symbol_info 当前标的入口已硬禁，使用项目自有官方源转换；自动刷新、在线时间和已生效边界见[元数据规范](tq_metadata.md)。

## TqApi 序列复用

同一 `TqApi` 中，serial identity 包含 request length：

```text
kline: (tuple(symbol), duration_seconds, data_length, adj_type)
tick : (symbol, data_length, adj_type)
```

相同 key 复用已有 serial；频繁改变同一 symbol/period/adj-type 的 `data_length` 会创建新 serial/chart。不同 symbol 或 timeframe 使用不同长度是允许的。TQ 不会为从未请求的 symbol/period 预热数据。

Route/OpenAPI description 必须说明此 cache key，建议调用方对同一 key 使用稳定 `data_length`。

## 合约代码契约

TQ symbol 本身表达数据类型，不增加 `data_type`：

- 具体合约：`SHFE.rb2505`；
- 主连：`KQ.m@SHFE.rb`；
- 指数/加权：`KQ.i@SHFE.rb`。

普通 OHLCV 只接受一个完整 symbol，重复同名参数（含相同值）返回 400 TQ_MULTIPLE_SYMBOLS_NOT_SUPPORTED；内部列表也拒绝。主连映射同样只允许单个 symbol。

TQ 行情没有 live/sandbox 参数，不受交易账户环境配置影响。主连与加权都是行情身份，不能据名称解释成可以直接下单的实际合约。

## `/tq/fetch_ohlcv`

| 参数 | 类型/默认 | 语义 |
| --- | --- | --- |
| symbol | 必填 str | 单个完整主连、加权或实际合约 |
| duration_seconds | 必填正整数 | 秒；超过一天为整天倍数 |
| data_length | 1..100000，默认 10000 | 最多响应数量；SDK 取 min(N,10000) |
| adj_type | None 或 F/B/FORWARD/BACK | 空按 None |
| enable_cache | true | false 禁项目读写和休市兜底 |

响应保留 records、整数纳秒 datetime、SDK id 和秒制 duration，以及 open/high/low/close/volume/open_oi/close_oi。合法 nullable 值可返回，核心缺值的可持久化批次整批不写；不能删掉中间坏行求连续。

SDK 在所属线程取得稳定副本；数据库和响应合并在其锁外。网络窗口完整提交，缓存内部排除未知末根；SDK 已满足 N 时直接返回，不为响应再读库。不足 N 时用 read_connected_history 获取相连历史，网络同时间值胜出，裁至本次窗口上界并取最新 N 根。纯网络与合并结果最终再次验证。

SDK 空/短结果均为成功，不检查交易状态，不补第二个窗口。无交集保持独立片段；未知末根不能单独连接。可自然复用超过一万根历史。

仅网络/服务失败且缓存启用时检查一次状态：实际合约检查自身，主连和加权通过统一历史映射入口取得参考交易日的实际合约；不读 SDK 当前标的。只有 raw_status=NOTRADING 且 is_open=false 返回原序列最新连续缓存（空库可 []）；竞价/开市返回原错误，未知/解析失败为 502 TQ_TRADING_STATUS_UNAVAILABLE，权限错误保持 403。缓存读失败为 500 TQ_CACHE_READ_FAILED。正常缓存读写错误不丢成功网络响应；容量失败为 507。

不按日历、时间段或行情不变化推算休市。大于一周的普通行情只作 SDK 单窗口薄转发，无项目缓存和休市兜底。

```text
GET /tq/fetch_ohlcv?symbol=KQ.m@SHFE.rb&duration_seconds=300&data_length=20000&enable_cache=true
```

## `/tq/fetch_tick`

| 参数 | 类型 | 必填 | 语义 |
| --- | --- | --- | --- |
| `symbol` | `str` | 是 | TQ 合约 |
| `data_length` | `1..10000` | 否 | 默认 10000 |
| `adj_type` | supported string/`None` | 否 | TQ 复权参数 |

返回 TQ Tick 时间序列，而 `/ccxt/fetch_tickers` 是当前快照，两者不对齐。常见字段包括 `last_price/average/highest/lowest/bid_price1/ask_price1/volume/amount/open_interest`。

## 日历与主连映射

`/tq/fetch_underlying_symbol` 保留当前 items；历史改用成对 start_time/end_time 整数纳秒，返回真实换月节点和 old_symbol，旧 n 与多 symbol 退出。`/tq/fetch_trading_calendar` 保留自然日闭区间。两者 enable_cache 默认 true；映射无论是否带范围都取在线时间、最新单根参考时间并重新获取官方历史源，当前 items 与历史使用同一来源。

详细输入、输出、刷新、D 上界和缓存规则统一见[元数据规范](tq_metadata.md)；可选旧合约价格见[换月过渡](tq_transition.md)。这些请求的 HTTP 下载不占 SDK 线程，生命周期先取消/等待元数据任务，再关闭 SDK 和共享缓存。

## TqManager 生命周期与锁

只有 service_whitelist 包含 tq 时，独立初始化任务才调用 TqManager.initialize(cache)，等待其专用 SDK 线程创建并初始化 TqApi；这个等待不阻止 HTTP 或其他身份。TQ 初始化中/失败、关闭或工作线程退出时，路由立即返回 503 SERVICE_NOT_READY、service=tq。请求始终复用实例，不触发初始化或重建。

专用线程串行执行 TQ SDK 调用，空闲时持续调用 `wait_update()` 处理网络消息；每个业务任务结束后，也以立即到期的 deadline 推进一次订阅和消息处理，避免持续排队的查询阻塞状态更新，且不额外等待网络。单个正在执行的 SDK 调用仍需完成后才能处理下一轮消息。关闭时停止接收新任务，等待在途调用完成，拒绝排队任务，并在同一线程关闭 `TqApi`。SDK 副本清洗在 TqClient 完成，缓存编排在 SDK 线程外；关闭等待在途业务退出后才由应用关闭共享库。

`TqApi` 是状态客户端，所有访问继续通过 TQ 自己的 `FileLock`。这是独立于 CCXT `threading.Lock` 和 DuckDB write lock 的锁域。

多 Uvicorn worker 会产生多个 `TqApi` 和多份进程内 serial cache；当前实现部署保持 single process。

## 配置与鉴权

启动快照、白名单与 HTTP 鉴权的共同规则见[配置规范](configuration.md)。

`config.toml` 中的 TQ 配置只用于服务连接 TqSdk，不是 HTTP 入口鉴权：

```toml
[tq]
username = "..."
password = ""
```

`tq` 可选；未列入白名单时不创建 SDK/连接，请求返回 503 `SERVICE_NOT_ENABLED`；列入白名单但缺配置则启动失败。配置只在启动时读取，中途修改文件无效。未登录的 `/tq/*` 仍由项目统一认证层返回 401。

## 依赖

当前实现显式声明：

```text
tqsdk
pandas
filelock
```

Pandas 是项目 direct dependency；TQ 数据路径不再 import Polars。

数据处理、错误码和测试见 [TQ Pandas 数据规范](tq_processing.md)。

## `/tq/fetch_trading_status`

`GET /tq/fetch_trading_status?symbol=SHFE.rb2610` 读取后台订阅的最新状态快照，输入单个完整 TQ 合约代码，复用既有实例和合约订阅。

```json
{"symbol":"SHFE.rb2610","is_open":true,"raw_status":"CONTINOUS","reason":null}
```

`is_open=true` 只表示连续交易；`AUCTIONORDERING`（集合竞价报单）和 `NOTRADING`（非交易）返回 false。编码沿用 [TQ 官方 TradingStatus 定义](https://doc.shinnytech.com/tqsdk/latest/reference/tqsdk.objs.html#tqsdk.objs.TradingStatus)，包括 `CONTINOUS` 的原始拼写。

启动时保存账户的 `tq_trading_status` 权限状态，未开通时直接返回 HTTP 403 `TQ_TRADING_STATUS_PERMISSION_DENIED`。其余行情和日历接口不增加此权限要求。

SDK 工作线程可用但无法确认合约状态时 HTTP 200、is_open=null；reason 为 not_received（尚未收到）、disconnected（状态连接断线）、unavailable（状态源不可用）、unrecognized_status（未知编码）。未知编码保留 raw_status，其余未知结果不携带旧状态。SDK 初始化失败或工作线程退出则先由服务门禁返回 503，不返回失效快照。未启用 TQ 返回 SERVICE_NOT_ENABLED；白名单引用缺失配置仍使应用配置校验失败。

TQ 交易状态使用独立的 `ts` 连接。服务只保存这个连接收到的最新状态，收到连接切换通知就清除旧状态，重连后等待新状态。由于 SDK 会去掉值未变化的 diff，`TradingStatusTqApi` 在 `_fetch_msg` 合并去重前观察通知；不改 SDK 订阅、缓存和重连策略。离线测试直接运行已安装 SDK 的消息循环验证这一兼容点。

连接初始化等待发生在启动阶段。HTTP 读取不调用 SDK、不经过行情操作队列，也不等待网络；使用独立短锁读取快照。首次查询只登记一次订阅意向，立即返回 null/not_received；SDK 线程在业务任务之间或空闲时通过协程发起订阅，避开同步 get_trading_status 的 30 秒等待分支。订阅完成前的重复查询不重复登记，收到推送后查询返回新状态。后台订阅错误在后续查询中明确返回。没有新状态变化不代表休市，也不设置状态年龄阈值。节假日或断网时仅报告未知，不推算交易日/品种时段，不保证固定 HTTP 响应时限。TQ SDK 尚未检测到的网络故障也无法提前识别。

状态路由使用异步 HTTP 入口；内存鉴权及参数校验也不占用同步线程池，因此不会排在耗时行情查询后。订单、持仓、资金及其他行情查询仍走各自原有 SDK 队列。

## 普通 K 线等待预算

队列等待与 serial 就绪共用单调时钟 10 秒预算。get_kline_serial 在 SDK 协程内登记，避免其同步长等待；再短 wait_update 至 is_serial_ready。超时返回 504 TQ_DATA_TIMEOUT，排队到期的操作不启动，退出业务不迟到写库。只取消本次外层登记任务，SDK 共享 serial 保留。停止服务通知等待退出，消息观察钩子与 Tick 原职责保持。
