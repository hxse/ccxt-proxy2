# TQ 数据处理、错误与验证

## 输入与输出

TqSdk 返回 Pandas DataFrame。Adapter 负责：

1. 别名/必需列检查；
2. 前置 placeholder trim；
3. dtype 转换；
4. time-axis 校验；
5. NaN/Infinity 转 JSON `null`；
6. records 输出；
7. underlying history wide-to-long；
8. trading calendar date/bool schema 与完整闭区间校验。

不做 CCXT six-column normalization，不强行删除 TQ raw fields。

## 占位行来源

Realtime serial 是固定宽度窗口。`data_length` 大于当前可用历史时，TqSdk 在前段放对象默认值，而不是返回一个纯短表。

Kline 常见默认：

```text
datetime = 0
open/high/low/close = NaN
volume/open_oi/close_oi = 0
```

Tick 常见默认：

```text
datetime = 0
关键价格 = NaN
数量/持仓字段可能 = 0
```

因此不能用“整行全空”或 volume==0 判断 placeholder。

## K 线占位行

前置 row 同时满足：

```text
datetime == 0
open/high/low/close 全部是 NaN/null
```

才是 Kline placeholder。

## Tick 占位行

前置 row 同时满足：

```text
datetime == 0
last_price/average/highest/lowest/bid_price1/ask_price1
全部是 NaN/null
```

不使用 volume、amount、open_interest 或 bid/ask volume 判断。

## 只裁前缀

```python
placeholder_mask = build_placeholder_mask(frame, kind)

valid_positions = np.flatnonzero(~placeholder_mask.to_numpy())
trimmed = (
    frame.iloc[0:0].copy()
    if len(valid_positions) == 0
    else frame.iloc[valid_positions[0] :].copy()
)
```

只能删连续前缀。首个有效 row 之后再出现 placeholder/非法 time 必须返回 422，不静默删除中间数据。

裁剪后 rows 可以少于 `data_length`；这是正常短响应，不补空行、不报错。

## 时间轴校验

裁剪后要求：

- `datetime` 存在且可转为 integer；
- `datetime > 0`；
- `datetime` 严格递增；
- Kline `open/high/low/close` 不能全空；
- Tick 关键价格不能全空。

Pandas 矢量化校验：

```python
dt = pd.to_numeric(frame["datetime"], errors="coerce")

if dt.isna().any() or (dt <= 0).any():
    raise TqDataFrameError("TQ_INVALID_TIME_AXIS")

if (dt.diff().dropna() <= 0).any():
    raise TqDataFrameError("TQ_INVALID_TIME_AXIS")
```

## 数值与 JSON 转换

Pandas/NumPy 中的下列值不是标准 JSON number：

```text
NaN, +Infinity, -Infinity
```

输出 records 前统一转 `None`。应使用 DataFrame/vectorized replacement，不在热路径对每个 cell 写 Python loop。

输出类型应是 JSON-safe Python scalars，不得泄漏 `np.int64/np.float64/Timestamp` 等不稳定 serialization object。Date 转 `YYYY-MM-DD` string，nanosecond `datetime` 保持 integer。

## 多合约 K 线

多 symbol serial 保留 TQ 的宽列，例如 `open1/close1/symbol1`。Placeholder 判断必须使用对应 primary price columns，不随意删动态列。

当前接口不将多 symbol Kline 拆为 CCXT rows，也不将其写入 DuckDB cache。

## 主连历史的宽表转长表

TQ 原始历史：

```text
date | KQ.m@DCE.a | KQ.m@DCE.eg | ...
```

响应转为：

```text
date / symbol / underlying_symbol
```

Pandas 可用 `melt()` 完成。空 mapping 在序列化前清理，不将 symbol 放到 dynamic JSON key。

## 错误契约

| HTTP | detail | 场景 |
| --- | --- | --- |
| 401 | 现有鉴权层 | Bearer token 缺失/无效 |
| 400 | `TQ_INVALID_SYMBOL` | symbol 空/显著非法 |
| 400 | `TQ_INVALID_DURATION_SECONDS` | duration 非正 |
| 400 | `TQ_INVALID_DATA_LENGTH` | 不在 `1..10000` |
| 400 | `TQ_INVALID_ADJ_TYPE` | adj type 不支持 |
| 400 | `TQ_INVALID_DATE_RANGE` | calendar 起点晚于终点 |
| 422 | `TQ_INVALID_TIME_AXIS` | time 非正/重复/倒序/中间 placeholder |
| 422 | `TQ_INVALID_TRADING_CALENDAR` | calendar date/trading schema 非法 |
| 422 | `TQ_CALENDAR_RANGE_UNAVAILABLE` | 超出 TqSdk 日历覆盖年份 |
| 422 | `TQ_NOT_CONT_SYMBOL` | underlying route 收到非 CONT |
| 422 | `TQ_UNDERLYING_SYMBOL_EMPTY` | CONT 缺 underlying |
| 503 | `SERVICE_NOT_ENABLED` / `SERVICE_NOT_READY` | HTTP 服务白名单或启动门禁拒绝 |
| 500 | `TQ_NOT_CONFIGURED` | 内部客户端缺少配置；正常 HTTP 入口先检查服务白名单 |
| 502 | `TQ_NETWORK_UNAVAILABLE` | TQ 网络/登录失败 |
| 502 | `TQ_CALENDAR_INCOMPLETE` | calendar 未逐日完整覆盖请求闭区间 |

批量 underlying 查询中任一 symbol 无效时整个请求返回 422，detail 带失败 symbol。

## 离线验证

默认测试不创建真实 `TqApi`，使用 fake API/Pandas DataFrame。覆盖：

- `data_length`/duration/adj type/symbol validation；
- 重复 `symbol` query 聚合；
- Kline/Tick 前置 placeholder trim；
- 可用 rows 少于 request length；
- 中间 placeholder、非正/重复/倒序 time 报错；
- NaN/Infinity 到 `null`；
- multi-symbol fields 保留；
- underlying current mapping 和 history melt；
- calendar ISO date range、date/bool conversion、顺序/唯一性与完整覆盖；
- `n=None` 不调 history API；
- 专用 SDK 线程持续推进消息，业务任务间也推进一次；状态 HTTP 读取不进入 SDK 队列；
- FileLock/singleton lifecycle；
- TQ path 不 import/use DuckDB cache 或 Polars。

## 在线与调试入口

Online tests 默认 skip，只在显式提供 TQ credentials/network 时运行。最小闭环：

- Kline serial 返回 list；
- Tick serial 返回 list；
- CONT underlying 返回 mapping；
- 中国期货交易日历返回完整的逐日 records；
- 结束时正确关闭 TqApi。

建议入口继续使用 `just test-tq-online` 和 `debug/tq_probe.py`；默认 `just test` 只收集 offline tests。

## 持续保持的实现约束

- 行情序列、主连、日历和状态遵循 [TQ 路由契约](tq_data.md)，不套用 CCXT 查询语义。
- `src/tools/tq_data_source.py` 无 `import polars`。
- `pandas` 是 direct dependency。
- TQ 仍使用现有 `FileLock`，不改成 CCXT/DuckDB lock。
- 不调 `get_ohlcv_with_cache`/`DuckDbOhlcvCache`。
- SDK 的消息处理、状态断线失效和实例关闭须有离线回归覆盖。
