# TQ 行情转发设计蓝图

## 目标

本设计为 `ccxt-proxy2` 增加一个独立的 TQ 行情转发层，用于读取国内期货相关的 K 线、Tick 与主连标的映射。

第一版采用薄转发策略：

- 不支持 `since`。
- 不接入本项目 `cache_tool`。
- 不做窗口估算、锚点裁剪、proof log 或额外落盘缓存。
- 请求参数尽量贴近 TQ 原始接口。
- 缓存行为交给 TqSdk 自身的实时序列复用机制。
- 仅做 HTTP 参数校验、pandas DataFrame 到 JSON 的序列化、前置空行裁剪和时间轴基本校验。

提供三个路由：

- `/tq/fetch_ohlcv`
- `/tq/fetch_tick`
- `/tq/fetch_underlying_symbol`

## 能力边界

免费账号第一版只使用 TQ 实时序列接口：

```python
api.get_kline_serial(symbol, duration_seconds, data_length, adj_type=None)
api.get_tick_serial(symbol, data_length, adj_type=None)
api.query_symbol_info(symbol)
api.query_his_cont_quotes(symbol, n=200)
```

TqSdk 3.10.0 中 `data_length` 最大支持 10000。服务端路由默认使用 `data_length=10000`，调用方也可以为不同品种、不同周期选择不同的固定数量。

缓存提示需要出现在 FastAPI 路由 docstring 或 `description` 中，确保自动生成的路由文档可见。

TqSdk 的实时序列复用 key 包含请求数量：

```python
get_kline_serial: (tuple(symbol), duration_seconds, data_length, adj_type)
get_tick_serial:  (symbol, data_length, adj_type)
```

同一个 `TqApi` 实例中，相同 key 会复用已有 serial；不同 `data_length` 会创建新的 serial/chart。为了尽量复用 TQ 进程内缓存，客户端应避免对同一个 `symbol + duration_seconds + adj_type` 频繁变化 `data_length`。不同 symbol、不同周期使用不同 `data_length` 是允许的。

TQ 不会在未请求某个 symbol/周期时后台缓存该数据。已请求的实时 serial 是固定宽度滚动窗口，不会因为服务运行时间变长而自动返回超过 `data_length` 的行数。

路由参数使用 TQ 原始参数名。TQ 参数类型为 `Union[str, list[str]]` 的 `symbol`，HTTP 层使用同名 `symbol` 表达：

```text
单 symbol: /tq/fetch_ohlcv?symbol=SHFE.rb2505&duration_seconds=60
多 symbol: /tq/fetch_ohlcv?symbol=SHFE.au2508&symbol=CFFEX.IF2506&duration_seconds=60
```

服务端只把同名 query 参数聚合为 Python `list[str]` 后传给 TQ，不提供额外的 `symbols` 参数。

专业版历史接口不作为第一版能力：

```python
api.get_kline_data_series(...)
api.get_tick_data_series(...)
```

这些接口有 TQ 专业版权限要求，虽然带有 `~/.tqsdk/data_series_1` 磁盘缓存，但免费账号默认不可用。

## Symbol 约定

TQ symbol 是唯一行情标识，不提供额外 `data_type` 参数。

示例：

- 普通具体合约：`SHFE.rb2505`
- 主连合约：`KQ.m@SHFE.rb`
- 指数/加权合约：`KQ.i@SHFE.rb`

服务端不根据 `data_type` 拼接 symbol。调用方必须传入完整 TQ symbol。

## 路由设计

### `GET /tq/fetch_ohlcv`

用途：薄转发 TQ `get_kline_serial`。

请求参数：

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `symbol` | `str \| list[str]` | 是 | 透传 TQ `symbol`。单合约传一个 `symbol`；多合约 K 线用重复 query 参数传多个同名 `symbol` |
| `duration_seconds` | `int` | 是 | TQ K 线周期，单位秒 |
| `data_length` | `int` | 否 | 默认 10000，范围 `1..10000` |
| `adj_type` | `str \| None` | 否 | 透传 TQ 复权参数：`F`、`B`、`FORWARD`、`BACK` 或空 |

FastAPI 路由 docstring 或 `description` 必须包含：

```text
本路由是 TQ get_kline_serial 实时 K 线序列薄转发，不支持 since，不接入本项目 OHLCV cache。
TqSdk 会在同一个 TqApi 实例内按 symbol/duration_seconds/data_length/adj_type 复用 serial。
为尽量复用 TQ 进程内缓存，请避免对同一 symbol + duration_seconds + adj_type 频繁变化 data_length；不同 symbol 可以使用不同 data_length。
```

响应格式为 TQ K 线 records，保留 TQ 字段名。路由使用 FastAPI 默认 JSON 序列化，`NaN` 转为 `null`：

```json
[
  {
    "id": 1234,
    "datetime": 1718000000000000000,
    "open": 3600.0,
    "high": 3610.0,
    "low": 3590.0,
    "close": 3605.0,
    "volume": 12345.0,
    "open_oi": 100000.0,
    "close_oi": 100200.0,
    "symbol": "KQ.m@SHFE.rb",
    "duration": 60
  }
]
```

说明：

- 返回不是 `/ccxt/fetch_ohlcv` 的 `list[list[float]]` 范式。
- 本路由暴露的是 TQ K 线 serial 数据，而不是 CCXT OHLCV 兼容层。
- 多合约 K 线返回 TQ 原始多合约字段，例如 `open1`、`close1`、`symbol1` 等。

### `GET /tq/fetch_tick`

用途：薄转发 TQ `get_tick_serial`。

请求参数：

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `symbol` | `str` | 是 | TQ 合约代码 |
| `data_length` | `int` | 否 | 默认 10000，范围 `1..10000` |
| `adj_type` | `str \| None` | 否 | 透传 TQ 复权参数：`F`、`B`、`FORWARD`、`BACK` 或空 |

FastAPI 路由 docstring 或 `description` 必须包含：

```text
本路由是 TQ get_tick_serial 实时 Tick 序列薄转发，不支持 since，不接入本项目 Tick cache。
TqSdk 会在同一个 TqApi 实例内按 symbol/data_length/adj_type 复用 serial。
为尽量复用 TQ 进程内缓存，请避免对同一 symbol + adj_type 频繁变化 data_length；不同 symbol 可以使用不同 data_length。
```

响应格式为 TQ Tick records，保留 TQ 字段名。路由使用 FastAPI 默认 JSON 序列化，`NaN` 转为 `null`：

```json
[
  {
    "id": 12345,
    "datetime": 1718000000000000000,
    "last_price": 3605.0,
    "average": 3600.0,
    "highest": 3610.0,
    "lowest": 3590.0,
    "bid_price1": 3604.0,
    "bid_volume1": 12.0,
    "ask_price1": 3605.0,
    "ask_volume1": 8.0,
    "volume": 12345.0,
    "amount": 4567890.0,
    "open_interest": 100000.0,
    "symbol": "SHFE.rb2505",
    "duration": 0
  }
]
```

本路由没有可直接对齐的 CCXT 路由。`/ccxt/fetch_tickers` 是当前行情快照，TQ `get_tick_serial` 是 Tick 时间序列，两者语义不同。

### `GET /tq/fetch_underlying_symbol`

用途：薄转发 TQ 主连相关查询，用于根据主连 symbol 查询当前实际主力合约，并可按 TQ `n` 参数返回最近 N 个交易日的历史映射。该路由服务于持仓换月、下单前 symbol 解析、仓位审计等场景。

请求参数：

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `symbol` | `str \| list[str]` | 是 | 透传 TQ `symbol`。单个主连传一个 `symbol`；多个主连用重复 query 参数传多个同名 `symbol` |
| `n` | `int \| None` | 否 | 透传 TQ `query_his_cont_quotes(symbol, n)` 的 `n`。不传时只返回当前主力解析；传入时额外返回最近 N 个交易日的主连标的映射 |

当前主力解析主路径：

```python
info = api.query_symbol_info(symbol)
underlying_symbol = info["underlying_symbol"]
```

`query_symbol_info` 返回的 `underlying_symbol` 字段对 `CONT` 主连合约有效。若元数据结果缺少 `underlying_symbol`，单 symbol 请求可 fallback 到：

```python
quote = api.get_quote(symbol)
underlying_symbol = quote.underlying_symbol
```

历史映射：

```python
history = api.query_his_cont_quotes(symbol, n=n)
```

TQ 原始返回是 pandas DataFrame，不是 JSON。列结构固定为：

```text
date | {symbol_1} | {symbol_2} | ...
```

示例原始表：

```text
        date KQ.m@DCE.a KQ.m@DCE.eg
0 2021-08-13  DCE.a2109  DCE.eg2109
1 2021-08-16  DCE.a2111  DCE.eg2109
2 2021-08-17  DCE.a2111  DCE.eg2109
```

HTTP 路由将该宽表转换成长表 JSON，避免把 symbol 放进动态 JSON key。

响应使用对象 envelope：

```json
{
  "items": [
    {
      "symbol": "KQ.m@DCE.i",
      "underlying_symbol": "DCE.i2509",
      "ins_class": "CONT",
      "exchange_id": "DCE",
      "product_id": "i"
    }
  ],
  "history": [
    {
      "date": "2026-06-10",
      "symbol": "KQ.m@DCE.i",
      "underlying_symbol": "DCE.i2509"
    }
  ]
}
```

契约：

- 对 `CONT` 主连合约，`underlying_symbol` 不能为空；为空时返回 422。
- 对非 `CONT` 合约，默认返回 422，避免把具体合约误认为可换月主连。
- 批量查询时每个 item 独立返回结果；任一 symbol 无效时整个请求返回 422，并在 detail 中包含失败 symbol。
- `n` 未传时不调用 `query_his_cont_quotes`。
- `n` 传入时仅支持主连 symbol；返回数据用于审计，不替代当前主力解析。

## 数据处理规范

TQ 返回 pandas DataFrame。项目代码不直接使用 pandas 做业务处理，只把 TQ 返回对象交给 Polars 做批量清洗与 JSON 序列化：

```python
frame = pl.from_pandas(tq_frame)
```

直接依赖只声明 `tqsdk`。`pandas` 是 `tqsdk` 的传递依赖；除非项目代码开始直接 `import pandas`，否则不单独声明 `pandas`。

### 前置占位行裁剪

TQ realtime serial 是固定宽度窗口。`data_length` 大于当前可用历史条数时，TqSdk 会用对象默认值补齐窗口前段，而不是返回可直接消费的真实行情。TqSdk 3.10.0 默认值规则为：

- Kline: `datetime = 0`，`open/high/low/close = NaN`，`volume/open_oi/close_oi = 0`。
- Tick: `datetime = 0`，价格字段为 `NaN`，数量/持仓字段多为 `0`。

因此不能用“整行全空”判断占位行，数量类字段为 `0` 是默认占位的一部分。

第一版裁掉连续前置占位行，避免把无效数据暴露给 API 调用方。只裁前缀，不在中间或尾部删除数据；裁剪后仍存在 `datetime <= 0`、关键价格字段全空或倒序时间轴时返回 422。

`data_length` 表示向 TQ 请求的实时序列窗口宽度上限，不表示响应必须返回的最小数量。若 TQ 可用历史不足，或裁掉前置占位行后剩余行数少于 `data_length`，路由应返回实际有效行数，不补空行、不报错。

Kline 前置占位行定义：

```text
datetime == 0
open/high/low/close 全部为 NaN 或 null
```

Tick 前置占位行定义：

```text
datetime == 0
last_price/average/highest/lowest/bid_price1/ask_price1 全部为 NaN 或 null
```

Tick 判断不使用 `volume`、`amount`、`open_interest`、买卖量等数量字段，因为它们在默认对象中可能是 `0`。

Polars 处理策略：

```python
def _is_empty_number(column: str) -> pl.Expr:
    value = pl.col(column).cast(pl.Float64, strict=False)
    return value.is_null() | value.is_nan()


kline_price_columns = ["open", "high", "low", "close"]
kline_placeholder = (
    (pl.col("datetime").cast(pl.Int64, strict=False) == 0)
    & pl.all_horizontal(*[_is_empty_number(column) for column in kline_price_columns])
)

tick_price_columns = [
    "last_price",
    "average",
    "highest",
    "lowest",
    "bid_price1",
    "ask_price1",
]
tick_placeholder = (
    (pl.col("datetime").cast(pl.Int64, strict=False) == 0)
    & pl.all_horizontal(*[_is_empty_number(column) for column in tick_price_columns])
)

with_flags = frame.with_columns(
    kline_placeholder.alias("__placeholder")
).with_row_index("__row")

first_data_row = with_flags.filter(~pl.col("__placeholder")).select(
    pl.col("__row").min()
).item()

trimmed = (
    frame.head(0)
    if first_data_row is None
    else with_flags.filter(pl.col("__row") >= first_data_row).drop(
        ["__row", "__placeholder"]
    )
)
```

裁剪后校验：

- `datetime` 非空。
- `datetime > 0`。
- `datetime` 严格递增。
- Kline 的 `open/high/low/close` 不能全部为空。
- Tick 的关键价格字段不能全部为空。

严格递增校验使用矢量化 diff：

```python
invalid = frame.select(
    (pl.col("datetime").diff().drop_nulls() <= 0).any()
).item()
```

### JSON 序列化

响应保留 TQ 字段名，不做 CCXT 兼容转换。

序列化前需要将非 JSON 值转换为 `null`：

- `NaN`
- `inf`
- `-inf`

优先用 Polars 表达式处理，不在热路径逐行循环。

## 并发与 TqApi 生命周期

第一版使用单例 `TqManager` 持有一个 `TqApi` 实例。

原因：

- TQ 实时序列缓存绑定在 `TqApi` 实例内。
- 每次请求创建新 `TqApi` 会破坏 TQ 自身 serial 复用。
- `TqApi` 是状态型客户端，不适合在 FastAPI 线程池中无锁并发访问。

所有访问 `TqApi` 的路由都走同一把进程内锁。若服务以多 worker 运行，每个 worker 会有自己的 `TqApi` 和自己的 TQ 进程内缓存。

不做本项目额外磁盘缓存，因此不需要 `cache_tool` 的 parquet lock。

## 配置与依赖

项目入口鉴权不在 TQ 配置中单独实现。TQ 路由必须复用现有 `/auth/token` 登录与 Bearer token 机制，和 CCXT 路由保持一致。

现有入口认证配置仍使用顶层 `SECRET` 与 `users`：

```json
{
  "SECRET": "...",
  "users": {
    "user1": {
      "password": "..."
    }
  }
}
```

TQ 配置只表示服务端连接 TqSdk 所需的账号信息，不作为 HTTP 客户端访问本服务的 token 或密码：

```json
{
  "tq": {
    "username": "...",
    "password": ""
  }
}
```

完整配置示例：

```json
{
  "SECRET": "...",
  "users": {
    "user1": {
      "password": "..."
    }
  },
  "tq": {
    "username": "...",
    "password": ""
  }
}
```

`password` 允许为空，用于免费账号或游客模式；如果 TQ SDK 实际运行要求账号密码，则启动或首次请求时返回明确错误。

配置模型新增 `TqConfig`，并挂到 `AppConfig.tq`。`tq` 建议为可选配置；服务启动不因为未配置 TQ 失败，但首次访问 `/tq/*` 时返回 `TQ_NOT_CONFIGURED`。

```python
class TqConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    username: str | None = None
    password: str = ""


class AppConfig(BaseModel):
    ...
    tq: TqConfig | None = None
```

TQ 路由鉴权范式必须与 CCXT 一致：

```python
from fastapi import APIRouter, Depends
from src.router.auth_handler import manager

tq_router = APIRouter(
    prefix="/tq",
    dependencies=[Depends(manager)],
    tags=["TQ DATA"],
)
```

不要新增 TQ 专用 HTTP token、query token、header token 或 Basic Auth。未登录访问 `/tq/*` 时应由现有 `fastapi-login` 依赖返回 401；TQ 账号错误、TQ 未配置或 TQ 网络失败属于服务端连接 TQ 的问题，不应和入口 token 混在一起。

Bruno/API 文档也保持同一套认证：TQ 请求文件使用 `auth: inherit`，由 collection 级 OAuth2 password flow 自动请求 `/auth/token` 并在请求头携带 `Authorization: Bearer ...`。

依赖：

```toml
"tqsdk"
```

## 文件结构

建议新增：

```text
src/types_tq.py
src/responses_tq.py
src/tools/tq_data_source.py
src/tools/tq_manager.py
src/router/tq_router.py
Test/test_tq_data_source.py
Test/online/test_tq_online.py
debug/tq_probe.py
```

修改：

```text
src/main.py
src/tools/config_types.py
pyproject.toml
justfile
```

## 测试策略

离线测试默认自动运行，不访问 TQ 网络。

覆盖范围：

- 路由参数校验：`data_length` 范围 `1..10000`。
- `duration_seconds` 透传。
- `adj_type` 透传。
- `symbol` 参数透传；重复同名 query 参数会聚合为 TQ 需要的 `list[str]`。
- pandas DataFrame 转 Polars。
- 前置占位行裁剪。
- 裁剪后返回数量允许小于请求的 `data_length`。
- 中间占位行报错。
- `datetime <= 0` 报错。
- 重复或倒序时间报错。
- `NaN/inf` JSON 序列化为 `null`。
- 主连解析路由能从 `query_symbol_info` 的 `underlying_symbol` 字段构造当前主力映射。
- 非 `CONT` 合约请求主连解析时返回 `TQ_NOT_CONT_SYMBOL`。
- `n` 传入时历史映射只在显式请求下返回。

在线测试手动运行，默认 skip。

`justfile` 增加：

```just
test-tq-offline:
    uv run --no-sync pytest -v -ra Test/test_tq_*.py

test-tq-online:
    TQ_ONLINE=1 uv run --no-sync pytest -v -ra -s Test/online/test_tq_online.py

debug-tq-ohlcv symbol duration_seconds="60" data_length="10000":
    uv run --no-sync python debug/tq_probe.py ohlcv --symbol "{{symbol}}" --duration-seconds "{{duration_seconds}}" --data-length "{{data_length}}"

debug-tq-tick symbol data_length="10000":
    uv run --no-sync python debug/tq_probe.py tick --symbol "{{symbol}}" --data-length "{{data_length}}"

debug-tq-underlying symbol n="":
    uv run --no-sync python debug/tq_probe.py underlying --symbol "{{symbol}}" --n "{{n}}"
```

`just test` 应包含离线 TQ 测试；在线测试只能通过 `just test-tq-online` 显式执行。

## 错误码

| HTTP 状态 | detail | 场景 |
| --- | --- | --- |
| 401 | 由现有认证层返回 | 未登录、Bearer token 缺失或无效；与 CCXT 路由一致 |
| 400 | `TQ_INVALID_SYMBOL` | symbol 为空或格式明显非法 |
| 400 | `TQ_INVALID_DURATION_SECONDS` | `duration_seconds <= 0` 或 TQ 不支持 |
| 400 | `TQ_INVALID_DATA_LENGTH` | `data_length` 不在 `1..10000` |
| 400 | `TQ_INVALID_ADJ_TYPE` | `adj_type` 不在 TQ 支持范围 |
| 422 | `TQ_INVALID_TIME_AXIS` | TQ 返回时间轴异常 |
| 422 | `TQ_NOT_CONT_SYMBOL` | 主连解析请求传入非 `CONT` 合约 |
| 422 | `TQ_UNDERLYING_SYMBOL_EMPTY` | 主连合约缺少 `underlying_symbol` |
| 500 | `TQ_NOT_CONFIGURED` | TQ 配置不可用 |
| 502 | `TQ_NETWORK_UNAVAILABLE` | TQ 网络或登录失败 |

## 实施顺序

1. 添加 TQ 配置类型与依赖。
2. 实现 `TqManager` 单例和锁。
3. 实现 TQ DataFrame 到 JSON records 的 Polars 清洗与序列化。
4. 实现 `/tq/fetch_ohlcv`。
5. 实现 `/tq/fetch_tick`。
6. 实现 `/tq/fetch_underlying_symbol`。
7. 增加离线测试、在线测试与 `just` 入口。
8. 接入 `src/main.py`。

## 验收标准

- `/tq/fetch_ohlcv` 只接受 TQ 实时序列参数，不出现 `since`、`limit`、`enable_cache`。
- `/tq/fetch_tick` 只接受 TQ 实时序列参数，不出现 `since`、`limit`、`enable_cache`。
- 默认 `data_length=10000`。
- 不调用本项目 `get_ohlcv_with_cache`。
- 不写入本项目 OHLCV parquet 缓存。
- 重复相同 TQ 请求可复用同一 `TqApi` 实例内的 serial。
- TQ 前置占位行被裁掉，中间异常行会报错。
- 返回数量允许小于请求的 `data_length`，但不能包含前置占位行。
- 所有自动化离线测试通过。
