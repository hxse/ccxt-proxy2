from typing import Any

from fastapi import APIRouter, Depends

from src.responses_tq import TqRecord, TqUnderlyingSymbolResponse
from src.router.auth_handler import manager
from src.tools.tq_manager import tq_manager
from src.types_tq import (
    TqOhlcvRequest,
    TqTickRequest,
    TqUnderlyingSymbolRequest,
    tq_ohlcv_request,
    tq_tick_request,
    tq_underlying_symbol_request,
)


tq_router = APIRouter(
    prefix="/tq",
    dependencies=[Depends(manager)],
    tags=["TQ DATA"],
)


TQ_COMMON_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {
        "description": (
            "请求参数不符合 TQ 实时序列接口要求。常见 detail: "
            "TQ_INVALID_SYMBOL、TQ_INVALID_DURATION_SECONDS、"
            "TQ_INVALID_DATA_LENGTH、TQ_INVALID_ADJ_TYPE。"
        )
    },
    401: {"description": "未通过本项目 Bearer token 鉴权。"},
    422: {
        "description": (
            "TQ 返回的数据不可安全序列化或业务语义不满足要求。"
            "例如时间轴非严格递增、前置占位行裁剪后仍有非法时间。"
        )
    },
    500: {"description": "服务端未配置 TQ，detail 为 TQ_NOT_CONFIGURED。"},
    502: {"description": "TQ 网络、登录或上游服务不可用。"},
}

TQ_OHLCV_DESCRIPTION = """
薄转发 TQ `get_kline_serial(symbol, duration_seconds, data_length, adj_type)`。

能力边界：

- 不支持 `since`、`limit`、`enable_cache`。
- 不接入本项目 `cache_tool`，不做历史窗口估算，也不保证返回覆盖某个起始时间。
- `data_length` 是传给 TQ 的实时序列窗口宽度上限，不是最小返回数量；有效历史不足时允许返回少于 `data_length` 的记录。
- TQ symbol 必须由调用方完整传入。普通合约、主连、指数/加权合约都通过 symbol 表达，不提供额外 `data_type` 参数。

TQ 进程内缓存提示：

- 同一个 `TqApi` 实例会按 `symbol + duration_seconds + data_length + adj_type` 复用 serial。
- 为复用 TQ 自身缓存，请避免对同一个 `symbol + duration_seconds + adj_type` 频繁变化 `data_length`。
- 不同 symbol、不同周期可以使用不同 `data_length`。

响应说明：

- 返回 TQ K 线 serial records，字段名保留 TQ 原始命名，例如 `datetime/open/high/low/close/volume/open_oi/close_oi`。
- 多 symbol K 线会保留 TQ 原始多合约字段，例如 `open1/close1/symbol1`。
- `datetime` 是 TQ 返回的纳秒时间戳。
- `NaN`、`inf`、`-inf` 会序列化为 JSON `null`。
- 只裁剪连续前置占位行；中间或尾部异常行不会被静默删除，会返回 422。
"""

TQ_TICK_DESCRIPTION = """
薄转发 TQ `get_tick_serial(symbol, data_length, adj_type)`。

能力边界：

- 不支持 `since`、`limit`、`enable_cache`。
- 不接入本项目 Tick cache；响应是 TQ Tick 实时序列，不是 CCXT ticker 快照。
- `data_length` 是传给 TQ 的实时序列窗口宽度上限，不是最小返回数量；有效历史不足时允许返回少于 `data_length` 的记录。

TQ 进程内缓存提示：

- 同一个 `TqApi` 实例会按 `symbol + data_length + adj_type` 复用 serial。
- 为复用 TQ 自身缓存，请避免对同一个 `symbol + adj_type` 频繁变化 `data_length`。
- 不同 symbol 可以使用不同 `data_length`。

响应说明：

- 返回 TQ Tick serial records，字段名保留 TQ 原始命名，例如 `datetime/last_price/bid_price1/ask_price1/volume/open_interest`。
- `datetime` 是 TQ 返回的纳秒时间戳。
- `NaN`、`inf`、`-inf` 会序列化为 JSON `null`。
- Tick 前置占位行只按关键价格字段判断，不使用 `volume`、`amount`、`open_interest` 等数量字段。
- 只裁剪连续前置占位行；中间或尾部异常行不会被静默删除，会返回 422。
"""

TQ_UNDERLYING_DESCRIPTION = """
查询 TQ 主连合约当前实际标的，并可选返回最近 N 个交易日的历史映射。

用途：

- 持仓换月前确认主连当前对应的实际合约。
- 下单前把 `KQ.m@...` 主连 symbol 解析到具体合约。
- 审计最近若干交易日主连标的切换历史。

实现说明：

- 当前标的优先来自 `api.query_symbol_info(symbol)` 的 `underlying_symbol`。
- 若当前元数据缺少 `underlying_symbol`，单个 symbol 会 fallback 到 `api.get_quote(symbol).underlying_symbol`。
- `n` 传入时额外调用 `api.query_his_cont_quotes(symbol, n=n)`，并将 TQ 返回的 pandas 宽表转换为稳定 JSON 长表。

响应说明：

- `items` 是当前主力解析结果。
- `history` 仅在传入 `n` 时返回最近 N 个交易日的主连映射。
- 当前 symbol 必须是 TQ `CONT` 主连合约；非主连或缺少 `underlying_symbol` 返回 422。
"""


@tq_router.get(
    "/fetch_ohlcv",
    response_model=list[TqRecord],
    summary="Fetch TQ realtime OHLCV serial",
    description=TQ_OHLCV_DESCRIPTION,
    responses=TQ_COMMON_RESPONSES,
)
def fetch_ohlcv(params: TqOhlcvRequest = Depends(tq_ohlcv_request)):
    """
    薄转发 TQ get_kline_serial 实时 K 线序列。
    """
    return tq_manager.fetch_ohlcv(params)


@tq_router.get(
    "/fetch_tick",
    response_model=list[TqRecord],
    summary="Fetch TQ realtime tick serial",
    description=TQ_TICK_DESCRIPTION,
    responses=TQ_COMMON_RESPONSES,
)
def fetch_tick(params: TqTickRequest = Depends(tq_tick_request)):
    """
    薄转发 TQ get_tick_serial 实时 Tick 序列。
    """
    return tq_manager.fetch_tick(params)


@tq_router.get(
    "/fetch_underlying_symbol",
    response_model=TqUnderlyingSymbolResponse,
    summary="Fetch TQ current underlying symbol for main contracts",
    description=TQ_UNDERLYING_DESCRIPTION,
    responses=TQ_COMMON_RESPONSES,
)
def fetch_underlying_symbol(
    params: TqUnderlyingSymbolRequest = Depends(tq_underlying_symbol_request),
):
    """
    根据 TQ 主连 symbol 查询当前实际主力合约和可选历史映射。
    """
    return tq_manager.fetch_underlying_symbol(params)
