from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from src.responses_system import ServiceUnavailableResponse
from src.responses_tq import (
    TqRecord,
    TqTradingCalendarItem,
    TqTradingStatusResponse,
    TqUnderlyingSymbolResponse,
)
from src.router.auth_handler import manager
from src.tools.tq_manager import tq_manager
from src.types_tq import (
    TqOhlcvRequest,
    TqTickRequest,
    TqTradingCalendarRequest,
    TqTradingStatusRequest,
    TqUnderlyingSymbolRequest,
    tq_ohlcv_request,
    tq_tick_request,
    tq_trading_calendar_request,
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
            "TQ_INVALID_DATA_LENGTH、TQ_INVALID_ADJ_TYPE、"
            "TQ_INVALID_DATE_RANGE。"
        )
    },
    401: {"description": "未通过本项目 Bearer token 鉴权。"},
    422: {
        "description": (
            "TQ 返回的数据不可安全序列化或业务语义不满足要求。"
            "例如时间轴非严格递增、前置占位行裁剪后仍有非法时间，或请求包含"
            "未知 query 参数。交易日历超出 TQ 可用年份时 detail 为 "
            "TQ_CALENDAR_RANGE_UNAVAILABLE。"
        )
    },
    500: {
        "description": "TQ_NOT_CONFIGURED 或无成功网络结果时的 TQ_CACHE_READ_FAILED。"
    },
    504: {"description": "行情队列与序列就绪超过 10 秒预算：TQ_DATA_TIMEOUT。"},
    507: {"description": "CACHE_CAPACITY_EXCEEDED：缓存容量保护失败并回滚。"},
    503: {
        "model": ServiceUnavailableResponse,
        "description": "SERVICE_NOT_ENABLED：tq 未列入 service_whitelist；SERVICE_NOT_READY：初始化中/失败、工作线程已退出或网络不可用，service=tq。请求不会触发初始化。",
    },
    502: {
        "description": (
            "TQ 上游数据或元数据处理失败；日历未完整覆盖请求的每日区间时 "
            "detail 为 TQ_CALENDAR_INCOMPLETE。"
        )
    },
}

TQ_OHLCV_DESCRIPTION = """
单合约 TQ 最新 K 线；没有 live/sandbox 参数，属于 live 行情。

- data_length 为最多响应数量，默认 10000，上限 100000。仅请求一次 SDK min(N,10000) 窗口。
- enable_cache 默认 true；通过项目原有时间戳重叠机制保存并补充连续历史，可能返回超过一万根。
- 成功不检查交易状态；网络失败只有真实合约 NOTRADING 状态才允许返回最新连续缓存。
- 不支持 since、limit、多 symbol 或重复 symbol；不以 interval 推算连续性，不试探窗口。
- 普通网络尾根照常返回，但不新增持久化。读取缓存中的末根不重复删尾。
- 同一 TqApi 按 symbol + duration_seconds + data_length + adj_type 复用 serial；不同长度可能创建新订阅。
- datetime 保留整数纳秒，duration 为秒；NaN/Infinity 转 null，前置占位可裁剪，中间坏行报错。
- 大于一周只作 SDK 单窗口薄转发，无项目缓存或休市兜底。
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
单主连当前有效标的与可选换月节点，统一以官方历史事件源为准。

- 无论是否传历史范围，每次固定在线时间、重新下载历史源，读取主连最新单根 5m 时间确定已生效交易日 D。
- 只传 symbol 返回 items；合约取源在 D 的节点，省略未设置的 history。
- 历史用成对 start_time/end_time，正整数 Unix 纳秒；夜盘按中国期货交易日解释。
- 历史最多到 D；未来预公告不进入 items、history 或历史缓存。
- enable_cache 默认 true；源摘要、完整日记录与节点一致时复用，否则交既有缓存接口更新。源失败不能用缓存冒充成功。
- history 只返回真实换月节点，含 date/symbol/underlying_symbol/old_symbol；解释左边界的节点可早于请求起点。
- 缺少前驱时 old_symbol=null；无法证明节点时明确报错。旧 n、多 symbol、refresh_source、now 不支持。
- SDK 当前标的可能滞后于历史源和行情；query_symbol_info 与两个旧日历/历史入口硬禁，不以 Quote 当前标的核对或回退。
- 可选 transition_timeframe 和默认 transition_bars=10，在真实节点附加旧合约 old_ 价格；同次取得 N＋1 才缓存前 N，无周期省略 transition。
"""

TQ_TRADING_CALENDAR_DESCRIPTION = """
从官方节假日源转换中国期货日历，并按真实日期重叠复用项目缓存。

- start_date/end_date 为 Asia/Shanghai 自然日，YYYY-MM-DD，闭区间；不是 UTC 时间戳，不做时区换算。
- enable_cache 默认 true。每次先在线取时；C≥源实际最后节假日 H、无 H 或缺覆盖时重新下载一次，H 不硬编码为年末。
- 返回每个自然日 date/trading，包括非交易日；完整性用标准日期库校验，缺行不填 false。
- 有效年份来自官方源首末日期所在年份；超范围明确报错。刷新后源末日未延长也不循环下载。
- 不用日历推断实际合约开市或 K 线完成；不调用 SDK 旧日历入口。
"""


@tq_router.get(
    "/fetch_ohlcv",
    response_model=list[TqRecord],
    summary="获取 TQ 实时 K 线序列",
    description=TQ_OHLCV_DESCRIPTION,
    response_description="清洗并按时间升序返回的 TQ K 线 records。",
    responses=TQ_COMMON_RESPONSES,
)
async def fetch_ohlcv(params: TqOhlcvRequest = Depends(tq_ohlcv_request)):
    """
    薄转发 TQ get_kline_serial 实时 K 线序列。
    """
    return await tq_manager.fetch_ohlcv(params)


@tq_router.get(
    "/fetch_tick",
    response_model=list[TqRecord],
    summary="获取 TQ 实时 Tick 序列",
    description=TQ_TICK_DESCRIPTION,
    response_description="清洗并按时间升序返回的 TQ Tick records。",
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
    response_model_exclude_unset=True,
    summary="查询 TQ 主连当前标的",
    description=TQ_UNDERLYING_DESCRIPTION,
    response_description="主连当前实际合约以及可选的历史映射。",
    responses=TQ_COMMON_RESPONSES,
)
async def fetch_underlying_symbol(
    params: TqUnderlyingSymbolRequest = Depends(tq_underlying_symbol_request),
):
    """
    根据 TQ 主连 symbol 查询当前实际主力合约和可选历史映射。
    """
    return await tq_manager.fetch_underlying_symbol(params)


@tq_router.get(
    "/fetch_trading_calendar",
    response_model=list[TqTradingCalendarItem],
    summary="查询 TQ 中国期货交易日历",
    description=TQ_TRADING_CALENDAR_DESCRIPTION,
    response_description="闭区间内逐自然日的 date/trading records。",
    responses=TQ_COMMON_RESPONSES,
)
async def fetch_trading_calendar(
    params: TqTradingCalendarRequest = Depends(tq_trading_calendar_request),
):
    """在线检查后查询官方日历与日期缓存。"""
    return await tq_manager.fetch_trading_calendar(params)


@tq_router.get(
    "/fetch_trading_status",
    response_model=TqTradingStatusResponse,
    summary="查询 TQ 合约当前交易状态",
    description="""
读取后台通过 `get_trading_status(symbol)` 订阅得到的最新状态快照。
输入单个完整合约代码，例如 `symbol=SHFE.rb2610`。

- `is_open=true`：CONTINOUS（上游原始拼写），处于连续交易。
- `is_open=false`：AUCTIONORDERING（集合竞价报单）或 NOTRADING（非交易）。
- `is_open=null`：尚未收到、状态连接断线、服务不可用或未知编码，具体见 reason。
- 断线后旧状态失效，重连后必须收到新状态；长期没有状态变化不等于休市。

需要 TQ 账户开通交易状态权限；没有权限返回 **403 TQ_TRADING_STATUS_PERMISSION_DENIED**。
HTTP 不调用 SDK、不等待网络，也不进入行情请求队列；只读取独立短锁保护的快照。
首次查询新合约只登记一次后台订阅意向，先返回 null/not_received；SDK 线程处理订阅、收到数据后，后续查询返回新状态。
TqApi 在启动阶段初始化。后台订阅失败在后续查询中返回对应 HTTP 错误；无权限无需等待订阅，直接返回 403。
节假日前置停机或网络故障只能返回未知，不根据本机时间、旧 K 线或交易日历推算休市。
只查询状态，不下单，也不代表账户当前能够成功成交。
""",
    response_description="symbol、is_open（boolean/null）、raw_status（string/null）和 reason（未知原因/null）。",
    responses={
        **TQ_COMMON_RESPONSES,
        400: {"description": "TQ_INVALID_SYMBOL：上游拒绝合约参数。"},
        403: {
            "description": "TQ_TRADING_STATUS_PERMISSION_DENIED：TQ 账户未开通交易状态权限。"
        },
        422: {"description": "缺少 symbol、空白 symbol，或包含未声明的 query 参数。"},
        502: {
            "description": "后台订阅记录的 TQ SDK/上游错误；未收到数据和连接不可用以 200、is_open=null 返回。"
        },
    },
)
async def fetch_trading_status(
    params: Annotated[TqTradingStatusRequest, Query()],
) -> TqTradingStatusResponse:
    """只读最新快照；首次合约登记后台订阅，HTTP 不调用 SDK 或等待网络。"""
    return tq_manager.fetch_trading_status(params)
