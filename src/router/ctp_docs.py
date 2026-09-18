"""直接显示在 /docs 中的 CTP 参数、回报和错误语义。"""

from typing import Any

from src.responses_ctp import CtpErrorResponse, CtpValidationResponse

CTP_READ_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: {"description": "本项目 Bearer token 缺失或无效。"},
    422: {
        "model": CtpValidationResponse,
        "description": "参数校验失败；未知/拼错的 query 或 body 字段也会被拒绝。",
    },
    429: {
        "model": CtpErrorResponse,
        "description": "CTP_SEND_FAILED：Req* 返回 -2/-3，表示在途请求数或发送频率超限，未重试。",
    },
    502: {
        "model": CtpErrorResponse,
        "description": "CTP_AUTH_FAILED / CTP_SETTLEMENT_FAILED / CTP_QUERY_FAILED / CTP_INVALID_RESPONSE：认证、结算确认、查询或回报失败。",
    },
    503: {
        "model": CtpErrorResponse,
        "description": "SERVICE_NOT_ENABLED / SERVICE_NOT_READY：所选 ctp/模式未列入 service_whitelist 或未就绪。其他 CTP_* 错误表示连接不可用；请求不启用白名单外实例。",
    },
    504: {
        "model": CtpErrorResponse,
        "description": "CTP_TIMEOUT：未收到查询 bIsLast；不返回部分数组。",
    },
}

CTP_WRITE_RESPONSES = {
    **CTP_READ_RESPONSES,
    409: {
        "model": CtpErrorResponse,
        "description": "CTP_CANCEL_REJECTED：撤单被拒绝，或目标订单已全部成交。",
    },
    422: {
        "model": CtpValidationResponse,
        "description": "参数校验错误列表，或 detail.code=CTP_ORDER_REJECTED 的上游拒单详情。",
    },
    502: {
        "model": CtpErrorResponse,
        "description": "OPERATION_STATUS_UNKNOWN：写操作发送后超时、断线或回报异常，可能已生效；先用 order_identity 查询订单/成交对账。也可能是认证/结算确认失败。",
    },
}

CTP_STATUS_RESPONSES = {
    code: response
    for code, response in CTP_READ_RESPONSES.items()
    if code not in {429, 504}
}
CTP_STATUS_RESPONSES[503] = {
    "model": CtpErrorResponse,
    "description": "SERVICE_NOT_ENABLED / SERVICE_NOT_READY：所选 ctp/模式未启用或未完成应用启动；运行中缺少可用连接快照时以 200、is_open=null 返回。",
}
CTP_STATUS_RESPONSES[502] = {
    "model": CtpErrorResponse,
    "description": "CTP_INVALID_RESPONSE：收到的原生状态通知结构异常。",
}

CTP_STATUS_DESCRIPTION = """
读取当前连接最新的 `OnRtnInstrumentStatus` 公共流通知；CTP 没有对应的主动查询 API。
按品种精确匹配，例如 `exchange_id=SHFE&product_id=rb`，不传 rb2610，也不自动解析合约。

- `mode=sandbox`（默认）读取 `[ctp.test]`，可接 SimNow；`mode=live` 读取 `[ctp.live]`。
  模拟盘只代表对应模拟前置的状态，不能当成实盘交易所状态；缺少所选配置时明确报错。
- `is_open=true`：InstrumentStatus="2"，连续交易。
- `is_open=false`："0"/"1"/"3"/"4"/"5"/"6"/"7"，开盘前、非交易、集合竞价、收盘等阶段。
- `is_open=null`：尚未收到该品种状态、断线、连接实例不可用或未知编码，具体见 reason。

`raw_status` 保留字符串编码；`data` 返回完整有效原生通知字段，包括 EnterTime、EnterReason。
EnterTime 是状态进入时间，不是查询时间；状态仅在变化时推送，不因通知较旧而判定失效。
断线后清除旧通知，重连后等待新通知；缺少通知不能推断品种不存在或市场休市。
列入 service_whitelist 的模式在启动阶段完成认证、登录、结算确认。
HTTP 只读取独立短锁保护的快照，不进入账户操作队列或业务回调锁，也不等待网络。
状态请求不创建连接、不触发登录/重登、不发送 ReqQry*、不额外等待推送，不产生订单或撤单。
连接恢复沿用既有机制；尚未重新登录或收到新通知时，本接口持续返回未知。
断线时直接返回 null/disconnected，重连后没有新通知时为 null/not_received；没有可用连接实例时为 null/unavailable。
状态通知没有 nRequestID 和交易日，本响应不包含 request_id/trading_day。
"""

SESSION_DESCRIPTION = """

账户与模式：`mode=sandbox`（默认）读取 config.toml 的 `[ctp.test]`，可接 SimNow；
`mode=live` 读取 `[ctp.live]`，两者连接及 flow 目录独立，不相互回退。
请求不能传密码、认证码或前置地址；使用本项目 Bearer token。
只有列入 service_whitelist 的模式会在程序启动时完成客户端认证、登录、结算确认。
请求复用已初始化实例；断线恢复仍使用启动时的配置快照，中途修改文件不生效。

返回：保留 CTP 原生字段名、字符串状态编码及订单编号前导空格；字段类型/含义见 response schema。
金额/价格的无效 double（DBL_MAX、NaN、Infinity）序列化为 null，省略 reserve* 无效字段。
`request_id` 是 CTP nRequestID，`trading_day` 是登录回报的 YYYYMMDD 交易日。
下单/撤单的 `OrderMemo` 由服务生成，用于回报关联，不是 HTTP 输入或幂等键。
缺少足够关联信息的私有流错误不直接判定本次操作失败；等待超时返回 OPERATION_STATUS_UNKNOWN，需查询对账。
"""

ORDER_DESCRIPTION = (
    """

| 操作 | side | offset |
| --- | --- | --- |
| 开多 | buy | open |
| 开空 | sell | open |
| 平多 | sell | close / close_today / close_yesterday |
| 平空 | buy | close / close_today / close_yesterday |

`volume` 是正整数手数；调用方按交易所规则选择平仓、平今、平昨，不自动拆单或平全部。
`instrument_id` 只接受具体期货合约，如 rb2610。TQ 的 SHFE.rb2610 应拆为 exchange_id=SHFE、instrument_id=rb2610；
主连解析与行情/价格继续使用 `/tq`，不接受 KQ.m@... 主连或指数代码。

有状态操作：`ReqOrderInsert` 返回 0 只表示本地发送成功。HTTP 200 还需等待匹配的 `OnRtnOrder`
接受/订单状态回报，返回详细订单快照；不等待全部成交。成交明细通过 `/ctp/fetch_trades` 查询。
OnRspOrderInsert、OnErrRtnOrderInsert 或拒单状态回报会转换为 CTP_ORDER_REJECTED，并保留可得的 ErrorID/错误说明。

不自动重试。提交后超时/断线返回 OPERATION_STATUS_UNKNOWN 和已分配的 order_identity，
先查询订单/成交对账；不能把 HTTP 错误当作未下单。OrderRef 由服务按登录 MaxOrderRef 递增，不是 HTTP 幂等键。
"""
    + SESSION_DESCRIPTION
)

CTP_MARKET_DESCRIPTION = (
    """
薄转发 `ReqOrderInsert`：固定 OrderPriceType=1（AnyPrice）、LimitPrice=0、
TimeCondition=1（IOC）、VolumeCondition=1（AV）。请求体**不接受 price**。

原生市价是否支持由交易所、品种和前置决定；上游拒绝时返回明确错误。
服务不取行情合成市价、不用涨跌停价替代、不自动降级。
如需指定价格保护的立即成交委托，可由调用方使用限价接口并选择 time_in_force=IOC。
"""
    + ORDER_DESCRIPTION
)

CTP_LIMIT_DESCRIPTION = (
    """
薄转发 `ReqOrderInsert`：固定 OrderPriceType=2（LimitPrice），**price 必填**，按合约报价单位填写。
time_in_force=GFD（默认）使用 GFD+AV；IOC 使用 IOC+AV；FOK 使用 IOC+CV。
上游校验最小变动价位、涨跌停板、可用资金和权限。返回的 LimitPrice 是委托价格，非成交价格。
"""
    + ORDER_DESCRIPTION
)

CTP_CANCEL_DESCRIPTION = (
    """
薄转发 `ReqOrderAction`，固定 ActionFlag=0（Delete），撤销指定订单尚未成交的部分。

请求体 `by` 选择一种定位方式（OpenAPI oneOf）：

- exchange_order：exchange_id + instrument_id + order_sys_id，OrderSysID 原样复制，包括前导空格。
- session_order：exchange_id + instrument_id + front_id + session_id + order_ref。
  必须使用**原订单**的 FrontID/SessionID，不得用重新登录后的会话替代。

混用两种方式的参数会被拒绝。不提供改单、批量撤单或自动查订单补充标识。
HTTP 200 等待匹配订单的 OnRtnOrder 确认 OrderStatus=5；返回详细 order，已成交部分不会被撤回。
显式拒绝返回 CTP_CANCEL_REJECTED；提交后超时/断线返回 OPERATION_STATUS_UNKNOWN 和订单定位信息。
不自动重试，先查询订单与成交对账。
"""
    + SESSION_DESCRIPTION
)

QUERY_DESCRIPTION = (
    """

按 nRequestID 聚合回调，直到 bIsLast=true；无数据返回空数组。超时丢弃不完整结果并报错。
同一账户的请求串行，查询按配置间隔发送；不缓存、不自动重试、不分页、不补历史。
这是 CTP 当前可查询交易日的数据，不提供任意历史日期检索。
TradingDay 是 YYYYMMDD，时间是 HH:MM:SS；不作时区或夜盘交易日推算。
"""
    + SESSION_DESCRIPTION
)
