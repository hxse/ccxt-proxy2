"""ctpwrapper 6.7.13 原生字段的 HTTP 类型；省略 reserve* 无效字段。

字段依据上游 ApiStructure.py，价格/金额的 CTP 无效 double 序列化为 null。
https://github.com/nooperpudd/ctpwrapper/blob/master/ctpwrapper/ApiStructure.py
"""

from pydantic import BaseModel, ConfigDict, Field


class CtpOrder(BaseModel):
    """CTP OrderField。"""

    model_config = ConfigDict(extra="forbid")

    BrokerID: str = Field(description="经纪公司代码")
    InvestorID: str = Field(description="投资者代码")
    OrderRef: str = Field(
        description="原订单报单引用；结合 FrontID/SessionID 定位，不是全局订单 ID。"
    )
    UserID: str = Field(description="用户代码")
    OrderPriceType: str = Field(
        description="价格条件：1=AnyPrice 市价、2=LimitPrice 限价；其他值保留 CTP 原始编码。"
    )
    Direction: str = Field(description="买卖方向：0=买、1=卖；不等同于持仓多空方向。")
    CombOffsetFlag: str = Field(
        description="组合开平标志：0=开仓、1=平仓、2=强平、3=平今、4=平昨、5=强减、6=本地强平。"
    )
    CombHedgeFlag: str = Field(
        description="组合投机套保标志：1=投机、2=套利、3=套保；保留原始编码。"
    )
    LimitPrice: float | None = Field(
        description="委托价格，使用合约报价单位；市价单为 0。无效数值为 null。"
    )
    VolumeTotalOriginal: int = Field(description="原始委托手数。")
    TimeCondition: str = Field(
        description="有效期类型：1=IOC、2=GFS、3=GFD、4=GTD、5=GTC、6=GFA。"
    )
    GTDDate: str = Field(description="GTD日期")
    VolumeCondition: str = Field(
        description="成交量条件：1=任意数量 AV、2=最小数量 MV、3=全部数量 CV。"
    )
    MinVolume: int = Field(description="最小成交量")
    ContingentCondition: str = Field(description="触发条件")
    StopPrice: float | None = Field(description="止损价；CTP 无效数值为 null。")
    ForceCloseReason: str = Field(description="强平原因")
    IsAutoSuspend: int = Field(description="自动挂起标志")
    BusinessUnit: str = Field(description="业务单元")
    RequestID: int = Field(description="请求编号")
    OrderLocalID: str = Field(description="本地报单编号")
    ExchangeID: str = Field(description="交易所代码")
    ParticipantID: str = Field(description="会员代码")
    ClientID: str = Field(description="客户代码")
    TraderID: str = Field(description="交易所交易员代码")
    InstallID: int = Field(description="安装编号")
    OrderSubmitStatus: str = Field(
        description="提交状态：0=报单已提交、1=撤单已提交、2=修改已提交、3=已接受、4=报单被拒绝、5=撤单被拒绝、6=改单被拒绝。"
    )
    NotifySequence: int = Field(description="报单提示序号")
    TradingDay: str = Field(
        description="CTP 交易日 YYYYMMDD；夜盘的自然日可能与交易日不同。"
    )
    SettlementID: int = Field(description="结算编号")
    OrderSysID: str = Field(
        description="交易所报单编号，可能包含前导空格；撤单时原样复制，不转整数。"
    )
    OrderSource: str = Field(description="报单来源")
    OrderStatus: str = Field(
        description="订单状态：0=全部成交、1=部分成交仍排队、2=部分成交不排队、3=未成交仍排队、4=未成交不排队、5=已撤单、a=未知、b=尚未触发、c=已触发。"
    )
    OrderType: str = Field(description="报单类型")
    VolumeTraded: int = Field(description="累计成交手数。")
    VolumeTotal: int = Field(description="剩余未成交手数。")
    InsertDate: str = Field(description="报单日期")
    InsertTime: str = Field(description="委托时间")
    ActiveTime: str = Field(description="激活时间")
    SuspendTime: str = Field(description="挂起时间")
    UpdateTime: str = Field(description="最后修改时间")
    CancelTime: str = Field(description="撤销时间")
    ActiveTraderID: str = Field(description="最后修改交易所交易员代码")
    ClearingPartID: str = Field(description="结算会员编号")
    SequenceNo: int = Field(description="序号")
    FrontID: int = Field(description="前置编号")
    SessionID: int = Field(description="会话编号")
    UserProductInfo: str = Field(description="用户端产品信息")
    StatusMsg: str = Field(description="状态信息")
    UserForceClose: int = Field(description="用户强平标志")
    ActiveUserID: str = Field(description="操作用户代码")
    BrokerOrderSeq: int = Field(description="经纪公司报单编号")
    RelativeOrderSysID: str = Field(description="相关报单")
    ZCETotalTradedVolume: int = Field(description="郑商所成交数量")
    IsSwapOrder: int = Field(description="互换单标志")
    BranchID: str = Field(description="营业部编号")
    InvestUnitID: str = Field(description="投资单元代码")
    AccountID: str = Field(description="资金账号")
    CurrencyID: str = Field(
        description="币种代码，如 CNY；本账户行的金额按此币种计价。"
    )
    MacAddress: str = Field(description="Mac地址")
    InstrumentID: str = Field(description="合约代码")
    ExchangeInstID: str = Field(description="合约在交易所的代码")
    IPAddress: str = Field(description="IP地址")
    OrderMemo: str = Field(
        description="原生报单回显字段；本服务提交操作时填写 12 字符的内部关联码，用于回报匹配，不是 HTTP 幂等键。"
    )
    SessionReqSeq: int = Field(description="session上请求计数 api自动维护")


class CtpTrade(BaseModel):
    """CTP TradeField。"""

    model_config = ConfigDict(extra="forbid")

    BrokerID: str = Field(description="经纪公司代码")
    InvestorID: str = Field(description="投资者代码")
    OrderRef: str = Field(
        description="原订单报单引用；结合 FrontID/SessionID 定位，不是全局订单 ID。"
    )
    UserID: str = Field(description="用户代码")
    ExchangeID: str = Field(description="交易所代码")
    TradeID: str = Field(description="成交编号")
    Direction: str = Field(description="买卖方向：0=买、1=卖；不等同于持仓多空方向。")
    OrderSysID: str = Field(
        description="交易所报单编号，可能包含前导空格；撤单时原样复制，不转整数。"
    )
    ParticipantID: str = Field(description="会员代码")
    ClientID: str = Field(description="客户代码")
    TradingRole: str = Field(description="交易角色")
    OffsetFlag: str = Field(
        description="开平标志：0=开仓、1=平仓、2=强平、3=平今、4=平昨、5=强减、6=本地强平。"
    )
    HedgeFlag: str = Field(
        description="投机套保标志：1=投机、2=套利、3=套保；保留原始编码。"
    )
    Price: float | None = Field(
        description="成交价格，使用合约报价单位；无效数值为 null。"
    )
    Volume: int = Field(description="成交数量，单位手。")
    TradeDate: str = Field(description="成交时期")
    TradeTime: str = Field(description="成交时间")
    TradeType: str = Field(description="成交类型")
    PriceSource: str = Field(description="成交价来源")
    TraderID: str = Field(description="交易所交易员代码")
    OrderLocalID: str = Field(description="本地报单编号")
    ClearingPartID: str = Field(description="结算会员编号")
    BusinessUnit: str = Field(description="业务单元")
    SequenceNo: int = Field(description="序号")
    TradingDay: str = Field(
        description="CTP 交易日 YYYYMMDD；夜盘的自然日可能与交易日不同。"
    )
    SettlementID: int = Field(description="结算编号")
    BrokerOrderSeq: int = Field(description="经纪公司报单编号")
    TradeSource: str = Field(description="成交来源")
    InvestUnitID: str = Field(description="投资单元代码")
    InstrumentID: str = Field(description="合约代码")
    ExchangeInstID: str = Field(description="合约在交易所的代码")
