"""VeighNa CTP 原生字段的 HTTP 类型；省略 reserve* 无效字段。

字段依据上游 vnctptd.cpp 的字典回报，价格/金额的 CTP 无效 double 序列化为 null。
https://github.com/vnpy/vnpy_ctp/blob/main/vnpy_ctp/api/vnctp/vnctptd/vnctptd.cpp
"""

from pydantic import BaseModel, ConfigDict, Field


class CtpPosition(BaseModel):
    """CTP InvestorPositionField。"""

    model_config = ConfigDict(extra="forbid")

    BrokerID: str = Field(description="经纪公司代码")
    InvestorID: str = Field(description="投资者代码")
    PosiDirection: str = Field(description="持仓方向：1=净持仓、2=多头、3=空头。")
    HedgeFlag: str = Field(
        description="投机套保标志：1=投机、2=套利、3=套保；保留原始编码。"
    )
    PositionDate: str = Field(
        description="持仓日期类型：1=今仓、2=昨仓；各维度原样返回，不合并。"
    )
    YdPosition: int = Field(description="上日持仓手数，不直接代表当前可平昨仓。")
    Position: int = Field(description="该行持仓手数，不直接代表可平手数。")
    LongFrozen: int = Field(description="多头冻结")
    ShortFrozen: int = Field(description="空头冻结")
    LongFrozenAmount: float | None = Field(
        description="开仓冻结金额；CTP 无效数值为 null。"
    )
    ShortFrozenAmount: float | None = Field(
        description="开仓冻结金额；CTP 无效数值为 null。"
    )
    OpenVolume: int = Field(description="开仓量")
    CloseVolume: int = Field(description="平仓量")
    OpenAmount: float | None = Field(description="开仓金额；CTP 无效数值为 null。")
    CloseAmount: float | None = Field(description="平仓金额；CTP 无效数值为 null。")
    PositionCost: float | None = Field(description="持仓成本；CTP 无效数值为 null。")
    PreMargin: float | None = Field(
        description="上次占用的保证金；CTP 无效数值为 null。"
    )
    UseMargin: float | None = Field(description="占用的保证金；CTP 无效数值为 null。")
    FrozenMargin: float | None = Field(
        description="冻结的保证金；CTP 无效数值为 null。"
    )
    FrozenCash: float | None = Field(description="冻结的资金；CTP 无效数值为 null。")
    FrozenCommission: float | None = Field(
        description="冻结的手续费；CTP 无效数值为 null。"
    )
    CashIn: float | None = Field(description="资金差额；CTP 无效数值为 null。")
    Commission: float | None = Field(description="手续费；CTP 无效数值为 null。")
    CloseProfit: float | None = Field(description="平仓盈亏；CTP 无效数值为 null。")
    PositionProfit: float | None = Field(description="持仓盈亏；CTP 无效数值为 null。")
    PreSettlementPrice: float | None = Field(
        description="上次结算价；CTP 无效数值为 null。"
    )
    SettlementPrice: float | None = Field(
        description="本次结算价；CTP 无效数值为 null。"
    )
    TradingDay: str = Field(
        description="CTP 交易日 YYYYMMDD；夜盘的自然日可能与交易日不同。"
    )
    SettlementID: int = Field(description="结算编号")
    OpenCost: float | None = Field(description="开仓成本；CTP 无效数值为 null。")
    ExchangeMargin: float | None = Field(
        description="交易所保证金；CTP 无效数值为 null。"
    )
    CombPosition: int = Field(description="组合成交形成的持仓")
    CombLongFrozen: int = Field(description="组合多头冻结")
    CombShortFrozen: int = Field(description="组合空头冻结")
    CloseProfitByDate: float | None = Field(
        description="逐日盯市平仓盈亏；CTP 无效数值为 null。"
    )
    CloseProfitByTrade: float | None = Field(
        description="逐笔对冲平仓盈亏；CTP 无效数值为 null。"
    )
    TodayPosition: int = Field(description="该行今仓手数；保留上游语义。")
    MarginRateByMoney: float | None = Field(
        description="保证金率；CTP 无效数值为 null。"
    )
    MarginRateByVolume: float | None = Field(
        description="保证金率(按手数)；CTP 无效数值为 null。"
    )
    StrikeFrozen: int = Field(description="执行冻结")
    StrikeFrozenAmount: float | None = Field(
        description="执行冻结金额；CTP 无效数值为 null。"
    )
    AbandonFrozen: int = Field(description="放弃执行冻结")
    ExchangeID: str = Field(description="交易所代码")
    YdStrikeFrozen: int = Field(description="执行冻结的昨仓")
    InvestUnitID: str = Field(description="投资单元代码")
    PositionCostOffset: float | None = Field(
        description="持仓成本差值；CTP 无效数值为 null。"
    )
    TasPosition: int = Field(description="tas持仓手数")
    TasPositionCost: float | None = Field(
        description="tas持仓成本；CTP 无效数值为 null。"
    )
    InstrumentID: str = Field(description="合约代码")
    OptionValue: float | None = Field(description="期权市值；CTP 无效数值为 null。")


class CtpTradingAccount(BaseModel):
    """CTP TradingAccountField。"""

    model_config = ConfigDict(extra="forbid")

    BrokerID: str = Field(description="经纪公司代码")
    AccountID: str = Field(description="投资者帐号")
    PreMortgage: float | None = Field(description="上次质押金额；CTP 无效数值为 null。")
    PreCredit: float | None = Field(description="上次信用额度；CTP 无效数值为 null。")
    PreDeposit: float | None = Field(description="上次存款额；CTP 无效数值为 null。")
    PreBalance: float | None = Field(
        description="上次结算准备金；CTP 无效数值为 null。"
    )
    PreMargin: float | None = Field(
        description="上次占用的保证金；CTP 无效数值为 null。"
    )
    InterestBase: float | None = Field(description="利息基数；CTP 无效数值为 null。")
    Interest: float | None = Field(description="利息收入；CTP 无效数值为 null。")
    Deposit: float | None = Field(description="入金金额；CTP 无效数值为 null。")
    Withdraw: float | None = Field(description="出金金额；CTP 无效数值为 null。")
    FrozenMargin: float | None = Field(
        description="冻结的保证金；CTP 无效数值为 null。"
    )
    FrozenCash: float | None = Field(description="冻结的资金；CTP 无效数值为 null。")
    FrozenCommission: float | None = Field(
        description="冻结的手续费；CTP 无效数值为 null。"
    )
    CurrMargin: float | None = Field(
        description="当前保证金总额；CTP 无效数值为 null。"
    )
    CashIn: float | None = Field(description="资金差额；CTP 无效数值为 null。")
    Commission: float | None = Field(description="手续费；CTP 无效数值为 null。")
    CloseProfit: float | None = Field(description="平仓盈亏；CTP 无效数值为 null。")
    PositionProfit: float | None = Field(description="持仓盈亏；CTP 无效数值为 null。")
    Balance: float | None = Field(
        description="期货结算准备金，单位为 CurrencyID 指定的币种；无效数值为 null。"
    )
    Available: float | None = Field(
        description="可用资金，单位为 CurrencyID 指定的币种；无效数值为 null。"
    )
    WithdrawQuota: float | None = Field(description="可取资金；CTP 无效数值为 null。")
    Reserve: float | None = Field(description="基本准备金；CTP 无效数值为 null。")
    TradingDay: str = Field(
        description="CTP 交易日 YYYYMMDD；夜盘的自然日可能与交易日不同。"
    )
    SettlementID: int = Field(description="结算编号")
    Credit: float | None = Field(description="信用额度；CTP 无效数值为 null。")
    Mortgage: float | None = Field(description="质押金额；CTP 无效数值为 null。")
    ExchangeMargin: float | None = Field(
        description="交易所保证金；CTP 无效数值为 null。"
    )
    DeliveryMargin: float | None = Field(
        description="投资者交割保证金；CTP 无效数值为 null。"
    )
    ExchangeDeliveryMargin: float | None = Field(
        description="交易所交割保证金；CTP 无效数值为 null。"
    )
    ReserveBalance: float | None = Field(
        description="保底期货结算准备金；CTP 无效数值为 null。"
    )
    CurrencyID: str = Field(
        description="币种代码，如 CNY；本账户行的金额按此币种计价。"
    )
    PreFundMortgageIn: float | None = Field(
        description="上次货币质入金额；CTP 无效数值为 null。"
    )
    PreFundMortgageOut: float | None = Field(
        description="上次货币质出金额；CTP 无效数值为 null。"
    )
    FundMortgageIn: float | None = Field(
        description="货币质入金额；CTP 无效数值为 null。"
    )
    FundMortgageOut: float | None = Field(
        description="货币质出金额；CTP 无效数值为 null。"
    )
    FundMortgageAvailable: float | None = Field(
        description="货币质押余额；CTP 无效数值为 null。"
    )
    MortgageableFund: float | None = Field(
        description="可质押货币金额；CTP 无效数值为 null。"
    )
    SpecProductMargin: float | None = Field(
        description="特殊产品占用保证金；CTP 无效数值为 null。"
    )
    SpecProductFrozenMargin: float | None = Field(
        description="特殊产品冻结保证金；CTP 无效数值为 null。"
    )
    SpecProductCommission: float | None = Field(
        description="特殊产品手续费；CTP 无效数值为 null。"
    )
    SpecProductFrozenCommission: float | None = Field(
        description="特殊产品冻结手续费；CTP 无效数值为 null。"
    )
    SpecProductPositionProfit: float | None = Field(
        description="特殊产品持仓盈亏；CTP 无效数值为 null。"
    )
    SpecProductCloseProfit: float | None = Field(
        description="特殊产品平仓盈亏；CTP 无效数值为 null。"
    )
    SpecProductPositionProfitByAlg: float | None = Field(
        description="根据持仓盈亏算法计算的特殊产品持仓盈亏；CTP 无效数值为 null。"
    )
    SpecProductExchangeMargin: float | None = Field(
        description="特殊产品交易所保证金；CTP 无效数值为 null。"
    )
    BizType: str = Field(description="业务类型")
    FrozenSwap: float | None = Field(
        description="延时换汇冻结金额；CTP 无效数值为 null。"
    )
    RemainSwap: float | None = Field(description="剩余换汇额度；CTP 无效数值为 null。")
    OptionValue: float | None = Field(description="期权市值；CTP 无效数值为 null。")
