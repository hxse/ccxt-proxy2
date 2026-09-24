"""从当前 CTP 交易会话取得价格资料，不依赖其他行情服务。"""

from src.order_prices import (
    PriceRules,
    PriceRulesUnavailable,
    prepare_price,
    rule_decimal,
)


class CtpOrderPrices:
    def __init__(self):
        self._session_key = None
        self._ticks = {}

    @staticmethod
    def _match(rows, exchange_id, instrument_id):
        # CTP 前置可能按前缀返回期货及其所有期权，必须精确选中目标合约。
        exact = [
            row
            for row in rows
            if row.get("InstrumentID") == instrument_id
            and row.get("ExchangeID") == exchange_id
        ]
        if len(exact) != 1:
            raise PriceRulesUnavailable("CTP 价格资料没有唯一匹配的交易所和合约。")
        return exact[0]

    def prepare(self, session, request):
        key = (session, session.callbacks.generation, session.login["TradingDay"])
        if key != self._session_key:
            self._ticks.clear()
            self._session_key = key
        contract = (request.exchange_id, request.instrument_id)
        fields = {
            "ExchangeID": request.exchange_id,
            "InstrumentID": request.instrument_id,
        }
        if contract not in self._ticks:
            _, rows = session.request("ReqQryInstrument", fields)
            info = self._match(rows, *contract)
            self._ticks[contract] = rule_decimal(info.get("PriceTick"), "PriceTick")
        _, rows = session.request("ReqQryDepthMarketData", fields)
        quote = self._match(rows, *contract)
        if quote.get("TradingDay") != session.login["TradingDay"]:
            session.needs_reset = True
            raise PriceRulesUnavailable("CTP 涨跌停资料与当前登录交易日不一致。")
        rules = PriceRules(
            self._ticks[contract],
            rule_decimal(quote.get("LowerLimitPrice"), "LowerLimitPrice"),
            rule_decimal(quote.get("UpperLimitPrice"), "UpperLimitPrice"),
        )
        return prepare_price(request.price, request.side, rules)
