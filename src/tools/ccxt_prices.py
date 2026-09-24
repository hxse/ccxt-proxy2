"""CCXT 价格规则适配；规则和参考行情都取自当前 exchange 实例。"""

from decimal import Decimal

import ccxt

from src.order_prices import (
    OrderPriceError,
    PriceRules,
    PriceRulesUnavailable,
    check_price_bounds,
    prepare_price,
    rule_decimal,
)

UNMANAGED_PRICE_PARAMS = {
    "price",
    "limitPrice",
    "price2",
    "priceMatch",
    "stopPrice",
    "triggerPrice",
    "activationPrice",
    "trailingTriggerPrice",
    "stopLoss",
    "takeProfit",
    "pair",
    "type",
    "symbol",
    "side",
    "trailingAmount",
    "trailingPercent",
    "trailingLimitAmount",
    "trailingLimitPercent",
    "limitPriceOffsetValue",
    "limitPriceOffsetUnit",
    "close",
}
TRIGGER_PARAMS = {"stopLossPrice", "takeProfitPrice"}


class CcxtOrderPrices:
    def __init__(self, exchange, provider: str, market_type: str, transport):
        self.exchange, self.provider = exchange, provider
        self.market_type, self.transport = market_type, transport

    def prepare(self, market: dict, order_type: str, side: str, price, params: dict):
        reserved = UNMANAGED_PRICE_PARAMS.intersection(params)
        if reserved:
            raise OrderPriceError(
                "INVALID_ORDER_PRICE",
                "原生扩展参数不能覆盖后端管理的报价；请使用正式价格字段。",
                fields=sorted(reserved),
            )
        for name in ("orderType", "ordertype"):
            if name in params and (
                order_type != "limit"
                or params[name] not in {"limit", "lmt", "ioc", "fok", "post"}
            ):
                raise OrderPriceError(
                    "INVALID_ORDER_PRICE", "原生订单类型不能绕过限价处理。", field=name
                )
        triggers = TRIGGER_PARAMS.intersection(params)
        if price is None and not triggers:
            if order_type == "limit":
                raise OrderPriceError("INVALID_ORDER_PRICE", "限价单必须提供 price。")
            return None
        rules = self.static_rules(market)
        for name in triggers:
            trigger = prepare_price(params[name], side, rules, field=name, strict=True)
            self._check_sdk_price(market, trigger.submitted_price)
        if price is None:
            return None
        adjustment = prepare_price(price, side, rules)
        self._check_sdk_price(market, adjustment.submitted_price)
        if not triggers:
            self.check_dynamic(market, side, adjustment)
        return adjustment

    def _check_sdk_price(self, market, submitted):
        try:
            sdk_price = self.exchange.price_to_precision(market["symbol"], submitted)
        except (ccxt.BaseError, ValueError, TypeError):
            raise PriceRulesUnavailable(
                "SDK 无法表示按官方步长对齐后的价格。"
            ) from None
        if rule_decimal(sdk_price, "SDK price") != Decimal(submitted):
            raise PriceRulesUnavailable("SDK 精度规则与官方价格网格不一致，停止提交。")

    def static_rules(self, market: dict) -> PriceRules:
        info = market.get("info") or {}
        if self.provider == "binance":
            filters = self._filters(market)
            price_filter = filters.get("PRICE_FILTER")
            if (
                not price_filter
                or not {"tickSize", "minPrice", "maxPrice"} <= price_filter.keys()
            ):
                raise PriceRulesUnavailable("Binance 合约资料缺少 PRICE_FILTER。")
            tick = rule_decimal(price_filter.get("tickSize"), "tickSize")
            lower = self._bound(price_filter.get("minPrice"), "minPrice")
            upper = self._bound(price_filter.get("maxPrice"), "maxPrice")
            return PriceRules(tick, lower, upper, lower or Decimal(0))
        if self.market_type == "future" and "tickSize" in info:
            tick = rule_decimal(info["tickSize"], "tickSize")
        else:
            precision = (market.get("precision") or {}).get("price")
            mode = self.exchange.precisionMode
            if mode == ccxt.TICK_SIZE:
                tick = rule_decimal(precision, "price precision")
            elif (
                mode == ccxt.DECIMAL_PLACES
                and isinstance(precision, int)
                and not isinstance(precision, bool)
            ):
                tick = Decimal(1).scaleb(-precision)
            else:
                raise PriceRulesUnavailable("交易对缺少可用的价格步长资料。")
        limits = (market.get("limits") or {}).get("price") or {}
        return PriceRules(
            tick,
            self._bound(limits.get("min"), "minPrice"),
            self._bound(limits.get("max"), "maxPrice"),
        )

    @staticmethod
    def _bound(value, name):
        if value is None:
            return None
        result = rule_decimal(value, name, allow_zero=True)
        return result or None

    @staticmethod
    def _filters(market):
        return {
            f["filterType"]: f for f in (market.get("info") or {}).get("filters", [])
        }

    def _public(self, method: str, market: dict):
        data = self.transport.read_call(
            "price reference", getattr(self.exchange, method), {"symbol": market["id"]}
        )
        if not isinstance(data, dict) or (
            "symbol" in data and data["symbol"] != market["id"]
        ):
            raise PriceRulesUnavailable("价格参考行情没有匹配请求的交易对。")
        return data

    def check_dynamic(self, market: dict, side: str, adjustment) -> None:
        price = Decimal(adjustment.submitted_price)
        if self.provider == "binance":
            filters = self._filters(market)
            if self.market_type == "future":
                rule = filters.get("PERCENT_PRICE")
                if rule is None:
                    raise PriceRulesUnavailable("Binance 合约资料缺少 PERCENT_PRICE。")
                data = self._public("fapiPublicGetPremiumIndex", market)
                reference = rule_decimal(data.get("markPrice"), "markPrice")
                lower = (
                    reference
                    * rule_decimal(rule.get("multiplierDown"), "multiplierDown")
                    if side == "sell"
                    else None
                )
                upper = (
                    reference * rule_decimal(rule.get("multiplierUp"), "multiplierUp")
                    if side == "buy"
                    else None
                )
                self._check(price, lower, upper, adjustment)
            else:
                for name in ("PERCENT_PRICE", "PERCENT_PRICE_BY_SIDE"):
                    if name in filters:
                        self._spot_bounds(
                            market, filters[name], side, price, adjustment
                        )
        elif self.market_type == "future":
            ticker = self.transport.read_method(
                "fetchTicker", "fetch_ticker", market["symbol"]
            )
            mark = rule_decimal(ticker.get("markPrice"), "markPrice")
            bid = rule_decimal(ticker.get("bid"), "bid")
            ask = rule_decimal(ticker.get("ask"), "ask")
            if bid > ask:
                raise PriceRulesUnavailable("Kraken 买卖报价相互矛盾。")
            # 官方 Derivatives order types：跨盘口限价受 mark price 20% collar 约束。
            # https://support.kraken.com/articles/360031471211-derivatives-order-types
            if (side == "buy" and price >= ask) or (side == "sell" and price <= bid):
                self._check(
                    price, mark * Decimal("0.8"), mark * Decimal("1.2"), adjustment
                )

    def _spot_bounds(self, market, rule, side, price, adjustment):
        minutes = rule.get("avgPriceMins")
        if not isinstance(minutes, int) or isinstance(minutes, bool) or minutes < 0:
            raise PriceRulesUnavailable("Binance 价格过滤器缺少有效参考窗口。")
        if minutes == 0:
            data = self._public("publicGetTickerPrice", market)
        else:
            data = self._public("publicGetAvgPrice", market)
            if data.get("mins") != minutes:
                raise PriceRulesUnavailable("Binance 平均价窗口与过滤器不一致。")
        reference = rule_decimal(data.get("price"), "reference price")
        prefix = (
            ("bid" if side == "buy" else "ask")
            if rule["filterType"] == "PERCENT_PRICE_BY_SIDE"
            else ""
        )
        down = prefix + "MultiplierDown" if prefix else "multiplierDown"
        up = prefix + "MultiplierUp" if prefix else "multiplierUp"
        self._check(
            price,
            reference * rule_decimal(rule.get(down), down),
            reference * rule_decimal(rule.get(up), up),
            adjustment,
        )

    @staticmethod
    def _check(price, lower, upper, adjustment):
        if lower is not None and upper is not None and lower > upper:
            raise PriceRulesUnavailable("动态价格下界大于上界。")
        check_price_bounds(
            price,
            lower,
            upper,
            requested_price=adjustment.requested_price,
            tick_size=adjustment.tick_size,
            field="price",
        )
