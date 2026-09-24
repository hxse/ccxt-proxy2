"""只公开已识别价格错误的安全字段，不转发 CCXT 的整段异常文本。"""

import json
import re

from src.order_prices import OrderPriceError

BINANCE_PRICE_CODES = {
    -4001: ("INVALID_ORDER_PRICE", "价格小于零。"),
    -4002: ("PRICE_OUT_OF_RANGE", "价格超过交易所允许的最大价格。"),
    -4013: ("PRICE_OUT_OF_RANGE", "价格低于交易所允许的最小价格。"),
    -4014: ("INVALID_PRICE_PRECISION", "价格不符合交易所最小变动价位。"),
    -4016: ("PRICE_OUT_OF_RANGE", "价格超过交易所当前允许的买入上限。"),
    -4024: ("PRICE_OUT_OF_RANGE", "价格低于交易所当前允许的卖出下限。"),
}
KRAKEN_PRICE_CODES = {
    "invalidPrice": ("INVALID_ORDER_PRICE", "Kraken 拒绝了委托价格或触发价格。"),
    "outsidePriceCollar": (
        "PRICE_OUT_OF_RANGE",
        "Kraken 委托价格超出允许的价格保护范围。",
    ),
    "EOrder:Tick size check failed": (
        "INVALID_PRICE_PRECISION",
        "Kraken 委托价格不符合最小变动价位。",
    ),
    "EOrder:Invalid price": ("INVALID_ORDER_PRICE", "Kraken 委托价格无效。"),
}


def price_rejection(exc):
    text = str(exc)
    provider = (
        "binance"
        if text.startswith(("binance ", "binance: "))
        else "kraken"
        if text.startswith(("kraken ", "kraken: ", "krakenfutures ", "krakenfutures: "))
        else None
    )
    if provider is None:
        return None
    payload = None
    start = text.find("{")
    if start >= 0:
        try:
            payload, _ = json.JSONDecoder().raw_decode(text[start:])
        except ValueError:
            pass
    provider_code = None
    selected = None
    if provider == "binance" and isinstance(payload, dict):
        provider_code = payload.get("code")
        if isinstance(provider_code, int) and not isinstance(provider_code, bool):
            selected = BINANCE_PRICE_CODES.get(provider_code)
    elif provider == "kraken":
        # Futures SDK 直接抛出 “krakenfutures createOrder() failed due to ...”。
        for code, entry in KRAKEN_PRICE_CODES.items():
            if re.search(r"(?<![A-Za-z])" + re.escape(code) + r"(?![A-Za-z])", text):
                provider_code, selected = code, entry
                break
    if selected is None:
        return None
    error = OrderPriceError(*selected)
    error.detail.update(provider=provider, provider_code=provider_code)
    if provider == "binance" and provider_code in {-4016, -4024}:
        message = payload.get("msg", "")
        match = (
            re.fullmatch(
                r"Limit price can't be (higher|lower) than ([0-9]+(?:\.[0-9]+)?)\.",
                message,
            )
            if isinstance(message, str)
            else None
        )
        if match:
            error.detail["price_context"] = {
                "upper_bound" if match[1] == "higher" else "lower_bound": match[2]
            }
    return error
