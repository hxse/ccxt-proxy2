from decimal import Decimal, getcontext, ROUND_FLOOR, ROUND_HALF_UP

getcontext().prec = 28


# trade_symbols = {
#     "binance": {
#         "_": "BTC/USD:BTC",  # amount是等值1cont*100, 不建议用
#         "BTC": "BTC/USDT:USDT",  # amount是等值BTC
#         "ETC": "ETC/USDT:USDT",  # amount是等值BTC
#     },
#     "kraken": {
#         "_": "BTC/USD:BTC",  # amount是等值USD, 不建议用
#         "BTC": "BTC/USD:USD",  # amount是等值BTC
#         "ETC": "ETC/USD:USD",  # amount是等值BTC
#     },
# }


def get_symbol(exchange_name, symbol):
    """
    根据交易所名称和币种类型获取交易对字符串。

    该函数使用字符串拼接构建交易对，如果未找到则直接抛出 ValueError。

    参数:
        exchange_name (str): 交易所的名称，例如 "binance"。
        symbol (str): 币种的类型，例如 "BTC" 或 "ETC"。

    返回:
        str: 对应的交易对字符串。

    抛出:
        ValueError: 如果交易所名称不匹配，则抛出此错误。
    """
    exchange_name = exchange_name.lower()
    symbol = symbol.upper()
    seg = symbol.split("/")
    seg_first = seg[0]
    seg_second = seg[1]

    if exchange_name == "binance":
        # 币安的交易对格式: <币种>/USDT:USDT
        return f"{seg_first}/{seg_second}:{seg_second}"
    elif exchange_name in ["kraken", "krakenfutures"]:
        # Kraken 的交易对格式: <币种>/USD:USD
        if seg_second == "USDT":
            seg_second = "USD"
        return f"{seg_first}/{seg_second}:{seg_second}"

    # 如果没有找到匹配的交易所，直接抛出错误
    raise ValueError(f"不支持的交易所: {exchange_name}")


class InsufficientAmountError(Exception):
    pass


class InsufficientCostError(Exception):
    pass


def adjust_coin_amount(amount: float, precision_amount: float) -> Decimal:
    """
    根据给定的精度调整币的数量。

    参数:
        amount (Decimal): 待调整的币数量。
        precision_amount (Decimal): 数量的最小变动单位 (币)。

    返回:
        Decimal: 调整后的币数量。

    抛出:
        InsufficientAmountError: 如果调整后的数量为零或过小。
    """

    amount_decimal = Decimal(str(amount))
    precision_amount_decimal = Decimal(str(precision_amount))

    adjusted_amount = (amount_decimal / precision_amount_decimal).to_integral_value(
        rounding=ROUND_FLOOR
    ) * precision_amount_decimal

    if adjusted_amount < precision_amount_decimal:
        raise InsufficientAmountError(
            f"调整后的数量 ({adjusted_amount}) 小于最小步长 ({precision_amount})，无法进行交易。"
        )
    return adjusted_amount


def adjust_usd_to_coin_amount(
    usd_amount: float,
    current_price: float,
    precision_amount: float,
) -> Decimal:
    """
    将美元金额转换为基础币种数量，并最终根据精度进行调整。
    该函数会自动根据 `precision_amount` 估算最小可交易数量。

    参数:
        usd_amount (float): 想要花费的美元金额 (U)。
        current_price (float): 当前市场价格 (U)。
        precision_amount (float): 数量的最小变动单位 (币)。

    返回:
        Decimal: 调整后的交易数量 (币)。

    抛出:
        ValueError: 如果当前价格无效。
        InsufficientAmountError: 如果调整后的数量为零或过小。
    """
    usd_amount_decimal = Decimal(str(usd_amount))
    precision_amount_decimal = Decimal(str(precision_amount))
    current_price_decimal = Decimal(str(current_price))

    if current_price_decimal <= 0:
        raise ValueError("Current price must be a positive number.")

    preliminary_amount = usd_amount_decimal / current_price_decimal

    return adjust_coin_amount(preliminary_amount, precision_amount_decimal)


def adjusted_market_price(price, precision_price):
    """
    将价格四舍五入到指定的精度。

    此函数通过四舍五入确保价格符合交易所的最小变动单位。

    参数:
        price (float): 原始价格 (U)。
        precision_price (float): 价格的最小变动单位 (U)。

    返回:
        Decimal: 调整后的价格 (U)。
    """
    price = Decimal(str(price))
    precision_price = Decimal(str(precision_price))

    if precision_price <= 0:
        return price

    adjusted_price = (price / precision_price).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP
    ) * precision_price

    return adjusted_price


def adjust_coin_amount_wrapper(exchange, symbol: str, coin_amount: float) -> Decimal:
    """
    根据交易所的数量精度，调整并返回符合要求的币单位交易数量。

    此函数通过获取指定交易对的市场信息，并应用交易所定义的数量精度规则，
    确保返回的币单位数量符合该交易所的最小交易步长要求。

    参数:
        exchange: ccxt交易所实例。
        symbol (str): 交易对字符串，例如 "BTC/USDT"。
        coin_amount (float): 原始币单位交易数量。

    返回:
        Decimal: 包含调整后的币单位交易数量。

    抛出:
        ccxt.NetworkError: 如果获取市场信息失败。
        InsufficientAmountError: 如果调整后的数量为零或过小。
    """
    if coin_amount is None:
        return None

    exchange_market_info = exchange.market(symbol)
    amount_precision_value = exchange_market_info["precision"]["amount"]

    adjusted_coin_amount = adjust_coin_amount(coin_amount, amount_precision_value)
    return adjusted_coin_amount


def adjust_usd_to_coin_amount_wrapper(
    exchange, trading_pair: str, usd_amount: float
) -> Decimal:
    """
    将 U 单位金额转换为币单位数量，并根据市场精度进行调整。

    此函数通过获取指定交易对的市场数据和最新行情，将用户提供的 U 单位金额
    （如 USDT 或 USD）转换为对应的币单位数量（如 BTC），并应用交易所定义的
    数量精度规则进行优化，确保返回的币单位数量符合交易所的最小交易步长。

    参数:
        exchange: ccxt交易所实例。
        trading_pair (str): 交易对字符串，例如 "BTC/USDT"。
        usd_amount (float): 待转换和调整的 U 单位金额。

    返回:
        Decimal: 调整后的币单位交易数量。

    抛出:
        ccxt.NetworkError: 如果获取市场或行情信息失败。
        ValueError: 如果当前价格无效。
        InsufficientAmountError: 如果调整后的数量为零或过小。
    """
    if usd_amount is None:
        return None

    exchange_market_data = exchange.market(trading_pair)
    amount_precision_step = exchange_market_data["precision"]["amount"]

    market_ticker = exchange.fetchTicker(trading_pair)
    last_trade_price = market_ticker["last"]

    adjusted_coin_amount = adjust_usd_to_coin_amount(
        usd_amount, last_trade_price, amount_precision_step
    )
    return adjusted_coin_amount


def adjusted_market_price_wrapper(
    exchange, symbol: str, original_price: float
) -> Decimal:
    """
    根据交易所的市场价格精度，调整并返回符合交易规则的价格。

    此函数通过获取指定交易对的市场信息，并应用交易所定义的价格精度规则，
    确保返回的调整后价格符合该交易所的最小价格步长要求。

    参数:
        exchange: ccxt交易所实例。
        symbol (str): 交易对字符串，例如 "BTC/USDT"。
        original_price (float): 原始的交易价格（浮点数）。

    返回:
        Decimal: 调整后的交易价格（Decimal 类型），符合交易所精度。

    抛出:
        ccxt.NetworkError: 如果获取市场信息失败。
    """
    if original_price is None:
        return None

    market_info = exchange.market(symbol)
    market_price_precision_step = market_info["precision"]["price"]

    adjusted_price = adjusted_market_price(original_price, market_price_precision_step)
    return adjusted_price
