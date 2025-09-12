# 文件名: adjust_trade_utils_decimal.py

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


def get_symbol(exchange_name, currency_type):
    """
    根据交易所名称和币种类型获取交易对字符串。

    该函数使用字符串拼接构建交易对，如果未找到则直接抛出 ValueError。

    参数:
        exchange_name (str): 交易所的名称，例如 "binance"。
        currency_type (str): 币种的类型，例如 "BTC" 或 "ETC"。

    返回:
        str: 对应的交易对字符串。

    抛出:
        ValueError: 如果交易所名称不匹配，则抛出此错误。
    """
    exchange_name = exchange_name.lower()
    currency_type = currency_type.upper()

    if exchange_name == "binance":
        # 币安的交易对格式: <币种>/USDT:USDT
        return f"{currency_type}/USDT:USDT"
    elif exchange_name == "kraken" or exchange_name == "krakenfutures":
        # Kraken 的交易对格式: <币种>/USD:USD
        return f"{currency_type}/USD:USD"

    # 如果没有找到匹配的交易所，直接抛出错误
    raise ValueError(f"不支持的交易所: {exchange_name}")


class InsufficientAmountError(Exception):
    pass


class InsufficientCostError(Exception):
    pass


def adjust_amount_from_usd(
    usd_amount,
    current_price,
    precision_amount,
):
    """
    将美元金额转换为基础币种数量，并根据交易所的精度进行调整。
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
    # 将所有 float 参数转换为 Decimal 类型
    usd_amount = Decimal(str(usd_amount))
    precision_amount = Decimal(str(precision_amount))
    current_price = Decimal(str(current_price))

    if current_price <= 0:
        raise ValueError("Current price must be a positive number.")

    preliminary_amount = usd_amount / current_price

    adjusted_amount = (preliminary_amount / precision_amount).to_integral_value(
        rounding=ROUND_FLOOR
    ) * precision_amount

    # 最小名义价值估算：如果调整后的数量小于一个最小步长，则认为过小
    if adjusted_amount < precision_amount:
        # 抛出 InsufficientAmountError，因为数量太小，无法形成有效订单
        raise InsufficientAmountError(
            f"调整后的数量 ({adjusted_amount}) 小于最小步长 ({precision_amount})，无法进行交易。"
        )

    return adjusted_amount


def adjust_price_to_precision(price, precision_price):
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


# ---
# 充分的测试用例

if __name__ == "__main__":

    def is_multiple_of_step(value, step):
        if step == 0:
            return value == Decimal("0")
        return (value % step) == Decimal("0")

    print("--- 开始测试 `adjust_amount_from_usd` (Decimal) 函数 ---")

    current_price_data = 20.396

    # 测试用例 1: 正常成功场景
    try:
        # 注意: 之前的 cost_min 参数已移除
        amount_to_buy = adjust_amount_from_usd(1000, current_price_data, 0.001)
        print(f"✅ 成功! 购买数量为: {amount_to_buy}")
        print(
            f"   总价值约为: {amount_to_buy * Decimal(str(current_price_data)):.2f} USDT"
        )
    except Exception as e:
        print(f"❌ 失败! 发生异常: {e}")

    # 测试用例 2: 金额过小，无法满足最小数量步长
    # 0.001 USDT / 20.396 = 0.000049...
    # precision_amount = 0.001 -> adjusted_amount = 0.0
    print("\n--- 测试用例 2: 金额过小，无法满足最小数量步长 ---")
    try:
        adjust_amount_from_usd(0.001, current_price_data, 0.001)
        print("❌ 失败! 应该抛出 InsufficientAmountError 异常。")
    except InsufficientAmountError as e:
        print(f"✅ 成功捕获异常: {e}")
    except Exception as e:
        print(f"❌ 失败! 捕获了错误的异常类型: {e}")

    # 测试用例 3: 价格为零或负数
    print("\n--- 测试用例 3: 价格为零或负数 ---")
    try:
        adjust_amount_from_usd(100, 0, 0.001)
        print("❌ 失败! 应该抛出 ValueError 异常。")
    except ValueError as e:
        print(f"✅ 成功捕获异常: {e}")
    except Exception as e:
        print(f"❌ 失败! 捕获了错误的异常类型: {e}")

    print("\n--- 开始测试 `adjust_price_to_precision` (Decimal) 函数 ---")

    # 测试用例 4: 正常价格四舍五入
    try:
        adjusted_price = adjust_price_to_precision(123.456, 0.01)
        print(f"✅ 成功! 调整前价格: 123.456, 调整后价格: {adjusted_price}")
        if is_multiple_of_step(adjusted_price, Decimal("0.01")):
            print("   (验证: 价格是精度步长的整数倍)")
        else:
            print("   (验证失败)")
    except Exception as e:
        print(f"❌ 失败! 发生异常: {e}")

    # 测试用例 5: 不规则精度四舍五入
    try:
        adjusted_price = adjust_price_to_precision(150.37, 0.15)
        print(f"✅ 成功! 调整前价格: 150.37, 调整后价格: {adjusted_price}")
        if is_multiple_of_step(adjusted_price, Decimal("0.15")):
            print("   (验证: 价格是精度步长的整数倍)")
        else:
            print("   (验证失败)")
    except Exception as e:
        print(f"❌ 失败! 发生异常: {e}")

    print("\n--- 所有测试完成 ---")
