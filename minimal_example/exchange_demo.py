import sys
import ccxt
import json
from typing import List
from adjust_amount import adjust_amount_from_usd, adjust_price_to_precision, get_symbol

with open("./data/config.json", "r", encoding="utf-8") as file:
    config = json.load(file)


exchange_name = "binance"
exchange_name = "krakenfutures"

api_key = config[exchange_name.removesuffix("futures")]["test"]["api_key"]
secret = config[exchange_name.removesuffix("futures")]["test"]["secret"]

exchange = getattr(ccxt, exchange_name)(
    {
        "apiKey": api_key,
        "secret": secret,
        "enableRateLimit": True,
        "options": {
            "defaultType": "future",
        },
    }
)

exchange.set_sandbox_mode(True)


exchange.load_markets()


all_symbols = exchange.symbols


_s = [i for i in all_symbols if i.startswith("BTC/".upper())]
print(_s)


symbol = get_symbol(exchange_name, "BTC")
# symbol = get_symbol(exchange_name, "ETC")

market = exchange.market(symbol)


amount = 150  # U单位
precision_amount = market["precision"]["amount"]  # 币单位
precision_price = market["precision"]["price"]  # U单位
ticker = exchange.fetchTicker(symbol)  # U单位
current_price = ticker["last"]  # U单位

amount = adjust_amount_from_usd(amount, current_price, precision_amount)

sl_price = adjust_price_to_precision(current_price * 0.95, precision_price)
tp_price = adjust_price_to_precision(current_price * 1.05, precision_price)

print(amount, precision_price, precision_amount, sl_price)

order = exchange.create_order(
    symbol,
    "market",
    "buy",
    amount,
    None,
    {
        "reduceOnly": False,
        "triggerPrice": None,
        "stopLossPrice": None,
        "takeProfitPrice": None,
    },
)


# ['LIMIT', 'MARKET', 'STOP', 'STOP_MARKET', 'TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'TRAILING_STOP_MARKET']
order = exchange.create_order(
    symbol,
    "STOP_MARKET" if exchange_name == "binance" else "market",
    "sell",
    amount,
    None,
    {
        "reduceOnly": True,
        "triggerPrice": None,
        "stopLossPrice": sl_price,
        "takeProfitPrice": None,
    },
)

order = exchange.create_order(
    symbol,
    "TAKE_PROFIT_MARKET" if exchange_name == "binance" else "market",
    "sell",
    amount,
    None,
    {
        "reduceOnly": True,
        "triggerPrice": None,
        "stopLossPrice": tp_price,
        "takeProfitPrice": None,
    },
)


def close_position_all(exchange, symbol):
    # todo, 把这个添加到路由上去

    """
    close all exist position
    """
    params = {"reduceOnly": True}
    positions = exchange.fetch_positions([symbol])
    for i in positions:
        side = "sell" if i["side"] == "long" else "buy"
        amount = i["contracts"]
        exchange.create_order(symbol, "market", side, amount, params=params)
    remaining_positions = exchange.fetch_positions([symbol])
    return remaining_positions


import pdb

pdb.set_trace()
all_orders = close_position_all(exchange, symbol)


import pdb

pdb.set_trace()

order = exchange.cancelAllOrders(symbol)
