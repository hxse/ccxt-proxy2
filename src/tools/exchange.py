import ccxt


def get_binance_exchange(config):
    market_type = config["market_type"]
    Sandbox_mode = config["Sandbox_mode"]
    http_proxy = config["proxy"]["http"]

    binance_enable_proxy = config["binance"]["enable_proxy"]
    binance_api_key = config["binance"][Sandbox_mode]["api_key"]
    binance_secret = config["binance"][Sandbox_mode]["secret"]

    binance_exchange = ccxt.binance(
        {
            "apiKey": binance_api_key,
            "secret": binance_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": market_type,
            },
        }
    )
    binance_exchange.httpProxy = http_proxy if binance_enable_proxy else None
    if Sandbox_mode == "test":
        # 币安test模式已废弃, 改用demo模式
        # https://www.binance.com/zh-CN/support/faq/detail/9be58f73e5e14338809e3b705b9687dd
        # binance_exchange.set_sandbox_mode(True)
        binance_exchange.enable_demo_trading(True)

    binance_exchange.load_markets()

    return binance_exchange


def get_kraken_exchange(config):
    market_type = config["market_type"]
    Sandbox_mode = config["Sandbox_mode"]
    http_proxy = config["proxy"]["http"]

    kraken_enable_proxy = config["kraken"]["enable_proxy"]
    kraken_api_key = config["kraken"][Sandbox_mode]["api_key"]
    kraken_secret = config["kraken"][Sandbox_mode]["secret"]

    if market_type == "future":
        kraken_exchange = ccxt.krakenfutures(
            {
                "apiKey": kraken_api_key,
                "secret": kraken_secret,
                "enableRateLimit": True,
            }
        )
        kraken_exchange.httpProxy = http_proxy if kraken_enable_proxy else None
        if Sandbox_mode == "test":
            kraken_exchange.set_sandbox_mode(True)
    else:
        kraken_exchange = ccxt.kraken(
            {
                "apiKey": kraken_api_key,
                "secret": kraken_secret,
                "enableRateLimit": True,
            }
        )
        kraken_exchange.httpProxy = http_proxy if kraken_enable_proxy else None
        if Sandbox_mode == "test":
            kraken_exchange.set_sandbox_mode(True)

    kraken_exchange.load_markets()

    return kraken_exchange
