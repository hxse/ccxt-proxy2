import ccxt

from src.base_types import MarketType, ModeType
from src.tools.config_types import AppConfig


def get_binance_exchange(
    config: AppConfig, market: MarketType, mode: ModeType = "sandbox"
):
    binance_config = config.binance
    if binance_config is None:
        raise ValueError("binance config is missing")

    http_proxy = config.proxy.effective_http
    credentials = binance_config.test if mode == "sandbox" else binance_config.live
    if credentials is None:
        raise ValueError(f"binance {mode} credentials are missing")

    fetch_market_types = ["linear"] if market == "future" else ["spot"]

    binance_exchange = ccxt.binance(
        {
            "apiKey": credentials.api_key,
            "secret": credentials.secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": market,
                "fetchCurrencies": False,
                "fetchMargins": False,
                "fetchMarkets": {"types": fetch_market_types},
            },
        }
    )
    binance_exchange.httpProxy = http_proxy if binance_config.enable_proxy else None
    if mode == "sandbox":
        # 币安test模式已废弃, 改用demo模式
        # https://www.binance.com/zh-CN/support/faq/detail/9be58f73e5e14338809e3b705b9687dd
        # binance_exchange.set_sandbox_mode(True)
        binance_exchange.enable_demo_trading(True)

    return binance_exchange


def get_kraken_exchange(
    config: AppConfig, market: MarketType, mode: ModeType = "sandbox"
):
    if market == "spot" and mode == "sandbox":
        raise ValueError("kraken spot sandbox is not supported")
    kraken_config = config.kraken
    if kraken_config is None:
        raise ValueError("kraken config is missing")

    http_proxy = config.proxy.effective_http
    credentials = kraken_config.test if mode == "sandbox" else kraken_config.live
    if credentials is None:
        raise ValueError(f"kraken {mode} credentials are missing")

    if market == "future":
        kraken_exchange = ccxt.krakenfutures(
            {
                "apiKey": credentials.api_key,
                "secret": credentials.secret,
                "enableRateLimit": True,
            }
        )
        kraken_exchange.httpProxy = http_proxy if kraken_config.enable_proxy else None
        if mode == "sandbox":
            kraken_exchange.set_sandbox_mode(True)
    else:
        kraken_exchange = ccxt.kraken(
            {
                "apiKey": credentials.api_key,
                "secret": credentials.secret,
                "enableRateLimit": True,
            }
        )
        kraken_exchange.httpProxy = http_proxy if kraken_config.enable_proxy else None
        if mode == "sandbox":
            kraken_exchange.set_sandbox_mode(True)

    return kraken_exchange
