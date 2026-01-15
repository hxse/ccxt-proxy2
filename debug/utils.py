import json
import ccxt
from pathlib import Path

# 加载配置
CONFIG_PATH = Path(__file__).parent.parent / "data" / "config.json"


def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def get_binance_sandbox(market: str = "future"):
    """获取币安沙盒实例 - 只允许 sandbox 模式"""
    config = load_config()
    http_proxy = config["proxy"]["http"]

    # 获取 test key
    api_key = config["binance"]["test"]["api_key"]
    secret = config["binance"]["test"]["secret"]

    exchange = ccxt.binance(
        {
            "apiKey": api_key,
            "secret": secret,
            "enableRateLimit": True,
            "options": {"defaultType": market},
        }
    )

    # 设置代理
    if config["binance"]["enable_proxy"]:
        exchange.httpProxy = http_proxy

    # 强制沙盒模式
    exchange.enable_demo_trading(True)

    # 加载市场
    exchange.load_markets()
    return exchange


def get_kraken_sandbox(market: str = "future"):
    """获取海妖沙盒实例 - 只允许 sandbox 模式"""
    config = load_config()
    http_proxy = config["proxy"]["http"]

    # 获取 test key
    api_key = config["kraken"]["test"]["api_key"]
    secret = config["kraken"]["test"]["secret"]

    if market == "future":
        exchange = ccxt.krakenfutures(
            {
                "apiKey": api_key,
                "secret": secret,
                "enableRateLimit": True,
            }
        )
    else:
        exchange = ccxt.kraken(
            {
                "apiKey": api_key,
                "secret": secret,
                "enableRateLimit": True,
            }
        )

    # 设置代理
    if config["kraken"]["enable_proxy"]:
        exchange.httpProxy = http_proxy

    # 强制沙盒模式
    exchange.set_sandbox_mode(True)

    # 加载市场
    exchange.load_markets()
    return exchange


def print_json(data, title: str = ""):
    """格式化打印 JSON"""
    if title:
        print(f"\n{'=' * 60}\n{title}\n{'=' * 60}")
    print(json.dumps(data, indent=2, default=str, ensure_ascii=False))
