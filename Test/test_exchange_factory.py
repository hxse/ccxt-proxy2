from src.tools.config_types import AppConfig
from src.tools.exchange import get_binance_exchange, get_kraken_exchange


class FakeExchange:
    def __init__(self, settings):
        self.settings = settings
        self.httpProxy = None
        self.demo_enabled = False
        self.sandbox_enabled = False
        self.factory: str | None = None

    def enable_demo_trading(self, enabled):
        self.demo_enabled = enabled

    def set_sandbox_mode(self, enabled):
        self.sandbox_enabled = enabled


def _config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "SECRET": "secret",
            "proxy": {"http": "http://127.0.0.1:7890"},
            "binance": {
                "enable_proxy": True,
                "test": {"api_key": "binance-test", "secret": "secret"},
                "live": {"api_key": "binance-live", "secret": "secret"},
            },
            "kraken": {
                "enable_proxy": True,
                "test": {"api_key": "kraken-test", "secret": "secret"},
                "live": {"api_key": "kraken-live", "secret": "secret"},
            },
        }
    )


def test_binance_future_factory_is_linear_only_and_uses_demo_mode(monkeypatch):
    created = []
    monkeypatch.setattr(
        "src.tools.exchange.ccxt.binance",
        lambda settings: created.append(FakeExchange(settings)) or created[-1],
    )

    exchange = get_binance_exchange(_config(), "future", "sandbox")

    assert exchange.settings["apiKey"] == "binance-test"
    assert exchange.settings["options"] == {
        "defaultType": "future",
        "fetchCurrencies": False,
        "fetchMargins": False,
        "fetchMarkets": {"types": ["linear"]},
    }
    assert exchange.demo_enabled is True
    assert exchange.sandbox_enabled is False
    assert exchange.httpProxy == "http://127.0.0.1:7890"


def test_binance_spot_live_factory_does_not_enable_demo(monkeypatch):
    created = []
    monkeypatch.setattr(
        "src.tools.exchange.ccxt.binance",
        lambda settings: created.append(FakeExchange(settings)) or created[-1],
    )

    exchange = get_binance_exchange(_config(), "spot", "live")

    assert exchange.settings["apiKey"] == "binance-live"
    assert exchange.settings["options"]["fetchMarkets"] == {"types": ["spot"]}
    assert exchange.demo_enabled is False


def test_kraken_future_and_spot_use_different_ccxt_classes(monkeypatch):
    created = []

    def factory(name):
        def create(settings):
            exchange = FakeExchange(settings)
            exchange.factory = name
            created.append(exchange)
            return exchange

        return create

    monkeypatch.setattr("src.tools.exchange.ccxt.krakenfutures", factory("future"))
    monkeypatch.setattr("src.tools.exchange.ccxt.kraken", factory("spot"))

    future = get_kraken_exchange(_config(), "future", "sandbox")
    spot = get_kraken_exchange(_config(), "spot", "live")

    assert future.factory == "future"
    assert future.settings["apiKey"] == "kraken-test"
    assert future.sandbox_enabled is True
    assert spot.factory == "spot"
    assert spot.settings["apiKey"] == "kraken-live"
    assert spot.sandbox_enabled is False
    assert all(item.httpProxy == "http://127.0.0.1:7890" for item in created)
