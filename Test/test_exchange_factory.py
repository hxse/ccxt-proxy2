import pytest
from requests.utils import select_proxy

from src.tools.config_types import AppConfig
from src.tools.exchange import get_binance_exchange, get_kraken_exchange


class FakeExchange:
    def __init__(self, settings):
        self.settings = settings
        self.proxies = None
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
    assert exchange.proxies == dict.fromkeys(("http", "https"), "http://127.0.0.1:7890")


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
    assert all(
        item.proxies == dict.fromkeys(("http", "https"), "http://127.0.0.1:7890")
        for item in created
    )


@pytest.mark.parametrize(
    "exchange_name, market",
    [("binance", "future"), ("kraken", "future"), ("kraken", "spot")],
)
@pytest.mark.parametrize("enabled", [True, False])
def test_real_sdk_applies_proxy_to_both_request_schemes(exchange_name, market, enabled):
    config = _config()
    getattr(config, exchange_name).enable_proxy = enabled
    factory = (
        get_binance_exchange if exchange_name == "binance" else get_kraken_exchange
    )
    exchange = factory(config, market, "live")
    observed = []

    class Captured(BaseException):
        pass

    def request(method, url, **kwargs):
        observed.append(select_proxy(url, kwargs.get("proxies") or {}))
        raise Captured

    exchange.session.request = request
    try:
        for scheme in ("http", "https"):
            with pytest.raises(Captured):
                exchange.fetch(scheme + "://example.invalid/time")
    finally:
        exchange.session.close()
    assert observed == [config.proxy.effective_http if enabled else None] * 2
    assert exchange.httpProxy is None and exchange.httpsProxy is None
