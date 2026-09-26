import ccxt
import pytest

from src.cache_tool import OhlcvResult
from src.cache_tool.models import canonical_row
from src.domain_errors import (
    CapabilityNotSupported,
    InvalidProviderRequest,
    NetworkIncomplete,
)
from src.tools.ccxt_client import CcxtClient
from Test.test_ccxt_client import MINUTE, _cache, _client, _minutes, _row

SYMBOL = "BTC/USDT:USDT"


@pytest.mark.parametrize("mode", ["since-limit", "since-latest", "latest-limit"])
def test_queries_share_forward_calls_and_only_drop_final_network_tail(temp_dir, mode):
    client, exchange = _client(temp_dir, times=_minutes(*range(1, 9)))
    client._ohlcv.page_limit = 3
    if mode == "since-limit":
        result = client.fetch_ohlcv_since_limit(SYMBOL, "1m", 4 * MINUTE, 5)
    elif mode == "since-latest":
        result = client.fetch_ohlcv_since_latest(SYMBOL, "1m", 4 * MINUTE)
    else:
        result = client.fetch_ohlcv_latest_limit(SYMBOL, "1m", 5)
    assert [r[0] for r in result.rows] == _minutes(4, 5, 6, 7, 8)
    assert result.last_bar_completion_confirmed is False
    calls = exchange.ohlcv_calls
    assert all(call["params"] == {} for call in calls)
    assert [call["since"] for call in calls if call["since"] is not None] == _minutes(
        4, 6
    )
    assert sum(call["since"] is None for call in calls) == (mode != "since-limit")
    cached = client.cache.read_best_prefix(
        client._series(SYMBOL, "1m", "default").key, 4 * MINUTE, None
    )
    assert [r[0] for r in cached] == _minutes(4, 5, 6, 7)


def test_latest_limit_keeps_initial_snapshot_and_ignores_invalid_newer_price(temp_dir):
    client, exchange = _client(temp_dir, times=_minutes(1, 2, 3, 4, 5))
    original = exchange.fetch_ohlcv

    def fetch(*args, **kwargs):
        rows = original(*args, **kwargs)
        if kwargs["since"] is not None:
            rows.append([6 * MINUTE, None, None, None, None, None])
        return rows

    exchange.fetch_ohlcv = fetch
    result = client.fetch_ohlcv_latest_limit(SYMBOL, "1m", 3)
    assert [r[0] for r in result.rows] == _minutes(3, 4, 5)
    assert sum(c["since"] is None for c in exchange.ohlcv_calls) == 1
    assert (
        client.cache.read_latest_summary(
            client._series(SYMBOL, "1m", "default").key
        ).end
        == 4 * MINUTE
    )


def test_latest_limit_reads_trusted_cache_clipped_to_snapshot(temp_dir):
    client, exchange = _client(temp_dir, times=_minutes(1, 2, 3, 4, 5))
    series = client._series(SYMBOL, "1m", "default")
    client.cache.write_segment(
        series.key,
        OhlcvResult([canonical_row(_row(t)) for t in _minutes(1, 2, 3, 4, 5, 6)], True),
        MINUTE,
    )
    result = client.fetch_ohlcv_latest_limit(SYMBOL, "1m", 3)
    assert [r[0] for r in result.rows] == _minutes(3, 4, 5)
    assert result.last_bar_completion_confirmed is True
    assert len(exchange.ohlcv_calls) == 1
    assert client.cache.read_latest_summary(series.key).end == 6 * MINUTE


def test_final_cache_response_must_still_pass_interval_validation(temp_dir):
    client, exchange = _client(temp_dir)
    series = client._series(SYMBOL, "1m", "default")
    client.cache.write_segment(
        series.key,
        OhlcvResult([canonical_row(_row(t)) for t in _minutes(1, 3)], True),
        MINUTE,
    )
    with pytest.raises(NetworkIncomplete, match="not contiguous"):
        client.fetch_ohlcv_since_limit(SYMBOL, "1m", MINUTE, 2)
    assert exchange.ohlcv_calls == []


@pytest.mark.parametrize("mode", ["since-limit", "latest-limit"])
def test_monthly_is_one_page_without_disk_cache(temp_dir, mode):
    times = [1704067200000, 1706745600000, 1709251200000]
    client, exchange = _client(temp_dir, times=times)
    if mode == "since-limit":
        result = client.fetch_ohlcv_since_limit(SYMBOL, "1M", times[0], 2)
    else:
        result = client.fetch_ohlcv_latest_limit(SYMBOL, "1M", 2)
    assert len(result.rows) == 2
    assert len(exchange.ohlcv_calls) == 1
    assert (
        client.cache.read_latest_summary(
            client._series(SYMBOL, "1M", "default").key
        ).count
        == 0
    )
    with pytest.raises(CapabilityNotSupported, match="single provider page"):
        client.fetch_ohlcv_latest_limit(SYMBOL, "1M", 1001)
    with pytest.raises(CapabilityNotSupported, match="SinceLatest"):
        client.fetch_ohlcv_since_latest(SYMBOL, "1M", times[0])


def test_invalid_computed_start_is_rejected_without_history_probe(temp_dir):
    client, exchange = _client(temp_dir)
    with pytest.raises(InvalidProviderRequest, match="reduce limit"):
        client.fetch_ohlcv_latest_limit(SYMBOL, "1m", 20)
    assert len(exchange.ohlcv_calls) == 1


def kraken_adapter(monkeypatch):
    """运行真实 SDK 转换；底层官方 candles 响应完全离线。"""
    sdk = ccxt.krakenfutures()
    symbol = "BTC/USD:USD"
    sdk.markets = {symbol: {"id": "PF_XBTUSD", "symbol": symbol}}
    requests = []
    monkeypatch.setattr(sdk, "seconds", lambda: 10 * 60 + 30)

    def candles(params):
        requests.append(dict(params))
        return {
            "candles": [
                {
                    "time": i * MINUTE,
                    "open": "2",
                    "high": "4",
                    "low": "1",
                    "close": "3",
                    "volume": "10",
                }
                for i in range(3, 11)
                if params["from"] <= i * 60 <= params["to"]
            ]
        }

    monkeypatch.setattr(sdk, "chartsGetPriceTypeSymbolInterval", candles)
    monkeypatch.setattr(sdk, "fetch", lambda *a, **kw: pytest.fail("禁止联网"))
    return sdk, requests, symbol


def test_real_kraken_sdk_short_window_continues_from_actual_tail(temp_dir, monkeypatch):
    sdk, requests, symbol = kraken_adapter(monkeypatch)
    cache = _cache(temp_dir)
    client = CcxtClient(sdk, "kraken", "future", "live", cache)
    client._ohlcv.page_limit = 3
    try:
        result = client.fetch_ohlcv_latest_limit(symbol, "1m", 10)
        assert [r[0] for r in result.rows] == _minutes(*range(3, 11))
        assert result.last_bar_completion_confirmed is False
        # 首个历史窗口只有第 3 根，后面仍能沿含首窗口正常到达 S。
        assert [(r["from"], r["to"]) for r in requests[1:3]] == [(60, 239), (180, 359)]
    finally:
        client.close()
        cache.close()


def test_real_kraken_sdk_empty_initial_window_is_not_searched(temp_dir, monkeypatch):
    sdk, requests, symbol = kraken_adapter(monkeypatch)
    cache = _cache(temp_dir)
    client = CcxtClient(sdk, "kraken", "future", "live", cache)
    client._ohlcv.page_limit = 3
    try:
        with pytest.raises(NetworkIncomplete, match="snapshot was not reached"):
            client.fetch_ohlcv_since_latest(symbol, "1m", 0)
        assert len(requests) == 2
        assert (
            cache.read_latest_summary(client._series(symbol, "1m", "default").key).count
            == 0
        )
    finally:
        client.close()
        cache.close()
