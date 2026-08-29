from typing import Any, Callable

import pytest

from src.domain_errors import InvalidProviderData, NetworkIncomplete
from src.tools.ccxt_ohlcv import OhlcvNetworkFetcher

MINUTE = 60_000


def _row(timestamp: int, close: float | None = None) -> list[float]:
    value = float(timestamp)
    return [timestamp, value, value + 2, value - 2, close or value + 1, 10]


def _minutes(*values: int) -> list[int]:
    return [value * MINUTE for value in values]


class PageSource:
    def __init__(
        self,
        times: list[int],
        hook: Callable[["PageSource"], None] | None = None,
    ) -> None:
        self.rows = [_row(timestamp) for timestamp in times]
        self.calls: list[dict[str, Any]] = []
        self.hook = hook

    def __call__(
        self, symbol, timeframe, *, since=None, limit: int = 1000, params=None
    ) -> list[list[float]]:
        self.calls.append({"since": since, "limit": limit, "params": params or {}})
        if self.hook:
            self.hook(self)
        candidates = self.rows
        if since is not None:
            candidates = [row for row in candidates if row[0] >= since]
            return candidates[:limit]
        cutoff = (params or {}).get("until")
        request_params = params or {}
        if cutoff is None and "to" in request_params:
            cutoff = request_params["to"] * 1_000
        if cutoff is not None:
            candidates = [row for row in candidates if row[0] <= cutoff]
        return candidates[-limit:]


def test_since_limit_uses_inclusive_overlap_and_successor():
    source = PageSource(_minutes(1, 2, 3, 4, 5, 6))
    fetcher = OhlcvNetworkFetcher("binance", "future", source)
    fetcher.page_limit = 3

    result = fetcher.fetch_since_limit("BTC/USDT", "1m", MINUTE, 4, "default")

    assert [row[0] for row in result.rows] == _minutes(1, 2, 3, 4)
    assert result.last_bar_completion_confirmed is True
    assert source.calls[1]["since"] == 3 * MINUTE


def test_since_limit_returns_tail_but_marks_it_unknown_without_successor():
    source = PageSource(_minutes(1, 2, 3, 4))
    fetcher = OhlcvNetworkFetcher("binance", "future", source)
    fetcher.page_limit = 3

    result = fetcher.fetch_since_limit("BTC/USDT", "1m", MINUTE, 4, "default")

    assert [row[0] for row in result.rows] == _minutes(1, 2, 3, 4)
    assert result.last_bar_completion_confirmed is False


def test_since_latest_freezes_snapshot_and_uses_later_row_only_as_proof():
    def append_successor_on_lookahead(source: PageSource) -> None:
        if len(source.calls) == 3:
            source.rows.append(_row(4 * MINUTE))

    source = PageSource(_minutes(1, 2, 3), append_successor_on_lookahead)
    fetcher = OhlcvNetworkFetcher("binance", "future", source)

    snapshot = fetcher.fetch_latest_anchor("BTC/USDT", "1m", "default")
    assert snapshot == 3 * MINUTE
    result = fetcher.fetch_to_snapshot(
        "BTC/USDT", "1m", MINUTE, snapshot, "default", 100_000
    )

    assert [row[0] for row in result.rows] == _minutes(1, 2, 3)
    assert result.last_bar_completion_confirmed is True


def test_latest_limit_pages_backwards_from_real_latest():
    source = PageSource(_minutes(*range(1, 9)))
    fetcher = OhlcvNetworkFetcher("binance", "future", source)
    fetcher.page_limit = 3

    result = fetcher.fetch_latest_limit("BTC/USDT", "1m", 5, "default")

    assert [row[0] for row in result.rows] == _minutes(4, 5, 6, 7, 8)
    assert result.last_bar_completion_confirmed is False
    assert source.calls[1]["params"] == {"until": 6 * MINUTE}


def test_kraken_futures_backward_page_uses_explicit_from_and_to_window():
    source = PageSource([60_000 * value for value in range(1, 9)])
    fetcher = OhlcvNetworkFetcher("kraken", "future", source)
    fetcher.page_limit = 3

    result = fetcher.fetch_latest_limit("BTC/USD:USD", "1m", 5, "default")

    assert [row[0] for row in result.rows] == [
        240_000,
        300_000,
        360_000,
        420_000,
        480_000,
    ]
    assert source.calls[1]["since"] == 240_000
    assert source.calls[1]["params"] == {"to": 360}


def test_forward_page_must_repeat_overlap_anchor():
    source = PageSource(_minutes(1, 2, 3, 4, 5))

    def missing_anchor(
        symbol, timeframe, *, since=None, limit: int = 1000, params=None
    ):
        rows = source(symbol, timeframe, since=since, limit=limit, params=params)
        return (
            rows if len(source.calls) == 1 else [row for row in rows if row[0] != since]
        )

    fetcher = OhlcvNetworkFetcher("binance", "future", missing_anchor)
    fetcher.page_limit = 3

    with pytest.raises(NetworkIncomplete):
        fetcher.fetch_since_limit("BTC/USDT", "1m", MINUTE, 4, "default")


def test_full_forward_page_without_new_rows_is_network_incomplete():
    rows = [_row(timestamp) for timestamp in _minutes(1, 2, 3, 4, 5)]

    def ignore_since(*args, limit: int = 1000, **kwargs):
        return rows[:limit]

    fetcher = OhlcvNetworkFetcher("binance", "future", ignore_since)
    fetcher.page_limit = 3

    with pytest.raises(NetworkIncomplete, match="forward page made no progress"):
        fetcher.fetch_since_limit("BTC/USDT", "1m", MINUTE, 4, "default")


def test_full_backward_page_without_new_rows_is_network_incomplete():
    rows = [_row(timestamp) for timestamp in _minutes(1, 2, 3, 4, 5, 6, 7, 8)]

    def ignore_until(*args, limit: int = 1000, **kwargs):
        return rows[-limit:]

    fetcher = OhlcvNetworkFetcher("binance", "future", ignore_until)
    fetcher.page_limit = 3

    with pytest.raises(NetworkIncomplete, match="backward page made no progress"):
        fetcher.fetch_latest_limit("BTC/USDT", "1m", 5, "default")


def test_short_anchor_only_page_is_a_valid_history_boundary():
    source = PageSource(_minutes(1, 2, 3))
    fetcher = OhlcvNetworkFetcher("binance", "future", source)
    fetcher.page_limit = 3

    result = fetcher.fetch_since_limit("BTC/USDT", "1m", MINUTE, 5, "default")

    assert [row[0] for row in result.rows] == _minutes(1, 2, 3)
    assert result.last_bar_completion_confirmed is False


@pytest.mark.parametrize(
    ("provider", "market"), [("binance", "future"), ("kraken", "future")]
)
def test_full_history_provider_rejects_non_contiguous_fixed_interval_page(
    provider: str, market: str
):
    source = PageSource(_minutes(1, 2, 4, 5))
    fetcher = OhlcvNetworkFetcher(provider, market, source)

    with pytest.raises(NetworkIncomplete, match="OHLCV page is not contiguous"):
        fetcher.fetch_since_limit("BTC/USD", "1m", MINUTE, 3, "default")


def test_binance_calendar_month_does_not_use_fixed_millisecond_continuity_check():
    january = 1_704_067_200_000
    february = 1_706_745_600_000
    march = 1_709_251_200_000
    source = PageSource([january, february, march])
    fetcher = OhlcvNetworkFetcher("binance", "future", source)

    result = fetcher.fetch_since_limit(
        "BTC/USD:USD", "1M", january, 2, "default"
    )

    assert [row[0] for row in result.rows] == [january, february]
    assert result.last_bar_completion_confirmed is True


def test_binance_calendar_month_latest_pages_with_until_anchor():
    months = [
        1_704_067_200_000,
        1_706_745_600_000,
        1_709_251_200_000,
        1_711_929_600_000,
    ]
    source = PageSource(months)
    fetcher = OhlcvNetworkFetcher("binance", "future", source)
    fetcher.page_limit = 3

    result = fetcher.fetch_latest_limit("BTC/USDT:USDT", "1M", 4, "default")

    assert [row[0] for row in result.rows] == months
    assert source.calls[1]["since"] is None
    assert source.calls[1]["params"] == {"until": months[1]}


def test_invalid_provider_row_fails_the_whole_network_operation():
    def invalid_page(*args, **kwargs):
        return [[10, 1.0, float("nan"), 0.0, 1.0, 1.0]]

    fetcher = OhlcvNetworkFetcher("binance", "future", invalid_page)

    with pytest.raises(InvalidProviderData):
        fetcher.fetch_since_limit("BTC/USDT", "1m", 10, 1, "default")


def test_kraken_spot_is_one_page_thin_forward_without_completion_claim():
    source = PageSource([10, 20, 30])
    fetcher = OhlcvNetworkFetcher("kraken", "spot", source)

    result = fetcher.fetch_since_limit("BTC/USD", "1m", 10, 2, "default")

    assert [row[0] for row in result.rows] == [10, 20]
    assert result.last_bar_completion_confirmed is False
    assert len(source.calls) == 1
