from typing import Any

import ccxt
from loguru import logger

from src.cache_tool import (
    MAX_RESPONSE_ROWS,
    DuckDbOhlcvCache,
    OhlcvResult,
    OhlcvSeries,
)
from src.cache_tool.models import merge_rows
from src.domain_errors import (
    CacheCapacityExceeded,
    CapabilityNotSupported,
    InvalidProviderRequest,
    ResponseRowLimitExceeded,
)
from src.tools.ccxt_ohlcv import OhlcvNetworkFetcher
from src.tools.ccxt_trading import _CcxtTradingMixin
from src.tools.ccxt_transport import CcxtTransport


class CcxtClient(_CcxtTradingMixin):
    _VALID_OHLCV_VARIANTS = {"default", "mark", "index", "premiumIndex"}

    def __init__(
        self,
        exchange: Any,
        exchange_name: str,
        market: str,
        mode: str,
        cache: DuckDbOhlcvCache | None,
    ) -> None:
        self.exchange = exchange
        self.exchange_name = exchange_name
        self.market = market
        self.mode = mode
        self.cache = cache
        self._transport = CcxtTransport(exchange, f"{exchange_name}/{market}/{mode}")
        self.ccxt_request_lock = self._transport.lock
        self._ohlcv = OhlcvNetworkFetcher(exchange_name, market, self._fetch_ohlcv_page)

    def load_markets(self) -> None:
        self._transport.read_call("loadMarkets", self.exchange.load_markets)

    def close(self) -> None:
        self._transport.close()

    def fetch_ohlcv_since_limit(
        self,
        symbol: str,
        timeframe: str,
        since: int,
        limit: int,
        *,
        variant: str = "default",
        enable_cache: bool = True,
    ) -> OhlcvResult:
        self._validate_ohlcv(symbol, timeframe, variant, limit)
        series = self._series(symbol, timeframe, variant)
        prefix = self._read_prefix(series, since, limit) if enable_cache else []
        if len(prefix) >= limit:
            return OhlcvResult(prefix[:limit], True)

        network_since = prefix[-1][0] if prefix else since
        network_limit = limit - len(prefix) + (1 if prefix else 0)
        network = self._ohlcv.fetch_since_limit(
            symbol, timeframe, network_since, network_limit, variant
        )
        if prefix and not self._contains_time(network, network_since):
            logger.bind(series_key=series.key).warning(
                "cache prefix overlap failed; refetching the full query"
            )
            prefix = []
            network = self._ohlcv.fetch_since_limit(
                symbol, timeframe, since, limit, variant
            )
        rows = merge_rows(prefix, network.rows)[:limit]
        confirmed = network.last_bar_completion_confirmed if rows else None
        raw = OhlcvResult(rows, confirmed)
        self._write_cache(series, raw, since, enable_cache)
        return raw

    def fetch_ohlcv_since_latest(
        self,
        symbol: str,
        timeframe: str,
        since: int,
        *,
        variant: str = "default",
        enable_cache: bool = True,
    ) -> OhlcvResult:
        self._validate_ohlcv(symbol, timeframe, variant)
        if not self._ohlcv.supports_full_history:
            raise CapabilityNotSupported(
                f"{self.exchange_name}/{self.market} does not support SinceLatest"
            )
        snapshot = self._ohlcv.fetch_latest_anchor(symbol, timeframe, variant)
        if snapshot is None or since > snapshot:
            return OhlcvResult([], None)

        series = self._series(symbol, timeframe, variant)
        prefix = (
            self._read_prefix(series, since, MAX_RESPONSE_ROWS + 1)
            if enable_cache
            else []
        )
        prefix = [row for row in prefix if row[0] <= snapshot]
        if len(prefix) > MAX_RESPONSE_ROWS:
            raise ResponseRowLimitExceeded()
        if prefix and prefix[-1][0] == snapshot:
            return OhlcvResult(prefix, True)

        network_since = prefix[-1][0] if prefix else since
        network_budget = MAX_RESPONSE_ROWS - len(prefix) + (1 if prefix else 0)
        if network_budget <= 0:
            raise ResponseRowLimitExceeded()
        network = self._ohlcv.fetch_to_snapshot(
            symbol, timeframe, network_since, snapshot, variant, network_budget
        )
        if prefix and not self._contains_time(network, network_since):
            logger.bind(series_key=series.key).warning(
                "cache prefix overlap failed; refetching the full query"
            )
            prefix = []
            network = self._ohlcv.fetch_to_snapshot(
                symbol, timeframe, since, snapshot, variant, MAX_RESPONSE_ROWS
            )
        rows = merge_rows(prefix, network.rows)
        rows = [row for row in rows if since <= row[0] <= snapshot]
        if len(rows) > MAX_RESPONSE_ROWS:
            raise ResponseRowLimitExceeded()
        raw = OhlcvResult(rows, network.last_bar_completion_confirmed if rows else None)
        self._write_cache(series, raw, since, enable_cache)
        return raw

    def fetch_ohlcv_latest_limit(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        *,
        variant: str = "default",
        enable_cache: bool = True,
    ) -> OhlcvResult:
        self._validate_ohlcv(symbol, timeframe, variant, limit)
        raw = self._ohlcv.fetch_latest_limit(symbol, timeframe, limit, variant)
        self._write_cache(
            self._series(symbol, timeframe, variant), raw, None, enable_cache
        )
        return raw

    def _fetch_ohlcv_page(self, *args: Any, **kwargs: Any):
        return self._read_method("fetchOHLCV", "fetch_ohlcv", *args, **kwargs)

    def _read_method(self, capability: str, method: str, *args: Any, **kwargs: Any):
        return self._transport.read_method(capability, method, *args, **kwargs)

    def _write_method(self, capability: str, method: str, *args: Any, **kwargs: Any):
        return self._transport.write_method(capability, method, *args, **kwargs)

    def _validate_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        variant: str,
        limit: int | None = None,
    ) -> None:
        self._transport.require("fetchOHLCV")
        if limit is not None and not 1 <= limit <= MAX_RESPONSE_ROWS:
            raise ResponseRowLimitExceeded()
        if variant not in self._VALID_OHLCV_VARIANTS:
            raise InvalidProviderRequest(f"unsupported OHLCV variant: {variant}")
        if timeframe not in (getattr(self.exchange, "timeframes", None) or {}):
            raise CapabilityNotSupported(
                f"{self.exchange_name}/{self.market} does not support {timeframe} OHLCV"
            )
        self._resolve_market(symbol)
        if variant != "default" and not (
            self.exchange_name == "binance" and self.market == "future"
        ):
            raise CapabilityNotSupported(
                f"{self.exchange_name}/{self.market} does not support {variant} OHLCV"
            )

    def _resolve_market(self, symbol: str) -> dict[str, Any]:
        try:
            market = self.exchange.market(symbol)
        except ccxt.BadSymbol as exc:
            if self.exchange_name == "binance" and self.market == "future":
                raise CapabilityNotSupported(
                    f"binance/future symbol is outside the linear market scope: {symbol}"
                ) from exc
            raise InvalidProviderRequest(f"unknown provider symbol: {symbol}") from exc
        if (
            self.exchange_name == "binance"
            and self.market == "future"
            and not market.get("linear")
        ):
            raise CapabilityNotSupported(
                f"binance/future supports linear markets only: {symbol}"
            )
        return market

    def _validate_symbol(self, symbol: str | None) -> None:
        if symbol is not None:
            if not isinstance(symbol, str) or not symbol.strip():
                raise InvalidProviderRequest("symbol must not be empty")
            self._resolve_market(symbol)

    def _validate_symbols(self, symbols: list[str] | None) -> None:
        for symbol in symbols or []:
            self._validate_symbol(symbol)

    def _series(self, symbol: str, timeframe: str, variant: str) -> OhlcvSeries:
        return OhlcvSeries(
            self.exchange_name, self.mode, self.market, symbol, timeframe, variant
        )

    def _read_prefix(self, series: OhlcvSeries, since: int, limit: int):
        if self.cache is None or not self._ohlcv.supports_full_history:
            return []
        try:
            return self.cache.read_best_prefix(series.key, since, limit)
        except Exception:
            logger.bind(series_key=series.key).warning(
                "cache read failed; continuing as cache miss"
            )
            return []

    def _write_cache(
        self,
        series: OhlcvSeries,
        result: OhlcvResult,
        covered_from: int | None,
        enabled: bool,
    ) -> None:
        if not enabled or self.cache is None or not self._ohlcv.supports_full_history:
            return
        try:
            self.cache.write_segment(series.key, result, covered_from)
        except CacheCapacityExceeded:
            raise
        except Exception:
            logger.bind(series_key=series.key).exception(
                "cache write failed; returning network response"
            )

    @staticmethod
    def _contains_time(result: OhlcvResult, timestamp: int) -> bool:
        return any(row[0] == timestamp for row in result.rows)
