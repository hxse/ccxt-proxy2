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
from src.tools.ccxt_ohlcv import (
    MissingPrefixAnchor,
    OhlcvNetworkFetcher,
    clip_snapshot,
    fixed_interval_ms,
)
from src.tools.ccxt_prices import CcxtOrderPrices
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
        self._order_prices = CcxtOrderPrices(
            exchange, exchange_name, market, self._transport
        )
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
        if not self._cacheable_timeframe(timeframe):
            return self._thin_ohlcv(symbol, timeframe, since, limit, variant)
        return self._query_ohlcv(symbol, timeframe, since, limit, variant, enable_cache)

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
        if not self._cacheable_timeframe(timeframe):
            raise CapabilityNotSupported(
                f"{self.exchange_name}/{self.market}/{timeframe} does not support SinceLatest"
            )
        snapshot = self._ohlcv.fetch_latest_anchor(symbol, timeframe, variant)
        if snapshot is None or since > snapshot:
            return OhlcvResult([], None)
        return self._query_ohlcv(
            symbol, timeframe, since, MAX_RESPONSE_ROWS, variant, enable_cache, snapshot
        )

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
        if not self._cacheable_timeframe(timeframe):
            return self._thin_ohlcv(symbol, timeframe, None, limit, variant)
        snapshot = self._ohlcv.fetch_latest_anchor(symbol, timeframe, variant)
        if snapshot is None:
            return OhlcvResult([], None)
        duration = fixed_interval_ms(timeframe)
        assert duration is not None
        since = snapshot - (limit - 1) * duration
        if since < 0:
            raise InvalidProviderRequest("derived start is invalid; reduce limit")
        return self._query_ohlcv(
            symbol, timeframe, since, limit, variant, enable_cache, snapshot
        )

    def _cacheable_timeframe(self, timeframe: str) -> bool:
        return (
            self._ohlcv.supports_full_history
            and fixed_interval_ms(timeframe) is not None
        )

    def _thin_ohlcv(
        self, symbol: str, timeframe: str, since: int | None, limit: int, variant: str
    ) -> OhlcvResult:
        if self._ohlcv.supports_full_history and limit > self._ohlcv.page_limit:
            raise CapabilityNotSupported(
                "timeframes above one week support a single provider page only"
            )
        return self._ohlcv.fetch_single(symbol, timeframe, since, limit, variant)

    def _query_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: int,
        count: int,
        variant: str,
        enable_cache: bool,
        snapshot: int | None = None,
    ) -> OhlcvResult:
        series = self._series(symbol, timeframe, variant)
        read_budget = count + (snapshot is not None)
        prefix = self._read_prefix(series, since, read_budget) if enable_cache else []
        prefix = clip_snapshot(prefix, snapshot)
        if len(prefix) > count:
            raise ResponseRowLimitExceeded()
        if prefix and (
            (snapshot is None and len(prefix) == count)
            or (snapshot is not None and prefix[-1][0] == snapshot)
        ):
            self._ohlcv.validate_result(prefix, timeframe, since, count, snapshot)
            return OhlcvResult(prefix, True)

        def fetch(cursor: int, budget: int) -> OhlcvResult:
            if snapshot is None:
                return self._ohlcv.fetch_since_limit(
                    symbol,
                    timeframe,
                    cursor,
                    budget,
                    variant,
                    require_initial_anchor=bool(prefix),
                )
            return self._ohlcv.fetch_to_snapshot(
                symbol,
                timeframe,
                cursor,
                snapshot,
                variant,
                budget,
                require_initial_anchor=bool(prefix),
            )

        cursor = prefix[-1][0] if prefix else since
        budget = count - len(prefix) + bool(prefix)
        if budget <= 0:
            raise ResponseRowLimitExceeded()
        try:
            network = fetch(cursor, budget)
        except MissingPrefixAnchor:
            logger.bind(series_key=series.key).warning(
                "cache prefix overlap failed; refetching the full query"
            )
            prefix = []
            network = fetch(since, count)
        rows = clip_snapshot(merge_rows(prefix, network.rows), snapshot)
        self._ohlcv.validate_result(rows, timeframe, since, count, snapshot)
        result = OhlcvResult(rows, False if rows else None)
        self._write_cache(series, result, since, enable_cache)
        return result

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
