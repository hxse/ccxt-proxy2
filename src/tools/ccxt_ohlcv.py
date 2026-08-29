from collections.abc import Callable
from typing import Any

from src.cache_tool.models import (
    OhlcvResult,
    OhlcvRow,
    canonical_rows,
    merge_rows,
)
from src.domain_errors import (
    InvalidProviderData,
    NetworkIncomplete,
    ResponseRowLimitExceeded,
)

PageCall = Callable[..., list[list[Any]]]


class OhlcvNetworkFetcher:
    def __init__(self, provider: str, market: str, page_call: PageCall) -> None:
        self.provider = provider
        self.market = market
        self._page_call = page_call
        self.page_limit = 1_000

    @property
    def supports_full_history(self) -> bool:
        return self.provider == "binance" or (
            self.provider == "kraken" and self.market == "future"
        )

    def fetch_since_limit(
        self,
        symbol: str,
        timeframe: str,
        since: int,
        limit: int,
        variant: str,
    ) -> OhlcvResult:
        if not self.supports_full_history:
            rows = self._page(symbol, timeframe, since, limit, variant, {})
            rows = [row for row in rows if row[0] >= since][:limit]
            return OhlcvResult(rows, False if rows else None)
        rows = self._fetch_forward(symbol, timeframe, since, limit + 1, variant)
        user_rows = rows[:limit]
        confirmed = len(rows) > len(user_rows)
        return OhlcvResult(user_rows, confirmed if user_rows else None)

    def fetch_latest_limit(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        variant: str,
    ) -> OhlcvResult:
        if not self.supports_full_history:
            rows = self._page(symbol, timeframe, None, limit, variant, {})
            rows = rows[-limit:]
            return OhlcvResult(rows, False if rows else None)

        rows: list[OhlcvRow] = []
        cursor: int | None = None
        while len(rows) < limit:
            overlap = 0 if cursor is None else 1
            request_limit = min(self.page_limit, limit - len(rows) + overlap)
            request_since, params = self._backward_request(
                cursor, timeframe, request_limit
            )
            page = self._page(
                symbol, timeframe, request_since, request_limit, variant, params
            )
            page_size = len(page)
            if cursor is not None:
                page = [row for row in page if row[0] <= cursor]
                self._require_anchor(page, cursor, "backward page")
            if not page:
                break
            previous = len(rows)
            rows = merge_rows(rows, page)
            if len(rows) == previous:
                if page_size >= request_limit:
                    raise NetworkIncomplete("backward page made no progress")
                break
            if page_size < request_limit:
                break
            cursor = rows[0][0]
        user_rows = rows[-limit:]
        return OhlcvResult(user_rows, False if user_rows else None)

    def fetch_latest_anchor(
        self, symbol: str, timeframe: str, variant: str
    ) -> int | None:
        rows = self._page(symbol, timeframe, None, 1, variant, {})
        return rows[-1][0] if rows else None

    def fetch_to_snapshot(
        self,
        symbol: str,
        timeframe: str,
        since: int,
        snapshot: int,
        variant: str,
        max_rows: int,
    ) -> OhlcvResult:
        if since > snapshot:
            return OhlcvResult([], None)
        rows: list[OhlcvRow] = []
        cursor = since
        first_page = True
        while snapshot not in {row[0] for row in rows}:
            request_limit = self.page_limit
            page = self._page(symbol, timeframe, cursor, request_limit, variant, {})
            if not first_page:
                self._require_anchor(page, cursor, "forward page")
            bounded = [row for row in page if since <= row[0] <= snapshot]
            previous = len(rows)
            rows = merge_rows(rows, bounded)
            if len(rows) > max_rows:
                raise ResponseRowLimitExceeded(
                    f"OHLCV response exceeds {max_rows} rows"
                )
            if snapshot in {row[0] for row in rows}:
                break
            if not page or len(rows) == previous or len(page) < request_limit:
                raise NetworkIncomplete("latest snapshot was not reached")
            cursor = rows[-1][0]
            first_page = False

        proof = self.fetch_since_limit(symbol, timeframe, snapshot, 1, variant)
        self._require_anchor(proof.rows, snapshot, "snapshot lookahead")
        rows = merge_rows(rows, proof.rows)
        rows = [row for row in rows if row[0] <= snapshot]
        return OhlcvResult(rows, proof.last_bar_completion_confirmed)

    def _fetch_forward(
        self,
        symbol: str,
        timeframe: str,
        since: int,
        count: int,
        variant: str,
    ) -> list[OhlcvRow]:
        rows: list[OhlcvRow] = []
        cursor = since
        first_page = True
        while len(rows) < count:
            overlap = 0 if first_page else 1
            request_limit = min(self.page_limit, count - len(rows) + overlap)
            page = self._page(symbol, timeframe, cursor, request_limit, variant, {})
            if not first_page:
                self._require_anchor(page, cursor, "forward page")
            page = [row for row in page if row[0] >= since]
            if not page:
                break
            previous = len(rows)
            rows = merge_rows(rows, page)
            if len(rows) == previous:
                if len(page) >= request_limit:
                    raise NetworkIncomplete("forward page made no progress")
                break
            if len(page) < request_limit:
                break
            cursor = rows[-1][0]
            first_page = False
        return rows[:count]

    def _page(
        self,
        symbol: str,
        timeframe: str,
        since: int | None,
        limit: int,
        variant: str,
        params: dict[str, Any],
    ) -> list[OhlcvRow]:
        request_params = dict(params)
        if variant != "default":
            request_params["price"] = variant
        try:
            raw = self._page_call(
                symbol,
                timeframe,
                since=since,
                limit=limit,
                params=request_params,
            )
            rows = canonical_rows(raw or [])
            self._validate_fixed_interval_page(rows, timeframe)
            return rows
        except (TypeError, ValueError, OverflowError) as exc:
            raise InvalidProviderData(str(exc)) from exc

    def _validate_fixed_interval_page(
        self, rows: list[OhlcvRow], timeframe: str
    ) -> None:
        if not self.supports_full_history or len(rows) < 2:
            return
        if timeframe.endswith("M"):
            return
        duration_ms = _timeframe_seconds(timeframe) * 1_000
        for previous, current in zip(rows, rows[1:]):
            if current[0] - previous[0] != duration_ms:
                raise NetworkIncomplete(
                    f"{self.provider} {self.market} OHLCV page is not contiguous "
                    f"for {timeframe}: {previous[0]} -> {current[0]}"
                )

    def _backward_request(
        self, cursor: int | None, timeframe: str, limit: int
    ) -> tuple[int | None, dict[str, int]]:
        if cursor is None:
            return None, {}
        if self.provider == "binance":
            return None, {"until": cursor}
        if self.provider == "kraken" and self.market == "future":
            duration_ms = _timeframe_seconds(timeframe) * 1_000
            start = max(0, cursor - duration_ms * (limit - 1))
            return start, {"to": cursor // 1_000}
        return None, {}

    def _require_anchor(
        self, rows: list[OhlcvRow], anchor: int, operation: str
    ) -> None:
        if not any(row[0] == anchor for row in rows):
            raise NetworkIncomplete(f"{operation} did not include overlap anchor")


def _timeframe_seconds(timeframe: str) -> int:
    unit_seconds = {
        "m": 60,
        "h": 3_600,
        "d": 86_400,
        "w": 604_800,
        "M": 2_592_000,
    }
    try:
        return int(timeframe[:-1]) * unit_seconds[timeframe[-1]]
    except (KeyError, ValueError) as exc:
        raise InvalidProviderData(f"unsupported timeframe: {timeframe}") from exc
