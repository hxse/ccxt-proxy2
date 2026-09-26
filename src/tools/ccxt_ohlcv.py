"""CCXT 唯一的 since + limit 获取链；网络阶段不读项目缓存。"""

import operator
from collections.abc import Callable
from typing import Any

from src.cache_tool.models import (
    OhlcvResult,
    OhlcvRow,
    canonical_row,
    canonical_rows,
    merge_rows,
)
from src.domain_errors import (
    InvalidProviderData,
    NetworkIncomplete,
    ResponseRowLimitExceeded,
)

PageCall = Callable[..., list[list[Any]]]


class MissingPrefixAnchor(NetworkIncomplete):
    """首个网络页未接上缓存前缀，由 Client 完整回退一次。"""


def clip_snapshot(rows, snapshot: int | None):
    """先验证时间并裁边界，S 之后的价格不参与本次查询校验。"""
    if snapshot is None:
        return rows
    selected = []
    for row in rows:
        try:
            timestamp = operator.index(row[0])
            if isinstance(row[0], bool) or timestamp < 0:
                raise ValueError("invalid OHLCV timestamp")
        except (IndexError, TypeError, ValueError) as exc:
            raise InvalidProviderData("invalid OHLCV timestamp") from exc
        if timestamp <= snapshot:
            selected.append(row)
    return selected


def fixed_interval_ms(timeframe: str) -> int | None:
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    try:
        seconds = int(timeframe[:-1]) * units[timeframe[-1]]
    except (KeyError, ValueError, IndexError):
        return None
    return seconds * 1000 if 0 < seconds <= 604800 else None


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

    def fetch_single(
        self, symbol: str, timeframe: str, since: int | None, limit: int, variant: str
    ) -> OhlcvResult:
        rows = self._page(symbol, timeframe, since, limit, variant)
        rows = [row for row in rows if since is None or row[0] >= since]
        rows = rows[-limit:] if since is None else rows[:limit]
        self.validate_result(rows, timeframe, since, limit)
        return OhlcvResult(rows, False if rows else None)

    def fetch_since_limit(
        self,
        symbol: str,
        timeframe: str,
        since: int,
        limit: int,
        variant: str,
        *,
        require_initial_anchor: bool = False,
    ) -> OhlcvResult:
        rows = self._fetch_forward(
            symbol,
            timeframe,
            since,
            limit,
            variant,
            require_initial_anchor=require_initial_anchor,
        )
        return OhlcvResult(rows, False if rows else None)

    def fetch_latest_anchor(
        self, symbol: str, timeframe: str, variant: str
    ) -> int | None:
        rows = self._page(symbol, timeframe, None, 1, variant)
        return rows[-1][0] if rows else None

    def fetch_to_snapshot(
        self,
        symbol: str,
        timeframe: str,
        since: int,
        snapshot: int,
        variant: str,
        max_rows: int,
        *,
        require_initial_anchor: bool = False,
    ) -> OhlcvResult:
        if since > snapshot:
            return OhlcvResult([], None)
        rows = self._fetch_forward(
            symbol,
            timeframe,
            since,
            max_rows,
            variant,
            snapshot,
            require_initial_anchor=require_initial_anchor,
        )
        return OhlcvResult(rows, False if rows else None)

    def _fetch_forward(
        self,
        symbol: str,
        timeframe: str,
        since: int,
        count: int,
        variant: str,
        snapshot: int | None = None,
        *,
        require_initial_anchor: bool = False,
    ) -> list[OhlcvRow]:
        rows: list[OhlcvRow] = []
        cursor = since
        first_page = True
        while True:
            # 固定快照多留一个溢出检测位置，重叠点另计；不是尾根完成性探测。
            budget = (
                count - len(rows) + (0 if first_page else 1) + (snapshot is not None)
            )
            request_limit = min(self.page_limit, budget)
            page = self._page(
                symbol, timeframe, cursor, request_limit, variant, snapshot
            )
            if (
                first_page
                and require_initial_anchor
                and not any(row[0] == cursor for row in page)
            ):
                raise MissingPrefixAnchor(
                    "network page did not include cache prefix anchor"
                )
            if not first_page:
                self._require_anchor(page, cursor)
            bounded = [row for row in page if row[0] >= since]
            previous = len(rows)
            rows = merge_rows(rows, bounded)
            if len(rows) > count:
                raise ResponseRowLimitExceeded(f"OHLCV response exceeds {count} rows")
            if snapshot is not None and rows and rows[-1][0] == snapshot:
                break
            if snapshot is None and len(rows) >= count:
                break
            if not page or len(rows) == previous:
                if snapshot is not None:
                    raise NetworkIncomplete(
                        "latest snapshot was not reached; adjust since or limit"
                    )
                if page and len(page) >= request_limit:
                    raise NetworkIncomplete("forward page made no progress")
                break
            cursor = rows[-1][0]
            first_page = False
        self.validate_result(rows, timeframe, since, count, snapshot)
        return rows

    def _page(
        self,
        symbol: str,
        timeframe: str,
        since: int | None,
        limit: int,
        variant: str,
        snapshot: int | None = None,
    ) -> list[OhlcvRow]:
        params = {} if variant == "default" else {"price": variant}
        try:
            raw = self._page_call(
                symbol, timeframe, since=since, limit=limit, params=params
            )
            rows = canonical_rows(clip_snapshot(raw or [], snapshot))
            self._validate_fixed_interval_page(rows, timeframe)
            return rows
        except (TypeError, ValueError, OverflowError) as exc:
            raise InvalidProviderData(str(exc)) from exc

    def validate_result(
        self,
        rows: list[OhlcvRow],
        timeframe: str,
        since: int | None,
        max_rows: int,
        snapshot: int | None = None,
    ) -> None:
        try:
            for row in rows:
                canonical_row(row)
        except (TypeError, ValueError, OverflowError) as exc:
            raise InvalidProviderData(str(exc)) from exc
        if any(current[0] <= previous[0] for previous, current in zip(rows, rows[1:])):
            raise InvalidProviderData("OHLCV timestamps must be unique and increasing")
        if rows and since is not None and rows[0][0] < since:
            raise InvalidProviderData("OHLCV result precedes since")
        if len(rows) > max_rows:
            raise ResponseRowLimitExceeded()
        self._validate_fixed_interval_page(rows, timeframe)
        if snapshot is not None and (not rows or rows[-1][0] != snapshot):
            raise NetworkIncomplete("latest snapshot was not reached")

    def _validate_fixed_interval_page(
        self, rows: list[OhlcvRow], timeframe: str
    ) -> None:
        duration_ms = fixed_interval_ms(timeframe)
        if not self.supports_full_history or duration_ms is None:
            return
        for previous, current in zip(rows, rows[1:]):
            if current[0] - previous[0] != duration_ms:
                raise NetworkIncomplete(
                    f"{self.provider} {self.market} OHLCV page is not contiguous for {timeframe}: {previous[0]} -> {current[0]}"
                )

    @staticmethod
    def _require_anchor(rows: list[OhlcvRow], anchor: int) -> None:
        if not any(row[0] == anchor for row in rows):
            raise NetworkIncomplete("forward page did not include overlap anchor")
