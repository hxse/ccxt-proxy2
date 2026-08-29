import math
from typing import Any

import ccxt

from src.domain_errors import InvalidProviderData, InvalidProviderRequest


class _CcxtTradingMixin:
    exchange: Any
    exchange_name: str
    market: str

    def fetch_tickers(self, symbols: list[str] | None = None):
        self._validate_symbols(symbols)
        return self._read_method("fetchTickers", "fetch_tickers", symbols, params={})

    def fetch_balance(self):
        return self._read_method("fetchBalance", "fetch_balance", params={})

    def fetch_market_info(self, symbol: str) -> dict[str, Any]:
        market = self._resolve_market(symbol)
        minimum = market["limits"]["amount"]["min"]
        if minimum is None:
            minimum = market["precision"]["amount"]
        leverage: int | None = None
        if self.exchange.has.get("fetchPositions"):
            positions = self.fetch_positions([symbol])
            if positions:
                value = positions[0].get("leverage")
                if value is not None:
                    try:
                        leverage = int(value)
                    except (TypeError, ValueError, OverflowError) as exc:
                        raise InvalidProviderData(
                            "provider returned an invalid leverage value"
                        ) from exc
        return {
            "symbol": symbol,
            "linear": bool(market.get("linear", False)),
            "settle": market["settle"],
            "precision_amount": float(market["precision"]["amount"]),
            "min_amount": float(minimum),
            "contract_size": float(market["contractSize"]),
            "leverage": leverage,
        }

    def create_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        amount: float,
        price: float | None = None,
        params: dict[str, Any] | None = None,
    ):
        self._validate_symbol(symbol)
        self._require_positive_finite(amount, "amount")
        if side not in {"buy", "sell"}:
            raise InvalidProviderRequest("side must be buy or sell")
        if price is not None:
            self._require_positive_finite(price, "price")
        return self._write_method(
            "createOrder",
            "create_order",
            symbol,
            order_type,
            side,
            amount,
            price,
            params=params or {},
        )

    def create_stop_market_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        trigger_price: float,
        *,
        reduce_only: bool,
        client_order_id: str | None,
        time_in_force: str | None,
        params: dict[str, Any] | None,
    ):
        return self._create_trigger_order(
            symbol,
            side,
            amount,
            trigger_price,
            trigger_field="stopLossPrice",
            reduce_only=reduce_only,
            client_order_id=client_order_id,
            time_in_force=time_in_force,
            params=params,
        )

    def create_take_profit_market_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        trigger_price: float,
        *,
        reduce_only: bool,
        client_order_id: str | None,
        time_in_force: str | None,
        params: dict[str, Any] | None,
    ):
        return self._create_trigger_order(
            symbol,
            side,
            amount,
            trigger_price,
            trigger_field="takeProfitPrice",
            reduce_only=reduce_only,
            client_order_id=client_order_id,
            time_in_force=time_in_force,
            params=params,
        )

    def _create_trigger_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        trigger_price: float,
        *,
        trigger_field: str,
        reduce_only: bool,
        client_order_id: str | None,
        time_in_force: str | None,
        params: dict[str, Any] | None,
    ):
        self._require_positive_finite(trigger_price, "triggerPrice")
        extra = dict(params or {})
        extra["reduceOnly"] = reduce_only
        extra[trigger_field] = trigger_price
        if client_order_id:
            extra["clientOrderId"] = client_order_id
        if time_in_force:
            extra["timeInForce"] = time_in_force
        return self.create_order(symbol, "market", side, amount, params=extra)

    def close_position(
        self,
        symbol: str,
        side: str | None = None,
        params: dict[str, Any] | None = None,
    ):
        positions = self.fetch_positions([symbol])
        if side:
            positions = [
                position for position in positions if position.get("side") == side
            ]
        close_params = {**(params or {}), "reduceOnly": True}
        for position in positions:
            try:
                amount = float(position.get("contracts") or 0)
            except (TypeError, ValueError, OverflowError) as exc:
                raise InvalidProviderData(
                    "provider returned invalid position contracts"
                ) from exc
            if not math.isfinite(amount):
                raise InvalidProviderData(
                    "provider returned invalid position contracts"
                )
            if amount <= 0:
                continue
            position_side = position.get("side")
            if position_side not in {"long", "short"}:
                raise InvalidProviderData(
                    "provider returned a nonzero position without a valid side"
                )
            close_side = "sell" if position_side == "long" else "buy"
            self.create_order(symbol, "market", close_side, amount, params=close_params)
        return self.fetch_positions([symbol])

    def fetch_open_orders(
        self, symbol: str | None, since: int | None, limit: int | None
    ):
        return self._fetch_order_list(
            "fetchOpenOrders", "fetch_open_orders", symbol, since, limit
        )

    def fetch_closed_orders(
        self, symbol: str | None, since: int | None, limit: int | None
    ):
        return self._fetch_order_list(
            "fetchClosedOrders", "fetch_closed_orders", symbol, since, limit
        )

    def fetch_order(self, order_id: str, symbol: str | None):
        self._require_nonempty(order_id, "order id")
        self._validate_symbol(symbol)
        try:
            return self._read_method(
                "fetchOrder", "fetch_order", order_id, symbol, params={}
            )
        except ccxt.OrderNotFound:
            if not self._uses_binance_futures_order_split:
                raise
            return self._read_method(
                "fetchOrder",
                "fetch_order",
                order_id,
                symbol,
                params={"stop": True},
            )

    def cancel_order(
        self, order_id: str, symbol: str | None, params: dict[str, Any] | None = None
    ):
        self._require_nonempty(order_id, "order id")
        self._validate_symbol(symbol)
        try:
            return self._write_method(
                "cancelOrder", "cancel_order", order_id, symbol, params=params or {}
            )
        except ccxt.OrderNotFound:
            if not self._uses_binance_futures_order_split:
                raise
            return self._write_method(
                "cancelOrder",
                "cancel_order",
                order_id,
                symbol,
                params={**(params or {}), "stop": True},
            )

    def cancel_all_orders(
        self, symbol: str | None, params: dict[str, Any] | None = None
    ):
        self._validate_symbol(symbol)
        base_params = params or {}
        first = self._write_method(
            "cancelAllOrders", "cancel_all_orders", symbol, params=base_params
        )
        if not self._uses_binance_futures_order_split:
            return first
        second = self._write_method(
            "cancelAllOrders",
            "cancel_all_orders",
            symbol,
            params={**base_params, "stop": True},
        )
        return [first, second]

    def fetch_my_trades(self, symbol: str | None, since: int | None, limit: int | None):
        self._validate_history_window(since, limit)
        self._validate_symbol(symbol)
        return self._read_method(
            "fetchMyTrades", "fetch_my_trades", symbol, since, limit, params={}
        )

    def fetch_positions(self, symbols: list[str] | None = None):
        self._validate_symbols(symbols)
        return self._read_method(
            "fetchPositions", "fetch_positions", symbols, params={}
        )

    def set_leverage(
        self, leverage: int, symbol: str | None, params: dict[str, Any] | None = None
    ):
        if isinstance(leverage, bool) or leverage <= 0:
            raise InvalidProviderRequest("leverage must be a positive integer")
        self._validate_symbol(symbol)
        return self._write_method(
            "setLeverage", "set_leverage", leverage, symbol, params=params or {}
        )

    def set_margin_mode(
        self,
        margin_mode: str,
        symbol: str | None,
        params: dict[str, Any] | None = None,
    ):
        if margin_mode not in {"cross", "isolated"}:
            raise InvalidProviderRequest("margin mode must be cross or isolated")
        self._validate_symbol(symbol)
        return self._write_method(
            "setMarginMode",
            "set_margin_mode",
            margin_mode,
            symbol,
            params=params or {},
        )

    def _fetch_order_list(
        self,
        capability: str,
        method: str,
        symbol: str | None,
        since: int | None,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        self._validate_history_window(since, limit)
        self._validate_symbol(symbol)
        ordinary = self._read_method(
            capability, method, symbol, since, limit, params={}
        )
        if not self._uses_binance_futures_order_split:
            return ordinary
        stop = self._read_method(
            capability, method, symbol, since, limit, params={"stop": True}
        )
        by_id = {str(order.get("id")): order for order in [*ordinary, *stop]}
        merged = sorted(
            by_id.values(), key=lambda order: order.get("timestamp") or 0, reverse=True
        )
        return merged if limit is None else merged[:limit]

    @property
    def _uses_binance_futures_order_split(self) -> bool:
        return self.exchange_name == "binance" and self.market == "future"

    def _read_method(self, capability: str, method: str, *args: Any, **kwargs: Any):
        raise NotImplementedError

    def _write_method(self, capability: str, method: str, *args: Any, **kwargs: Any):
        raise NotImplementedError

    def _resolve_market(self, symbol: str) -> dict[str, Any]:
        raise NotImplementedError

    def _validate_symbol(self, symbol: str | None) -> None:
        raise NotImplementedError

    def _validate_symbols(self, symbols: list[str] | None) -> None:
        raise NotImplementedError

    @staticmethod
    def _require_positive_finite(value: float, field: str) -> None:
        if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
            raise InvalidProviderRequest(f"{field} must be finite and positive")

    @staticmethod
    def _require_nonempty(value: str, field: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise InvalidProviderRequest(f"{field} must not be empty")

    @staticmethod
    def _validate_history_window(since: int | None, limit: int | None) -> None:
        if since is not None and (isinstance(since, bool) or since < 0):
            raise InvalidProviderRequest("since must be a non-negative integer")
        if limit is not None and (isinstance(limit, bool) or not 1 <= limit <= 100_000):
            raise InvalidProviderRequest("limit must be between 1 and 100000")
