from typing import Any

import ccxt

from src.domain_errors import InvalidProviderData


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
        close_params = {"reduceOnly": True, **(params or {})}
        for position in positions:
            amount = float(position.get("contracts") or 0)
            if amount <= 0:
                continue
            close_side = "sell" if position.get("side") == "long" else "buy"
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
                params={"stop": True, **(params or {})},
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
            params={"stop": True, **base_params},
        )
        return [first, second]

    def fetch_my_trades(self, symbol: str | None, since: int | None, limit: int | None):
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
        return sorted(
            by_id.values(), key=lambda order: order.get("timestamp") or 0, reverse=True
        )

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
