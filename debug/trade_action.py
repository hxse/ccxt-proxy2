import argparse
import json
import sys
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.main import app  # noqa: E402
from src.router.auth_handler import manager  # noqa: E402


DEFAULT_EXCHANGE = "binance"
DEFAULT_MARKET = "future"
DEFAULT_MODE = "sandbox"
DEFAULT_SYMBOL = "BTC/USDT:USDT"
DEFAULT_AMOUNT = 0.005


def auth_override() -> dict[str, str]:
    return {"username": "debug"}


def print_response(label: str, status_code: int, data: Any) -> None:
    print(f"\n=== {label} ===")
    print(f"status: {status_code}")
    print(json.dumps(data, ensure_ascii=False, indent=2, default=str))


def request(client: TestClient, method: str, path: str, **kwargs: Any) -> Any:
    response = client.request(method, path, **kwargs)
    try:
        data = response.json()
    except Exception:
        data = response.text
    print_response(f"{method} {path}", response.status_code, data)
    response.raise_for_status()
    return data


def base_payload(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "exchange_name": args.exchange,
        "market": args.market,
        "mode": args.mode,
    }


def symbol_payload(args: argparse.Namespace) -> dict[str, Any]:
    return {
        **base_payload(args),
        "symbol": args.symbol,
    }


def require_trigger(args: argparse.Namespace) -> float:
    if args.trigger_price is None:
        raise SystemExit("--trigger-price is required for this action")
    return args.trigger_price


def run_action(client: TestClient, args: argparse.Namespace) -> None:
    action = args.action

    if action == "ready":
        request(client, "GET", "/readyz")
        return

    if action == "balance":
        request(client, "GET", "/ccxt/fetch_balance", params=base_payload(args))
        return

    if action == "positions":
        request(
            client,
            "GET",
            "/ccxt/fetch_positions",
            params={**base_payload(args), "symbols": args.symbol},
        )
        return

    if action == "open-orders":
        request(client, "GET", "/ccxt/fetch_open_orders", params=symbol_payload(args))
        return

    if action == "cancel-all":
        request(client, "POST", "/ccxt/cancel_all_orders", json=symbol_payload(args))
        return

    if action in {"open-long", "open-short"}:
        side = "buy" if action == "open-long" else "sell"
        request(
            client,
            "POST",
            "/ccxt/create_market_order",
            json={**symbol_payload(args), "side": side, "amount": args.amount},
        )
        return

    if action == "close-position":
        payload = symbol_payload(args)
        if args.side:
            payload["side"] = args.side
        request(client, "POST", "/ccxt/close_position", json=payload)
        return

    if action in {"stop-loss-long", "stop-loss-short"}:
        side = "sell" if action == "stop-loss-long" else "buy"
        request(
            client,
            "POST",
            "/ccxt/create_stop_market_order",
            json={
                **symbol_payload(args),
                "side": side,
                "amount": args.amount,
                "triggerPrice": require_trigger(args),
                "reduceOnly": True,
            },
        )
        return

    if action in {"take-profit-long", "take-profit-short"}:
        side = "sell" if action == "take-profit-long" else "buy"
        request(
            client,
            "POST",
            "/ccxt/create_take_profit_market_order",
            json={
                **symbol_payload(args),
                "side": side,
                "amount": args.amount,
                "triggerPrice": require_trigger(args),
                "reduceOnly": True,
            },
        )
        return

    if action == "set-leverage":
        if args.leverage is None:
            raise SystemExit("--leverage is required for this action")
        request(
            client,
            "POST",
            "/ccxt/set_leverage",
            json={**symbol_payload(args), "leverage": args.leverage},
        )
        return

    if action == "set-margin-mode":
        if args.margin_mode is None:
            raise SystemExit("--margin-mode is required for this action")
        request(
            client,
            "POST",
            "/ccxt/set_margin_mode",
            json={**symbol_payload(args), "marginMode": args.margin_mode},
        )
        return

    raise SystemExit(f"unsupported action: {action}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one focused trading debug action.")
    parser.add_argument(
        "action",
        choices=[
            "ready",
            "balance",
            "positions",
            "open-orders",
            "cancel-all",
            "open-long",
            "open-short",
            "close-position",
            "stop-loss-long",
            "stop-loss-short",
            "take-profit-long",
            "take-profit-short",
            "set-leverage",
            "set-margin-mode",
        ],
    )
    parser.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    parser.add_argument("--market", default=DEFAULT_MARKET)
    parser.add_argument("--mode", default=DEFAULT_MODE)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--amount", type=float, default=DEFAULT_AMOUNT)
    parser.add_argument("--side", choices=["long", "short", ""], default="")
    parser.add_argument("--trigger-price", type=float)
    parser.add_argument("--leverage", type=int)
    parser.add_argument("--margin-mode", choices=["cross", "isolated"])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app.dependency_overrides[manager] = auth_override
    try:
        with TestClient(app) as client:
            run_action(client, args)
    finally:
        app.dependency_overrides = {}


if __name__ == "__main__":
    main()
