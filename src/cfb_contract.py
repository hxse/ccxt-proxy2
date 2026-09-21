"""固定的转发入口及 CFB 自动文档快照，不包含业务校验或返回模型。"""

import json
from pathlib import Path
from typing import Any

CFB_ROUTES = {
    "/cfb/create_market_order": "post",
    "/cfb/create_limit_order": "post",
    "/cfb/cancel_order": "post",
    "/cfb/fetch_orders": "get",
    "/cfb/fetch_trades": "get",
    "/cfb/fetch_positions": "get",
    "/cfb/fetch_balance": "get",
    "/cfb/fetch_trading_status": "get",
}
CONTRACT_PATH = Path(__file__).parent / "openapi" / "cfb.json"


def load_contract() -> dict[str, Any]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
