from typing import Any

CCXT_READ_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: {"description": "Bearer token 无效或缺失。"},
    422: {
        "description": "参数、symbol、timeframe、variant 或 Provider capability 无效。"
    },
    502: {"description": "Provider 网络、认证、数据完整性或上游操作失败。"},
    503: {"description": "请求的 exchange/market/mode identity 未启用或已关闭。"},
}
OHLCV_RESPONSES = {
    **CCXT_READ_RESPONSES,
    507: {"description": "OHLCV cache capacity eviction 失败；本次写入已回滚。"},
}
CCXT_ORDER_READ_RESPONSES = {
    **CCXT_READ_RESPONSES,
    404: {"description": "Provider 中不存在指定订单。"},
}
CCXT_WRITE_RESPONSES: dict[int | str, dict[str, Any]] = {
    **CCXT_ORDER_READ_RESPONSES,
    409: {"description": "资金不足、保证金状态冲突或 Provider 拒绝该操作。"},
}

CLOSE_POSITION_DESCRIPTION = (
    "有状态操作：读取当前 positions，并对指定 symbol/可选 side 的非零仓位提交强制 "
    "reduceOnly market order；这是多个 CCXT 动作的薄编排而非原子事务。失败时调用方"
    "必须重新 fetch_positions 对账，服务端不承诺回滚已提交动作。"
)
CANCEL_ALL_ORDERS_DESCRIPTION = (
    "高影响有状态操作：撤销指定 symbol 或 Provider 范围内的全部订单；Binance "
    "Futures 依次转发 normal 与 conditional 撤单，不提供跨请求原子性。失败时调用方"
    "必须重新 fetch_open_orders 对账。"
)
