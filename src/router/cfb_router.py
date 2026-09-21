"""八条固定 CFB 路由；参数和响应原样转发，自动文档同步自上游。"""

from fastapi import APIRouter, Depends, Request, Response

from src.router.auth_handler import manager
from src.tools.cfb_proxy import cfb_proxy


async def require_cfb(request: Request) -> None:
    request.app.state.service_runtime.require("cfb")


cfb_router = APIRouter(
    prefix="/cfb",
    tags=["CFB"],
    dependencies=[Depends(manager), Depends(require_cfb)],
    default_response_class=Response,
)


@cfb_router.post("/create_market_order")
async def create_market_order(request: Request) -> Response:
    """转发市价开平仓请求及原始提交结果；开平、方向和模式由上游处理。"""
    return await cfb_proxy.forward(request, "/cfb/create_market_order")


@cfb_router.post("/create_limit_order")
async def create_limit_order(request: Request) -> Response:
    """转发限价开平仓请求及原始提交结果，不修改价格、数量或交易模式。"""
    return await cfb_proxy.forward(request, "/cfb/create_limit_order")


@cfb_router.post("/cancel_order")
async def cancel_order(request: Request) -> Response:
    """转发撤单 JSON 和可选幂等头；订单身份由 CFB 核对。"""
    return await cfb_proxy.forward(request, "/cfb/cancel_order")


@cfb_router.get("/fetch_orders")
async def fetch_orders(request: Request) -> Response:
    """原样转发订单查询条件及 CFB 返回的完整响应。"""
    return await cfb_proxy.forward(request, "/cfb/fetch_orders")


@cfb_router.get("/fetch_trades")
async def fetch_trades(request: Request) -> Response:
    """原样转发成交查询条件及 CFB 返回的完整响应。"""
    return await cfb_proxy.forward(request, "/cfb/fetch_trades")


@cfb_router.get("/fetch_positions")
async def fetch_positions(request: Request) -> Response:
    """原样转发持仓查询条件及 CFB 返回的完整响应。"""
    return await cfb_proxy.forward(request, "/cfb/fetch_positions")


@cfb_router.get("/fetch_balance")
async def fetch_balance(request: Request) -> Response:
    """原样转发账户资金查询条件及 CFB 返回的完整响应。"""
    return await cfb_proxy.forward(request, "/cfb/fetch_balance")


@cfb_router.get("/fetch_trading_status")
async def fetch_trading_status(request: Request) -> Response:
    """转发品种交易状态查询，保留 CFB 状态字段和错误语义。"""
    return await cfb_proxy.forward(request, "/cfb/fetch_trading_status")
