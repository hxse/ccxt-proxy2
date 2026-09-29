"""CFB 原有诊断迁入主 HTTP 服务；不启动第二个 HTTP 应用。"""

import base64
import binascii
from collections.abc import Callable

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from .client import Client
from .errors import BridgeError, ServiceStatus
from .http_execution import CfbRoute


def diagnostic_router(get_client: Callable[[], Client]) -> APIRouter:
    router = APIRouter(route_class=CfbRoute)

    @router.get("/status", response_model=ServiceStatus, summary="CFB 终端详细状态")
    async def status(request: Request):
        """读取执行器状态、队列及自动重连信息；不触发重新登录。"""
        reply = await get_client().call("status", request_id=request.state.request_id)
        return JSONResponse(reply.body, status_code=reply.status, headers=reply.headers)

    @router.get("/readyz", response_model=ServiceStatus, summary="CFB 终端交易就绪")
    async def ready(request: Request):
        """终端交易就绪返回 200，未就绪返回 503；主应用及其他 SDK 不受影响。"""
        reply = await get_client().call("status", request_id=request.state.request_id)
        if reply.status != 200:
            return JSONResponse(reply.body, status_code=reply.status, headers=reply.headers)
        value = ServiceStatus.model_validate(reply.body)
        return JSONResponse(value.model_dump(mode="json"), status_code=200 if value.trading_ready else 503,
                            headers=reply.headers)

    @router.get("/healthz", summary="CFB 执行进程存活")
    async def health(request: Request):
        """检查独立执行进程的协议连通性；成功不代表已登录或允许交易。"""
        reply = await get_client().call("health", request_id=request.state.request_id)
        return JSONResponse(reply.body, status_code=reply.status, headers=reply.headers)

    @router.get("/desktop/screenshot", response_class=Response, summary="CFB 虚拟桌面截图",
                responses={200: {"content": {"image/png": {}}}})
    async def screenshot(request: Request):
        """读取当前虚拟桌面的 PNG，失败明确报错；不操作终端控件。"""
        reply = await get_client().call("screenshot", request_id=request.state.request_id)
        if reply.status != 200:
            return JSONResponse(reply.body, status_code=reply.status, headers=reply.headers)
        try:
            if not isinstance(reply.body, dict) or reply.body.get("content_type") != "image/png":
                raise ValueError
            encoded = reply.body["base64"]
            if not isinstance(encoded, str):
                raise ValueError
            data = base64.b64decode(encoded, validate=True)
            if not data.startswith(b"\x89PNG\r\n\x1a\n"):
                raise ValueError
        except (KeyError, TypeError, ValueError, binascii.Error):
            raise BridgeError("TERMINAL_DATA_INVALID", "截图数据无效", 502) from None
        return Response(data, media_type="image/png", headers=reply.headers)

    return router
