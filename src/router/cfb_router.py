"""统一鉴权下的 CFB 路由；领域模型直接生成文档，业务经唯一 IPC 执行。"""

from fastapi import APIRouter, Depends, Request

from src.cfb.client import cfb_client
from src.cfb.diagnostics import diagnostic_router
from src.cfb.routes import business_router
from src.router.auth_handler import manager


async def require_cfb(request: Request) -> None:
    request.app.state.service_runtime.require("cfb")


cfb_router = APIRouter(dependencies=[Depends(manager), Depends(require_cfb)], tags=["CFB"],
                      responses={401: {"description": "未通过本项目 Bearer 鉴权。"}})
cfb_router.include_router(business_router(lambda: cfb_client))
cfb_router.include_router(diagnostic_router(lambda: cfb_client), prefix="/cfb")
