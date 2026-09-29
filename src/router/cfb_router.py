"""统一鉴权下的 CFB 路由；领域模型直接生成文档，业务经唯一 IPC 执行。"""

from fastapi import APIRouter, Depends, Request

from src.base_types import ModeType
from src.cfb.client import Client
from src.cfb.diagnostics import diagnostic_router
from src.cfb.manager import cfb_manager
from src.cfb.routes import business_router
from src.router.auth_handler import manager


def get_client(request: Request, mode: ModeType) -> Client:
    request.app.state.service_runtime.require(f"cfb/{mode}")
    return cfb_manager.get(mode)


cfb_router = APIRouter(dependencies=[Depends(manager)], tags=["CFB"],
                      responses={401: {"description": "未通过本项目 Bearer 鉴权。"}})
cfb_router.include_router(business_router(get_client))
cfb_router.include_router(diagnostic_router(get_client), prefix="/cfb")
