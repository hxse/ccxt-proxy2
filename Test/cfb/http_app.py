"""迁入的业务样本使用正式路由和协议处理器；只在测试中省略网络与鉴权。"""

import asyncio

from fastapi import FastAPI

from src.cfb.client import Client
from src.cfb.diagnostics import diagnostic_router
from src.cfb.errors import BridgeError
from src.cfb.ipc import Request
from src.cfb.results import Reply
from src.cfb.routes import business_router
from src.cfb.server import Server, response


def create_app(service, *, manage_lifecycle=False):
    assert not manage_lifecycle
    server = Server(service, service.settings.bridge.data_dir / "run/bridge.sock")

    class LoopbackClient(Client):
        async def call(self, kind, *, request_id=None, operation=None, key=None):
            disconnected = asyncio.create_task(asyncio.Event().wait())
            request = Request(kind=kind, request_id=request_id or "cfb-test",
                              configuration=service.settings.identity_signature,
                              operation=operation, idempotency_key=key)
            try:
                return await server.handle(request, disconnected)
            except BridgeError as exc:
                return response(service, Reply(request_id=request.request_id, status=exc.status,
                    body=exc.response(request.request_id).model_dump(mode="json")))
            finally:
                disconnected.cancel()
                await asyncio.gather(disconnected, return_exceptions=True)

    client = LoopbackClient()
    client.settings = service.settings
    client.enabled = True
    app = FastAPI()
    app.include_router(business_router(lambda: client))
    app.include_router(diagnostic_router(lambda: client), prefix="/cfb")
    return app
