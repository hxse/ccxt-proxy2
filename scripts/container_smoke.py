"""在镜像内部执行，不连接任何真实账户的构建烟测。"""

import asyncio

import httpx

from src.main import app
from src.tools.ctp_native import load_td_api

assert load_td_api() is not None


async def check():
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://container"
        ) as client:
            response = await client.get("/readyz")
            assert response.status_code == 200
            assert response.json() == {"status": "ready", "initialized": []}


asyncio.run(check())
