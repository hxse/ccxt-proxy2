"""手动验证已构建的本地镜像；只请求 live 公共时间，不启动业务 SDK。"""

import json
import os
import subprocess

import pytest

from src.tools.config_loader import resolve_config_path

pytestmark = [
    pytest.mark.online,
    pytest.mark.skipif(
        os.getenv("PUBLIC_TIME_ONLINE") != "1", reason="manual image check required"
    ),
]

PROBE = """
import asyncio
import json
from time import monotonic

import httpx
from fastapi import FastAPI

from src.router.auth_handler import auth_router
from src.router.system_router import system_router
from src.tools.shared import config

async def check():
    app = FastAPI()
    app.include_router(auth_router)
    app.include_router(system_router)
    username = config.market_data_client.user or next(iter(config.users), None)
    assert username in config.users, "需要已配置的本地登录用户"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://image") as client:
        login = await client.post("/auth/token", data={"username": username, "password": config.users[username].password})
        assert login.status_code == 200, "本地鉴权失败"
        start = monotonic()
        response = await client.get("/system/fetch_time", headers={"Authorization": "Bearer " + login.json()["access_token"]})
        payload = response.json()
        if response.status_code != 200:
            print(json.dumps({"status": response.status_code, "detail": payload.get("detail")}), flush=True)
            raise SystemExit(1)
        assert type(payload["serverTime"]) is int and payload["serverTime"] > 0
        assert response.headers["cache-control"] == "no-store"
        print(json.dumps({"status": 200, "serverTime": payload["serverTime"], "seconds": round(monotonic()-start, 3), "profile": "local", "proxy_enabled": bool(config.binance and config.binance.enable_proxy)}), flush=True)

asyncio.run(check())
"""


def test_local_image_fetches_live_public_time():
    result = subprocess.run(
        [
            "podman",
            "run",
            "--rm",
            "--pull=never",
            "--interactive",
            "--env",
            "CCXT_PROXY_PROFILE=local",
            "--volume",
            f"{resolve_config_path()}:/app/config.toml:ro",
            "--entrypoint",
            "/app/.venv/bin/python",
            "localhost/ccxt-proxy2:latest",
            "-",
        ],
        input=PROBE,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    if result.stderr:
        print(result.stderr)
    assert "Unclosed" not in result.stderr and "ResourceWarning" not in result.stderr
    summary = json.loads(result.stdout.splitlines()[-1])
    assert summary["status"] == 200 and summary["profile"] == "local"
    print(json.dumps(summary, ensure_ascii=False))
