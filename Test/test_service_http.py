"""真实应用生命周期、鉴权与禁用服务检查；独立进程使用临时无外部账号配置。"""

import os
import subprocess
import sys


def test_application_ignores_config_edits_and_disabled_http_never_loads_sdks(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(
        "SECRET='offline-test-secret-at-least-32-bytes'\nservice_whitelist=[]\n[users.bruno]\npassword='before'\n"
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os
import sys
import asyncio
from contextlib import contextmanager
from pathlib import Path
from Test.test_ctp_http import LocalClient
from src.tools import market_data_config
market_data_config.PLAN_PATH = Path("Test/fixtures/market_data.toml").resolve()
from src.main import app

@contextmanager
def application():
    with asyncio.Runner() as runner:
        lifecycle = app.router.lifespan_context(app)
        runner.run(lifecycle.__aenter__())
        try:
            yield LocalClient(app)
        finally:
            runner.run(lifecycle.__aexit__(None, None, None))

with application() as client:
    assert client.get("/readyz").json() == {"status": "ready", "initialized": [], "services": {}}
    login = client.post("/auth/token", data={"username": "bruno", "password": "before"})
    assert login.status_code == 200
    headers = {"Authorization": "Bearer " + login.json()["access_token"]}
    path = Path(os.environ["CCXT_PROXY_CONFIG_PATH"])
    path.write_text("SECRET='edited'\\nservice_whitelist=[{service='tq'}]\\n[tq]\\nusername='unused'\\npassword='unused'\\n[users.bruno]\\npassword='after'\\n")
    for route, params, identity in [
        ("/tq/fetch_trading_status", {"symbol":"SHFE.rb2610"}, "tq"),
        ("/ctp/fetch_trading_status", {"is_live":False, "exchange_id":"SHFE", "product_id":"rb"}, "ctp/sandbox"),
        ("/cfb/fetch_balance", {"is_live": True}, "cfb/live"),
        ("/ccxt/fetch_balance", {"exchange_name":"binance", "market":"future", "is_live": False}, "ccxt/binance/future/sandbox"),
    ]:
        response = client.get(route, params=params, headers=headers)
        assert response.status_code == 503, response.status_code
        assert response.json() == {"detail": {"code": "SERVICE_NOT_ENABLED", "service": identity}}
    assert client.get("/readyz").json() == {"status": "ready", "initialized": [], "services": {}}
    assert client.post("/auth/token", data={"username":"bruno", "password":"before"}).status_code == 200
    assert client.post("/auth/token", data={"username":"bruno", "password":"after"}).status_code == 401
    assert "tqsdk" not in sys.modules and "vnpy_ctp" not in sys.modules and "_ccxt_proxy_ctp.vnctptd" not in sys.modules
print("startup snapshot and disabled HTTP verified")
""",
        ],
        env={**os.environ, "CCXT_PROXY_CONFIG_PATH": str(path)},
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Warning:" not in result.stderr
    assert "startup snapshot and disabled HTTP verified" in result.stdout
