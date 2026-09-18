"""真实应用生命周期、鉴权与禁用服务检查；独立进程使用临时无外部账号配置。"""

import os
import subprocess
import sys


def test_application_ignores_config_edits_and_disabled_http_never_loads_sdks(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(
        "SECRET='offline'\nservice_whitelist=[]\n[users.bruno]\npassword='before'\n"
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os
import sys
from pathlib import Path
from fastapi.testclient import TestClient
from src.main import app

with TestClient(app) as client:
    assert client.get("/readyz").json() == {"status": "ready", "initialized": []}
    login = client.post("/auth/token", data={"username": "bruno", "password": "before"})
    assert login.status_code == 200
    headers = {"Authorization": "Bearer " + login.json()["access_token"]}
    path = Path(os.environ["CCXT_PROXY_CONFIG_PATH"])
    path.write_text("SECRET='edited'\\nservice_whitelist=[{service='tq'}]\\n[tq]\\nusername='unused'\\npassword='unused'\\n[users.bruno]\\npassword='after'\\n")
    for route, params, identity in [
        ("/tq/fetch_trading_status", {"symbol":"SHFE.rb2610"}, "tq"),
        ("/ctp/fetch_trading_status", {"exchange_id":"SHFE", "product_id":"rb"}, "ctp/sandbox"),
        ("/ccxt/fetch_balance", {"exchange_name":"binance", "market":"future", "mode":"sandbox"}, "ccxt/binance/future/sandbox"),
    ]:
        response = client.get(route, params=params, headers=headers)
        assert response.status_code == 503, response.status_code
        assert response.json() == {"detail": {"code": "SERVICE_NOT_ENABLED", "service": identity}}
    assert client.get("/readyz").json() == {"status": "ready", "initialized": []}
    assert client.post("/auth/token", data={"username":"bruno", "password":"before"}).status_code == 200
    assert client.post("/auth/token", data={"username":"bruno", "password":"after"}).status_code == 401
    assert "tqsdk" not in sys.modules and "ctpwrapper" not in sys.modules
print("startup snapshot and disabled HTTP verified")
""",
        ],
        env={**os.environ, "CCXT_PROXY_CONFIG_PATH": str(path)},
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "startup snapshot and disabled HTTP verified" in result.stdout
