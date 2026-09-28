"""手动在线入口的独立 HTTP 服务：临时配置/库，仅初始化选定 live 行情。"""

import os
import secrets
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest
import tomli_w

ROOT = Path(__file__).resolve().parents[2]
ALLOWED = {
    "/ccxt/fetch_ohlcv/since-limit",
    "/ccxt/fetch_ohlcv/since-latest",
    "/ccxt/fetch_ohlcv/latest-limit",
    "/ccxt/fetch_tickers",
    "/tq/fetch_ohlcv",
    "/tq/fetch_underlying_symbol",
    "/tq/fetch_trading_calendar",
    "/cache/summary",
    "/system/fetch_time",
}


class LiveService:
    def __init__(self, config, identities, directory):
        directory = Path(directory)
        self.log_path = directory / "server.log"
        self._process = None
        self._log = None
        self._failed = False
        self._password = secrets.token_urlsafe(32)
        data = config.model_dump(exclude_none=True)
        data["SECRET"] = secrets.token_urlsafe(40)
        data["users"] = {"online": {"password": self._password}}
        data["service_whitelist"] = [
            item.model_dump(exclude_none=True) for item in identities
        ]
        data["ohlcv_cache"]["database_path"] = str(directory / "isolated.duckdb")
        # 未启用的服务也不把私有账号带入临时配置。
        keep = {item.service for item in identities} | {
            item.exchange for item in identities if hasattr(item, "exchange")
        }
        for name in ("tq", "ctp", "cfb", "telegram", "binance", "kraken"):
            if name not in keep:
                data.pop(name, None)
        data.pop("market_data_client", None)
        path = directory / "config.toml"
        with path.open("xb") as stream:
            os.chmod(path, 0o600)
            tomli_w.dump(data, stream)
        self._config_path = path
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            self._port = reservation.getsockname()[1]
        self._http = httpx.Client(base_url=f"http://127.0.0.1:{self._port}", timeout=55)

    def start(self):
        # cwd 与所有持久化都在临时目录；默认计划在应用导入前关闭。
        program = (
            "from pathlib import Path\n"
            "from src.tools import market_data_config\n"
            f"market_data_config.PLAN_PATH = Path({str(ROOT / 'Test/fixtures/market_data.toml')!r})\n"
            "from src.main import app\n"
            "import uvicorn\n"
            f"uvicorn.run(app, host='127.0.0.1', port={self._port}, log_level='warning')\n"
        )
        self._log = self.log_path.open("w")
        os.chmod(self.log_path, 0o600)
        self._process = subprocess.Popen(
            [sys.executable, "-c", program],
            cwd=self._config_path.parent,
            env={
                **os.environ,
                "CCXT_PROXY_CONFIG_PATH": str(self._config_path),
                "PYTHONPATH": str(ROOT),
            },
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                raise RuntimeError(
                    f"live test service startup failed; isolated log: {self.log_path}"
                )
            try:
                ready = self._http.get("/readyz", timeout=1)
                if ready.status_code == 200:
                    states = ready.json()["services"]
                    if any(value == "failed" for value in states.values()):
                        raise RuntimeError(
                            f"live test provider startup failed; isolated log: {self.log_path}"
                        )
                    if any(value != "ready" for value in states.values()):
                        time.sleep(0.1)
                        continue
                    token = self._http.post(
                        "/auth/token",
                        data={"username": "online", "password": self._password},
                    )
                    assert token.status_code == 200
                    self._http.headers["Authorization"] = (
                        "Bearer " + token.json()["access_token"]
                    )
                    return self
            except httpx.RequestError:
                pass
            time.sleep(0.1)
        raise RuntimeError(
            f"live test service not ready within 60 seconds; isolated log: {self.log_path}"
        )

    def get(self, path, **params):
        assert path in ALLOWED
        if self._failed:
            pytest.skip(
                "previous live request failed; stopping requests to this service"
            )
        try:
            response = self._http.get(path, params=params)
        except httpx.RequestError as exc:
            self._failed = True
            pytest.fail(f"live read failed: {type(exc).__name__}")
        if response.status_code >= 500:
            self._failed = True
        assert response.status_code == 200, (
            f"{path}: HTTP {response.status_code}, {response.text[:500]}"
        )
        return response.json()

    def close(self):
        self._http.close()
        if self._process is not None and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)
        if self._log is not None:
            self._log.close()
        self._config_path.unlink(missing_ok=True)
