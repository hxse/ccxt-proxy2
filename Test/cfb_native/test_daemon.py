"""实际执行进程启动检查；镜像断网、空账户、临时 /data。"""

import asyncio
import importlib.util
import os
import subprocess
import sys
import time
from pathlib import Path

from src.cfb.client import Client
from src.cfb.errors import BridgeError, ServiceStatus


def test_daemon_opens_socket_and_desktop_without_http_or_account(tmp_path):
    assert importlib.util.find_spec("fastapi") is None
    assert importlib.util.find_spec("uvicorn") is None
    config = tmp_path / "config.toml"
    config.write_text('SECRET="isolated"\n[[service_whitelist]]\nservice="cfb"\n'
                      '[cfb]\nvnc_enabled=false\n[cfb.bridge]\nstartup_timeout_seconds=60\n')
    process = subprocess.Popen([sys.executable, "-m", "src.cfb.daemon", "--config", str(config)],
                               env=dict(os.environ, CCXT_PROXY_PROFILE="dev"),
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    async def inspect():
        client = Client(Path("/data/run/bridge.sock"))
        client.enabled = True
        client.timeout = 2
        deadline = time.monotonic() + 60
        try:
            while True:
                assert process.poll() is None, "独立执行器提前退出"
                assert time.monotonic() < deadline, "空账户桌面在预算内未出现"
                try:
                    health = await client.call("health")
                    state = await client.call("status")
                except BridgeError:
                    await asyncio.sleep(.2)
                    continue
                assert health.status == 200 and health.body == {"status": "ok"}
                status = ServiceStatus.model_validate(state.body)
                assert not status.trading_ready and not status.account_configured
                if status.terminal_window_visible:
                    screenshot = await client.call("screenshot")
                    assert screenshot.status == 200
                    assert isinstance(screenshot.body, dict) and screenshot.body["content_type"] == "image/png"
                    break
                assert status.error is None, state.body
                await asyncio.sleep(.2)
        finally:
            await client.close()

    try:
        asyncio.run(inspect())
    finally:
        process.terminate()
        process.wait(timeout=35)
    assert process.returncode == 0
    assert not Path("/data/run/bridge.sock").exists()
