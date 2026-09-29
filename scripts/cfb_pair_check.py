"""同时运行两个空账户、断网的 CFB 容器，验证实际跨容器 socket 和隔离。"""

import asyncio
import json
import tempfile
import time
from pathlib import Path
from uuid import uuid4

from scripts.container_common import command
from src.cfb.errors import BridgeError, ServiceStatus
from src.cfb.layout import MODES
from src.cfb.manager import Manager
from src.cfb.models import Operation
from src.tools.config_loader import load_config

CONFIG = '''SECRET="isolated-cfb-pair-check"
service_whitelist = [
    {service="cfb", is_live=false},
    {service="cfb", is_live=true},
]
[cfb]
vnc_enabled=false
[cfb.accounts.live]
broker_id="6020"
site="一套"
'''


def check_pair() -> None:
    with tempfile.TemporaryDirectory(prefix="ccxt-cfb-pair-") as temporary:
        root = Path(temporary)
        config_path = root / "config.toml"
        config_path.write_text(CONFIG)
        config = load_config(config_path, profile="local")
        assert config.cfb is not None
        registry = Manager(root / "data")
        names = {}
        try:
            for mode in MODES:
                folder = root / "data" / mode
                folder.mkdir(parents=True)
                registry.initialize(config.cfb, mode)
                name = f"ccxt-proxy2-cfb-check-{uuid4().hex[:8]}-{mode}"
                names[mode] = name
                command(["podman", "run", "--detach", "--pull=never", "--name", name,
                         "--network=none", "--shm-size=256m", "--env", "CCXT_PROXY_PROFILE=local",
                         "--volume", f"{folder}:/data:rw", "--volume", f"{config_path}:/app/config.toml:ro",
                         "localhost/ccxt-proxy2-cfb:verification-runtime", "python", "-m", "src.cfb.daemon",
                         "--is-live", str(mode == "live").lower(), "--config", "/app/config.toml"])
            asyncio.run(_inspect(registry, names))
            print("CFB 双容器验证通过：模式、桌面、socket 分离，停止模拟盘不影响实盘；未连接账户。")
        finally:
            asyncio.run(registry.close())
            if names:
                command(["podman", "rm", "--force", "--ignore", "--time=30", *names.values()], timeout=90)


async def _inspect(registry: Manager, names: dict) -> None:
    for mode in MODES:
        registry.get(mode).timeout = 2
    deadline = time.monotonic() + 60
    ready = set()
    last = {}
    while len(ready) < 2:
        assert time.monotonic() < deadline, json.dumps(last, ensure_ascii=False)
        for mode in MODES:
            try:
                reply = await registry.get(mode).call("status")
            except BridgeError:
                continue
            assert reply.status == 200, reply.body
            status = ServiceStatus.model_validate(reply.body)
            last[mode] = status.model_dump(mode="json")
            assert status.request_mode == mode and not status.account_configured
            assert not status.trading_ready and status.error is None, last[mode]
            if status.terminal_window_visible:
                ready.add(mode)
        await asyncio.sleep(.2)
    wrong = await registry.get("sandbox").call("execute", operation=Operation(
        action="fetch_balance", parameters={"is_live": True}))
    assert wrong.status == 409
    assert isinstance(wrong.body, dict)
    error = wrong.body["error"]
    assert isinstance(error, dict) and error["code"] == "ENVIRONMENT_MISMATCH"
    command(["podman", "stop", "--time=30", names["sandbox"]])
    assert (await registry.get("live").call("health")).status == 200
    try:
        await registry.get("sandbox").call("health")
    except BridgeError as exc:
        assert exc.code == "SERVICE_NOT_READY"
    else:
        raise AssertionError("已停止模拟盘不应转到实盘")
