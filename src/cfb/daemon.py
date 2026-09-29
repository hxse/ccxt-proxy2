"""独立执行容器入口：配置、终端和 Unix socket，无 FastAPI/Uvicorn。"""

import argparse
import asyncio
import logging
import signal
from pathlib import Path

from src.tools.config_loader import load_config

from .network import prepare_proxy
from .server import Server
from .service import BridgeService


async def run(config_path: Path) -> None:
    config = load_config(config_path)
    if not any(item.service == "cfb" for item in config.service_whitelist) or config.cfb is None:
        raise ValueError("CFB is not enabled")
    settings = config.cfb
    if settings.enable_proxy:
        assert config.proxy.effective_http is not None
        prepare_proxy(settings, config.proxy.effective_http)
    service = BridgeService(settings, vnc=settings.vnc_enabled)
    server = Server(service, settings.bridge.data_dir / "run/bridge.sock")
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stopped.set)
    started = False
    try:
        await asyncio.to_thread(service.start)
        started = True
        await server.start()
        await stopped.wait()
    finally:
        await server.close()
        if started:
            await asyncio.to_thread(service.stop)


def main() -> int:
    parser = argparse.ArgumentParser(description="CFB Unix socket 执行器")
    parser.add_argument("--config", type=Path, default=Path("/app/config.toml"))
    args = parser.parse_args()
    try:
        asyncio.run(run(args.config))
    except Exception as exc:
        logging.error("CFB 执行器启动失败：%s；配置值不输出", type(exc).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
