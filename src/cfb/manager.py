"""按模式管理轻量客户端；各实例固定账户，业务请求不切换终端。"""

import asyncio
from pathlib import Path
from threading import RLock

from src.base_types import ModeType

from .client import Client
from .config import CfbConfig
from .errors import BridgeError


class Manager:
    def __init__(self, root: Path = Path("data/cfb")):
        self.root = root
        self._clients: dict[ModeType, Client] = {}
        self._lock = RLock()

    def initialize(self, config: CfbConfig, mode: ModeType) -> None:
        settings = config.for_mode(mode)
        with self._lock:
            if mode in self._clients:
                return
            client = Client(self.root / mode / "run/bridge.sock")
            client.initialize(settings)
            self._clients[mode] = client

    def is_ready(self, mode: ModeType) -> bool:
        with self._lock:
            client = self._clients.get(mode)
            return client is not None and client.is_ready()

    def get(self, mode: ModeType) -> Client:
        with self._lock:
            client = self._clients.get(mode)
            if client is None:
                raise BridgeError("SERVICE_NOT_READY", f"CFB {mode} 客户端未就绪")
            return client

    async def close(self) -> None:
        with self._lock:
            clients = tuple(self._clients.values())
            self._clients.clear()
        await asyncio.gather(*(client.close() for client in clients))


cfb_manager = Manager()
