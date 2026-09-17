"""按模式复用 CTP client；不加入 CCXT 的 exchange registry。"""

import threading

from loguru import logger

from src.base_types import ModeType
from src.tools.config_types import CtpConfig
from src.tools.ctp_callbacks import CtpError
from src.tools.ctp_client import CtpClient
from src.tools.ctp_session import ApiFactory
from src.tools.ctp_spi import create_api
from src.tools.shared import config


class CtpManager:
    def __init__(self, ctp_config: CtpConfig | None, factory: ApiFactory = create_api):
        self._config, self._factory = ctp_config, factory
        self._clients: dict[ModeType, CtpClient] = {}
        self._lock = threading.Lock()

    def get_client(self, mode: ModeType) -> CtpClient:
        with self._lock:
            account = (
                None
                if self._config is None
                else {"sandbox": self._config.test, "live": self._config.live}[mode]
            )
            if account is None or self._config is None:
                raise CtpError(
                    503,
                    "CTP_NOT_CONFIGURED",
                    "未配置对应的 ctp.test/ctp.live 账户。",
                    mode,
                )
            if mode not in self._clients:
                self._clients[mode] = CtpClient(
                    account, self._config, mode, self._factory
                )
            return self._clients[mode]

    def close(self) -> None:
        with self._lock:
            clients, self._clients = self._clients, {}
            for mode, client in clients.items():
                try:
                    client.close()
                except Exception:
                    logger.bind(mode=mode).exception("CTP client shutdown failed")


ctp_manager = CtpManager(config.ctp)
