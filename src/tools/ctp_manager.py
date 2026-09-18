"""按模式复用 CTP client；不加入 CCXT 的 exchange registry。"""

import threading
from collections.abc import Callable

from loguru import logger

from src.base_types import ModeType
from src.responses_ctp import CtpTradingStatusResponse
from src.tools.config_types import CtpConfig
from src.tools.ctp_callbacks import CtpError
from src.tools.ctp_client import CtpClient
from src.tools.ctp_session import ApiFactory
from src.tools.ctp_spi import create_api
from src.tools.shared import config, service_runtime
from src.types_ctp import CtpTradingStatusQuery


class CtpManager:
    def __init__(
        self,
        ctp_config: CtpConfig | None,
        factory: ApiFactory = create_api,
        *,
        access_guard: Callable[[str], None] | None = None,
    ):
        self._config = ctp_config.model_copy(deep=True) if ctp_config else None
        self._factory, self._access_guard = factory, access_guard
        self._clients: dict[ModeType, CtpClient] = {}
        self._lock = threading.Lock()

    def get_client(self, mode: ModeType) -> CtpClient:
        if self._access_guard is not None:
            self._access_guard(f"ctp/{mode}")
        return self._get_client(mode)

    def initialize(self, mode: ModeType) -> None:
        """仅由启动协调器调用；HTTP 访问先经过白名单与就绪检查。"""
        self._get_client(mode).initialize()

    def fetch_trading_status(
        self, request: CtpTradingStatusQuery
    ) -> CtpTradingStatusResponse:
        if self._access_guard is not None:
            self._access_guard(f"ctp/{request.mode}")
        # 客户端在启动时登记；取现有引用，不经过可能正在关闭连接的 manager 锁。
        client = self._clients.get(request.mode)
        if client is None:
            return CtpTradingStatusResponse(
                mode=request.mode,
                exchange_id=request.exchange_id,
                product_id=request.product_id,
                reason="unavailable",
            )
        return client.fetch_trading_status(request)

    def _get_client(self, mode: ModeType) -> CtpClient:
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


ctp_manager = CtpManager(config.ctp, access_guard=service_runtime.require)
