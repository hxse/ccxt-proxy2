"""统一服务启动与访问控制；只使用进程启动时取得的配置快照。"""

from collections.abc import Callable
from threading import Event
from typing import Any

from fastapi import HTTPException
from loguru import logger

from src.tools.config_types import AppConfig


class ServiceRuntime:
    def __init__(self, config: AppConfig):
        self._config = config.model_copy(deep=True)
        self._enabled = frozenset(
            item.identity for item in self._config.service_whitelist
        )
        self.ready = False
        self.initialized: list[str] = []
        self._closers: dict[str, Callable[[], None]] = {}

    def require(self, identity: str) -> None:
        if identity not in self._enabled:
            raise HTTPException(
                503, detail={"code": "SERVICE_NOT_ENABLED", "service": identity}
            )
        if not self.ready:
            raise HTTPException(
                503, detail={"code": "SERVICE_NOT_READY", "service": identity}
            )

    def start(
        self, ccxt: Any, tq: Any, ctp: Any, *, stop: Event | None = None
    ) -> None:
        if self.ready:
            return
        self.initialized = []
        managers = {"ccxt": ccxt, "tq": tq, "ctp": ctp}
        try:
            for item in self._config.service_whitelist:
                # 取消不能中断正在运行的 SDK；只停止后续初始化，清理需等待本方法结束。
                if stop is not None and stop.is_set():
                    return
                # 先登记清理，即使本次初始化只成功了一部分也必须释放。
                self._closers.setdefault(item.service, managers[item.service].close)
                logger.bind(service=item.identity).info("initializing service")
                if item.service == "ccxt":
                    ccxt.initialize(self._config, item)
                elif item.service == "tq":
                    tq.initialize()
                else:
                    ctp.initialize(item.mode)
                self.initialized.append(item.identity)
                logger.bind(service=item.identity).info("service initialized")
            self.ready = stop is None or not stop.is_set()
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        self.ready = False
        closers, self._closers = self._closers, {}
        for service, close in reversed(list(closers.items())):
            try:
                close()
            except Exception:
                logger.bind(service=service).exception("service shutdown failed")
        self.initialized = []
