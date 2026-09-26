"""统一服务启动与访问控制；只使用进程启动时取得的配置快照。"""

from collections.abc import Callable
from threading import Event, Lock
from typing import Any

from fastapi import HTTPException
from loguru import logger

from src.tools.cache_resource import CacheResource
from src.tools.config_types import AppConfig
from src.tools.market_data_config import load_market_data_plan, validate_client
from src.tools.market_data_pipeline import MarketDataScheduler


class ServiceRuntime:
    def __init__(self, config: AppConfig):
        self._config = config.model_copy(deep=True)
        self._enabled = frozenset(
            item.identity for item in self._config.service_whitelist
        )
        self.cache_maintenance_lock = Lock()
        self.ready = False
        self.initialized: list[str] = []
        self._closers: dict[str, Callable[[], None]] = {}
        self.cache = CacheResource(self._config.ohlcv_cache)
        self._stopped = False
        self.jobs: MarketDataScheduler | None = None

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
        self,
        ccxt: Any,
        tq: Any,
        ctp: Any,
        cfb: Any = None,
        *,
        stop: Event | None = None,
    ) -> None:
        if self.ready:
            return
        if self._stopped:
            self.cache = CacheResource(self._config.ohlcv_cache)
            self._stopped = False
        self.initialized = []
        managers = {"ccxt": ccxt, "tq": tq, "ctp": ctp}
        try:
            plan = load_market_data_plan()
            validate_client(plan, self._config)
            self.jobs = MarketDataScheduler(plan, self._config)
            for item in self._config.service_whitelist:
                # 取消不能中断正在运行的 SDK；只停止后续初始化，清理需等待本方法结束。
                if stop is not None and stop.is_set():
                    return
                if item.service == "cfb":
                    # AsyncClient 只在这里创建；异步关闭交给应用 lifespan 的事件循环。
                    logger.bind(service=item.identity).info("initializing service")
                    cfb.initialize(self._config.cfb)
                    self.initialized.append(item.identity)
                    logger.bind(service=item.identity).info("service initialized")
                    continue
                # 先登记清理，即使本次初始化只成功了一部分也必须释放。
                self._closers.setdefault(item.service, managers[item.service].close)
                logger.bind(service=item.identity).info("initializing service")
                if item.service == "ccxt":
                    ccxt.initialize(self._config, item, self.cache.get())
                elif item.service == "tq":
                    tq.initialize(self.cache.get())
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
        self._stopped = True
        closers, self._closers = self._closers, {}
        for service, close in reversed(list(closers.items())):
            try:
                close()
            except Exception:
                logger.bind(service=service).exception("service shutdown failed")
        self.initialized = []
        self.cache.close()
