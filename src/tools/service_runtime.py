"""应用就绪与各身份就绪分开；SDK 初始化任务由本生命周期统一持有。"""

from collections.abc import Callable
from functools import partial
from threading import Event, Lock, RLock, Thread
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
        self._enabled = tuple(item.identity for item in self._config.service_whitelist)
        self.cache_maintenance_lock = Lock()
        self._lock = RLock()
        self._states = dict.fromkeys(self._enabled, "stopped")
        self._checks: dict[str, Callable[[], bool]] = {}
        self._threads: list[Thread] = []
        self._stop = Event()
        self.ready = False
        self._closers: dict[str, Callable[[], None]] = {}
        self.cache = CacheResource(self._config.ohlcv_cache)
        self._stopped = False
        self.jobs: MarketDataScheduler | None = None

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            # 只读取本地线程状态，不做网络探测，也不触发 SDK 初始化。
            for identity, check in self._checks.items():
                if self._states[identity] == "ready" and not check():
                    self._states[identity] = "failed"
            states = dict(self._states)
            return {
                "status": "ready" if self.ready else "not_ready",
                "initialized": [
                    key for key, value in states.items() if value == "ready"
                ],
                "services": states,
            }

    @property
    def initialized(self) -> list[str]:
        return self.snapshot()["initialized"]

    def require(self, identity: str) -> None:
        if identity not in self._enabled:
            raise HTTPException(
                503, detail={"code": "SERVICE_NOT_ENABLED", "service": identity}
            )
        state = self.snapshot()
        if state["status"] != "ready" or state["services"][identity] != "ready":
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
        with self._lock:
            if self.ready:
                return
            self._stop = stop if stop is not None else Event()
            if self._stop.is_set():
                return
            if self._stopped:
                self.cache = CacheResource(self._config.ohlcv_cache)
                self._stopped = False
            # 配置错误属于应用级错误，必须在发起任何 SDK 初始化前拒绝。
            plan = load_market_data_plan()
            validate_client(plan, self._config)
            self.jobs = MarketDataScheduler(plan, self._config)
            self._states = dict.fromkeys(self._enabled, "initializing")
            self._checks = {}
            self._threads = []
            if any(item.service == "ccxt" for item in self._config.service_whitelist):
                ccxt.configure(self._config, access_guard=self.require)
            managers = {"ccxt": ccxt, "tq": tq, "ctp": ctp, "cfb": cfb}
            for item in self._config.service_whitelist:
                if self._stop.is_set():
                    break
                manager = managers[item.service]
                if item.service != "cfb":
                    self._closers.setdefault(item.service, manager.close)
                if item.service == "ccxt":
                    self._checks[item.identity] = partial(
                        manager.is_ready, item.exchange, item.market, item.mode
                    )
                elif item.service in {"tq", "cfb"}:
                    self._checks[item.identity] = manager.is_ready
                thread = Thread(
                    target=self._initialize,
                    args=(item, manager),
                    name=f"initialize:{item.identity}",
                    daemon=True,
                )
                thread.start()
                self._threads.append(thread)
            self.ready = not self._stop.is_set()

    def _initialize(self, item, manager) -> None:
        if self._stop.is_set():
            return
        logger.bind(service=item.identity).info("initializing service")
        try:
            if item.service == "ccxt":
                manager.initialize(self._config, item, self.cache.get())
            elif item.service == "tq":
                manager.initialize(self.cache.get())
            elif item.service == "ctp":
                manager.initialize(item.mode)
            else:
                manager.initialize(self._config.cfb)
        except BaseException as exc:
            with self._lock:
                self._states[item.identity] = "failed"
            logger.bind(service=item.identity, error_type=type(exc).__name__).error(
                "service initialization failed; other services remain available"
            )
        else:
            with self._lock:
                if not self._stop.is_set():
                    self._states[item.identity] = "ready"
            logger.bind(service=item.identity).info("service initialized")

    def begin_shutdown(self) -> None:
        with self._lock:
            self.ready = False
            self._stop.set()

    def wait_for_startup(self) -> None:
        """仅用于关闭/离线验证；HTTP 启动和业务请求不能等待全部 SDK。"""
        with self._lock:
            threads = list(self._threads)
        for thread in threads:
            thread.join()

    def close(self) -> None:
        self.begin_shutdown()
        self.wait_for_startup()
        with self._lock:
            self._stopped = True
            closers, self._closers = self._closers, {}
        for service, close in reversed(list(closers.items())):
            try:
                close()
            except Exception:
                logger.bind(service=service).exception("service shutdown failed")
        with self._lock:
            self._states = dict.fromkeys(self._enabled, "stopped")
            self._checks = {}
            self._threads = []
        self.cache.close()
