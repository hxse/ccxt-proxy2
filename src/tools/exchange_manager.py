"""按交易所、市场和模式隔离的长期 CcxtClient 注册表。"""

from collections.abc import Callable
from threading import Lock

from fastapi import HTTPException
from loguru import logger

from src.base_types import ExchangeName, MarketType, ModeType
from src.cache_tool import DuckDbOhlcvCache
from src.tools.ccxt_client import CcxtClient
from src.tools.config_types import AppConfig, CcxtServiceConfig
from src.tools.exchange import get_binance_exchange, get_kraken_exchange


class ExchangeManager:
    _instance: "ExchangeManager | None" = None

    def __new__(cls) -> "ExchangeManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._registry: dict[tuple[ExchangeName, MarketType, ModeType], CcxtClient] = {}
        self._whitelist: list[CcxtServiceConfig] = []
        self._lock = Lock()
        self._initializers: dict[tuple[ExchangeName, MarketType, ModeType], Lock] = {}
        self._access_guard: Callable[[str], None] | None = None
        self._generation = 0

    def configure(
        self, config: AppConfig, *, access_guard: Callable[[str], None] | None = None
    ) -> None:
        with self._lock:
            self._whitelist = [
                item for item in config.service_whitelist if item.service == "ccxt"
            ]
            self._access_guard = access_guard

    def init_from_config(
        self, config: AppConfig, cache: DuckDbOhlcvCache | None
    ) -> None:
        self.close()
        self.configure(config)
        for item in self._whitelist:
            try:
                self.initialize(config, item, cache)
            except Exception as exc:
                logger.bind(service=item.identity, error_type=type(exc).__name__).error(
                    "CcxtClient initialization failed; other identities remain available"
                )

    def initialize(
        self, config: AppConfig, item: CcxtServiceConfig, cache: DuckDbOhlcvCache | None
    ) -> None:
        key = (item.exchange, item.market, item.mode)
        if item not in config.service_whitelist:
            raise HTTPException(
                503, detail={"code": "SERVICE_NOT_ENABLED", "service": item.identity}
            )
        with self._lock:
            if item not in self._whitelist:
                self._whitelist.append(item)
            initializer = self._initializers.setdefault(key, Lock())
            generation = self._generation
        with initializer:
            with self._lock:
                if key in self._registry:
                    return
            client = None
            exchange = None
            try:
                if cache is None:
                    raise ValueError("CCXT requires an application-owned cache")
                if item.exchange == "binance":
                    exchange = get_binance_exchange(config, item.market, item.mode)
                else:
                    exchange = get_kraken_exchange(config, item.market, item.mode)
                client = CcxtClient(
                    exchange, item.exchange, item.market, item.mode, cache
                )
                client.load_markets()
                with self._lock:
                    if generation != self._generation:
                        raise HTTPException(
                            503, {"code": "SERVICE_NOT_READY", "service": item.identity}
                        )
                    # 路由永远不能取得尚未成功加载 markets 的半初始化客户端。
                    self._registry[key] = client
            except BaseException:
                close = (
                    client.close
                    if client is not None
                    else getattr(exchange, "close", None)
                )
                if callable(close):
                    try:
                        close()
                    except Exception as exc:
                        logger.bind(
                            service=item.identity, error_type=type(exc).__name__
                        ).error("failed CcxtClient cleanup failed")
                raise

    def close(self) -> None:
        with self._lock:
            self._generation += 1
            clients = list(self._registry.values())
            self._registry = {}
            self._whitelist = []
        for client in clients:
            try:
                client.close()
            except Exception:
                logger.bind(
                    exchange=client.exchange_name,
                    market=client.market,
                    mode=client.mode,
                ).exception("CcxtClient shutdown failed")

    def is_ready(
        self, exchange_name: ExchangeName, market: MarketType, mode: ModeType
    ) -> bool:
        with self._lock:
            client = self._registry.get((exchange_name, market, mode))
        return client is not None and client.is_ready()

    def get_client(
        self,
        exchange_name: ExchangeName,
        market: MarketType,
        mode: ModeType,
    ) -> CcxtClient:
        identity = f"ccxt/{exchange_name}/{market}/{mode}"
        if self._access_guard is not None:
            self._access_guard(identity)
        with self._lock:
            client = self._registry.get((exchange_name, market, mode))
            enabled = any(item.identity == identity for item in self._whitelist)
        if client is None:
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "SERVICE_NOT_READY" if enabled else "SERVICE_NOT_ENABLED",
                    "service": identity,
                },
            )
        return client


exchange_manager = ExchangeManager()
