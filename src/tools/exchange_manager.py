"""Long-lived CcxtClient registry."""

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

    def init_from_config(
        self, config: AppConfig, cache: DuckDbOhlcvCache | None
    ) -> None:
        self.close()
        try:
            for item in config.service_whitelist:
                if item.service == "ccxt":
                    self.initialize(config, item, cache)
        except BaseException:
            self.close()
            raise

    def initialize(
        self, config: AppConfig, item: CcxtServiceConfig, cache: DuckDbOhlcvCache | None
    ) -> None:
        key = (item.exchange, item.market, item.mode)
        if item not in config.service_whitelist:
            raise HTTPException(
                503, detail={"code": "SERVICE_NOT_ENABLED", "service": item.identity}
            )
        if key in self._registry:
            return
        try:
            if cache is None:
                raise ValueError("CCXT requires an application-owned cache")
            if item.exchange == "binance":
                exchange = get_binance_exchange(config, item.market, item.mode)
            else:
                exchange = get_kraken_exchange(config, item.market, item.mode)
            client = CcxtClient(exchange, item.exchange, item.market, item.mode, cache)
            self._registry[key] = client
            client.load_markets()
            self._whitelist.append(item)
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
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

    def get_client(
        self,
        exchange_name: ExchangeName,
        market: MarketType,
        mode: ModeType,
    ) -> CcxtClient:
        client = self._registry.get((exchange_name, market, mode))
        if client is None:
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "SERVICE_NOT_ENABLED",
                    "service": f"ccxt/{exchange_name}/{market}/{mode}",
                },
            )
        return client


exchange_manager = ExchangeManager()
