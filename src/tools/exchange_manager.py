"""Long-lived CcxtClient registry."""

from fastapi import HTTPException
from loguru import logger

from src.base_types import ExchangeName, MarketType, ModeType
from src.cache_tool import DuckDbOhlcvCache
from src.tools.ccxt_client import CcxtClient
from src.tools.config_types import AppConfig
from src.tools.exchange import get_binance_exchange, get_kraken_exchange
from src.types import ExchangeWhitelistItem


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
        self._whitelist: list[ExchangeWhitelistItem] = []
        self._cache: DuckDbOhlcvCache | None = None

    def init_from_config(self, config: AppConfig) -> None:
        self._registry.clear()
        self._whitelist = [
            ExchangeWhitelistItem(**item.model_dump())
            for item in config.exchange_whitelist
        ]
        cache_config = config.ohlcv_cache
        self._cache = DuckDbOhlcvCache(
            cache_config.database_path,
            cache_config.max_rows_per_series,
            cache_config.max_rows_total,
        )
        if not self._whitelist:
            logger.warning("exchange whitelist is empty")
            return

        for item in self._whitelist:
            key = (item.exchange, item.market, item.mode)
            bound = logger.bind(
                exchange=item.exchange, market=item.market, mode=item.mode
            )
            bound.info("initializing CcxtClient")
            if item.exchange == "binance":
                exchange = get_binance_exchange(config, item.market, item.mode)
            elif item.exchange == "kraken":
                exchange = get_kraken_exchange(config, item.market, item.mode)
            else:
                raise ValueError(f"unsupported exchange: {item.exchange}")
            client = CcxtClient(
                exchange, item.exchange, item.market, item.mode, self._cache
            )
            client.load_markets()
            self._registry[key] = client
            bound.info("CcxtClient initialized")

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
                detail=(
                    "交易所实例未启用: "
                    f"{exchange_name}/{market}/{mode}，请在 exchange_whitelist 中添加"
                ),
            )
        return client

exchange_manager = ExchangeManager()
