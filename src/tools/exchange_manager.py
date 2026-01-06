"""交易所实例管理器"""

from typing import Any
from fastapi import HTTPException
from src.types import ExchangeName, MarketType, ModeType, ExchangeWhitelistItem
from src.tools.exchange import get_binance_exchange, get_kraken_exchange


class ExchangeManager:
    """
    交易所实例管理器

    负责根据配置白名单初始化和管理 CCXT 交易所实例
    使用单例模式，全局只有一个实例
    """

    _instance: "ExchangeManager | None" = None

    def __new__(cls) -> "ExchangeManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        # 防止重复初始化
        if self._initialized:
            return
        self._initialized = True

        # 交易所实例注册表
        # key: (exchange, market, mode) -> value: ccxt exchange instance
        self._registry: dict[tuple[str, str, str], Any] = {}

        # 白名单配置
        self._whitelist: list[ExchangeWhitelistItem] = []

    def init_from_config(self, config: dict) -> None:
        """
        根据配置文件白名单初始化交易所实例
        此方法应在应用启动时调用一次
        """
        whitelist_raw = config.get("exchange_whitelist", [])
        self._whitelist = [ExchangeWhitelistItem(**item) for item in whitelist_raw]

        for item in self._whitelist:
            key = (item.exchange, item.market, item.mode)

            if item.exchange == "binance":
                self._registry[key] = get_binance_exchange(
                    config, market=item.market, mode=item.mode
                )
            elif item.exchange == "kraken":
                self._registry[key] = get_kraken_exchange(
                    config, market=item.market, mode=item.mode
                )

            print(
                f"[ExchangeManager] 已初始化: {item.exchange}/{item.market}/{item.mode}"
            )

    def get(
        self,
        exchange_name: ExchangeName,
        market: MarketType,
        mode: ModeType,
    ) -> Any:
        """
        获取交易所实例

        参数:
            exchange_name: 交易所名称 (binance/kraken)
            market: 市场类型 (future/spot)
            mode: 模式 (sandbox/live)

        返回:
            ccxt 交易所实例

        异常:
            HTTPException 503: 请求的交易所组合未在白名单中启用
        """
        key = (exchange_name, market, mode)

        instance = self._registry.get(key)

        if instance is None:
            raise HTTPException(
                status_code=503,
                detail=f"交易所实例未启用: {exchange_name}/{market}/{mode}，请联系后端管理员在 config.json 的 exchange_whitelist 中添加",
            )

        return instance

    def is_enabled(
        self,
        exchange_name: ExchangeName,
        market: MarketType,
        mode: ModeType,
    ) -> bool:
        """检查指定交易所组合是否已启用"""
        key = (exchange_name, market, mode)
        return key in self._registry


# 全局单例，供外部导入使用
exchange_manager = ExchangeManager()
