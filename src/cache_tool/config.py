from pathlib import Path
from datetime import datetime, timezone


# 单次网络请求最大数量（硬编码，取交易所限制的最小公约数）
# 币安等主流交易所限制为 1500，不暴露给用户配置
MAX_PER_REQUEST = 1500

# 不同周期的分块窗口配置
# 分钟级 → 按月分块
# 小时级 → 按年分块
# 日线及以上 → 按10年分块（取整：10-19, 20-29, ...）
PARTITION_CONFIG: dict[str, str] = {
    # 分钟级
    "1m": "month",
    "5m": "month",
    "15m": "month",
    "30m": "month",
    # 小时级
    "1h": "year",
    "4h": "year",
    # 日线及以上
    "1d": "decade",
    "1w": "decade",
}


def get_partition_key(timestamp_ms: int, period: str) -> str:
    """
    根据时间戳和周期，返回分块的 key（用于文件名）

    示例:
        - month: "2023-01"
        - year: "2023"
        - decade: "2020s" (10-19年取整)
    """
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    window = PARTITION_CONFIG.get(period, "year")

    if window == "month":
        return f"{dt.year}-{dt.month:02d}"
    elif window == "year":
        return str(dt.year)
    elif window == "decade":
        # 10年取整: 2023 -> 2020s, 2030 -> 2030s
        decade_start = (dt.year // 10) * 10
        return f"{decade_start}s"
    else:
        return str(dt.year)


def get_data_dir(
    base_dir: Path,
    exchange: str,
    mode: str,  # "live" 或 "demo"
    market: str,  # "future" 或 "spot"
    symbol: str,
    period: str,
) -> Path:
    """
    获取数据目录路径

    结构: base_dir / exchange / mode / market / symbol / period
    示例: data/ohlcv/binance/live/future/BTC_USDT/15m/
    """
    safe_symbol = symbol.replace("/", "_").replace(":", "_")
    return base_dir / exchange / mode / market / safe_symbol / period


def period_to_ms(period: str) -> int:
    """将周期字符串转换为毫秒"""
    if period.endswith("m"):
        return int(period[:-1]) * 60 * 1000
    elif period.endswith("h"):
        return int(period[:-1]) * 3600 * 1000
    elif period.endswith("d"):
        return int(period[:-1]) * 24 * 3600 * 1000
    elif period.endswith("w"):
        return int(period[:-1]) * 7 * 24 * 3600 * 1000
    else:
        raise ValueError(f"Unsupported period: {period}")
