import pandas as pd
from pathlib import Path
from datetime import datetime, timezone


def convert_ms_timestamp_to_utc_datetime(ms_timestamp: int) -> datetime:
    """
    将毫秒级时间戳转换为带有 UTC 时区信息的 datetime 对象。
    """
    return datetime.fromtimestamp(ms_timestamp / 1000, tz=timezone.utc)


def format_timestamp(dt: datetime) -> str:
    """
    格式化 datetime 对象为 'YYYYMMDDTHHMMSSZ' UTC 格式。
    """
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y%m%dT%H%M%SZ")


def parse_timestamp_string(ts_str: str) -> int:
    """
    解析 'YYYYMMDDTHHMMSSZ' 格式的字符串为毫秒级时间戳。
    """
    dt = datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ")
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def period_to_ms(period: str) -> int:
    """
    将 K 线周期字符串转换为毫秒。
    支持 '1m', '5m', '1h', '1d' 等。
    """
    if period.endswith("m"):
        return int(period[:-1]) * 60 * 1000
    elif period.endswith("h"):
        return int(period[:-1]) * 3600 * 1000
    elif period.endswith("d"):
        return int(period[:-1]) * 24 * 3600 * 1000
    else:
        raise ValueError(f"Unsupported period format: {period}")


def get_file_info(filename: str) -> dict | None:
    """解析文件名，提取 start_time, end_time, count"""
    try:
        parts = Path(filename).stem.split(" ")
        symbol, period, start_str, end_str, count = (
            parts[0],
            parts[1],
            parts[2],
            parts[3],
            int(parts[4]),
        )
        return {
            "symbol": symbol,
            "period": period,
            "count": count,
            "start_time": parse_timestamp_string(start_str),
            "end_time": parse_timestamp_string(end_str),
        }
    except (ValueError, IndexError):
        return None


def sanitize_symbol(symbol: str) -> str:
    """
    清理交易对符号中的特殊字符，例如将 'BTC/USDT' 转换为 'BTC_USDT'。
    """
    # 构建一个转换表，将 '/' 和 ':' 映射为 '_'
    # str.maketrans() 创建了一个映射表，将字符 ':', '/' 映射到 '_'
    translator = str.maketrans("/:", "__")
    return symbol.translate(translator)


def get_sorted_cache_files(
    cache_dir: Path, symbol: str, period: str, file_type: str = "parquet"
) -> list[Path]:
    """
    根据 symbol 和 period 获取并排序缓存目录下特定 symbol 和 period 的文件名。
    """
    if not cache_dir.exists():
        return []

    all_files = [f for f in cache_dir.iterdir() if f.suffix == f".{file_type}"]

    valid_and_matched_files = [
        f
        for f in all_files
        if (info := get_file_info(f.name))
        and info["symbol"] == sanitize_symbol(symbol)
        and info["period"] == period
    ]

    return sorted(
        valid_and_matched_files, key=lambda f: get_file_info(f.name)["start_time"]
    )
