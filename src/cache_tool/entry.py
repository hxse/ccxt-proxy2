import polars as pl
from pathlib import Path
from typing import Protocol
from filelock import FileLock

from .config import get_data_dir, MAX_PER_REQUEST
from .storage import read_ohlcv, save_ohlcv
from .log_manager import compact_log
from .models import DataLocation


class FetchCallback(Protocol):
    """数据获取回调函数的类型定义"""

    def __call__(
        self,
        symbol: str,
        period: str,
        start_time: int | None,
        count: int,
        **kwargs,
    ) -> pl.DataFrame: ...


def get_ohlcv_with_cache(
    base_dir: Path,
    loc: DataLocation,
    start_time: int | None,
    count: int,
    fetch_callback: FetchCallback,
    fetch_callback_params: dict | None = None,
    enable_cache: bool = True,
) -> pl.DataFrame:
    """
    获取 OHLCV 数据（简化缓存算法）

    算法特点：
    - 只在起始时检查一次缓存
    - 之后连续网络请求
    - 不在中间检查缓存

    Args:
        base_dir: 数据根目录
        loc: 数据位置参数（exchange, mode, market, symbol, period）
        start_time: 起始时间戳（毫秒），None 表示获取最新数据
        count: 数据条数
        fetch_callback: 数据获取回调函数
        fetch_callback_params: 回调函数额外参数
        enable_cache: 是否启用缓存
    """
    if fetch_callback_params is None:
        fetch_callback_params = {}

    data_dir = get_data_dir(
        base_dir, loc.exchange, loc.mode, loc.market, loc.symbol, loc.period
    )
    lock_path = data_dir / ".lock"
    data_dir.mkdir(parents=True, exist_ok=True)

    with FileLock(lock_path):
        # 无起始时间：跳过缓存读取，只写入
        if start_time is None:
            new_data = fetch_callback(
                loc.symbol, loc.period, None, count, **fetch_callback_params
            )
            if enable_cache and not new_data.is_empty():
                save_ohlcv(base_dir, loc, new_data)
            return new_data

        # 先合并日志
        compact_log(data_dir)

        # 读取合并后的日志
        from .log_manager import read_log

        log_entries = read_log(data_dir)

        result = pl.DataFrame()
        current_time = start_time
        remaining_count = count

        # 步骤1：检查起始时间是否在缓存中
        cache_entry = None
        for entry in log_entries:
            if entry.data_start <= start_time <= entry.data_end:
                cache_entry = entry
                break

        if cache_entry is not None and enable_cache:
            # 从缓存读取起始段
            cached_data = read_ohlcv(base_dir, loc, start_time, cache_entry.data_end)
            result = cached_data
            current_time = cache_entry.data_end
            remaining_count = count - len(result)

        # 步骤2：连续网络请求（不再检查中间缓存）
        is_first_request = True
        while remaining_count > 0:
            # 只有第二轮开始才 +1 补偿首条重复
            # 第一轮不需要：少的 1 条会被后续补回，如果没后续则直接返回
            if is_first_request:
                batch_size = min(MAX_PER_REQUEST, remaining_count)
                is_first_request = False
            else:
                batch_size = min(MAX_PER_REQUEST, remaining_count + 1)

            new_data = fetch_callback(
                loc.symbol,
                loc.period,
                current_time,
                batch_size,
                **fetch_callback_params,
            )

            # 边界检查：网络返回空数据
            if new_data.is_empty():
                break

            # 合并数据（keep="last" 保留新数据）
            prev_len = len(result)
            if result.is_empty():
                result = new_data
            else:
                result = pl.concat([result, new_data])
                result = result.unique(subset=["time"], keep="last").sort("time")

            # 边界检查：去重后没有新数据（防止死循环）
            if len(result) == prev_len:
                break

            # 更新状态
            current_time = int(result["time"].max())  # type: ignore
            remaining_count = count - len(result)

            # 边界检查：网络返回不足
            if len(new_data) < batch_size:
                break

        # 截取到目标数量
        if len(result) > count:
            result = result.head(count)

        # 保存到缓存
        if enable_cache and not result.is_empty():
            save_ohlcv(base_dir, loc, result)

        return result
