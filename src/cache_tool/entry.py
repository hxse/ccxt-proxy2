import polars as pl
from pathlib import Path
from typing import Protocol, cast
from filelock import FileLock

from .config import get_data_dir, MAX_PER_REQUEST
from .storage import read_ohlcv, save_ohlcv
from .log_manager import append_log, compact_log, read_log
from .models import DataLocation, LogEntry


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


def _merge_ohlcv(existing: pl.DataFrame, new: pl.DataFrame) -> pl.DataFrame:
    """按 time 合并并去重，保留新数据。"""
    if existing.is_empty():
        return new.sort("time")
    if new.is_empty():
        return existing.sort("time")
    return pl.concat([existing, new]).unique(subset=["time"], keep="last").sort("time")


def _trim_cache_tail(df: pl.DataFrame) -> pl.DataFrame:
    """缓存默认去掉最后一根 K 线。"""
    if df.is_empty() or len(df) <= 1:
        return pl.DataFrame()
    return df.head(len(df) - 1)


def _find_cache_span_end(log_entries: list[LogEntry], start_time: int) -> int | None:
    """从 start_time 命中的日志段开始，找到可连续到达的末尾时间。"""
    start_idx = next(
        (
            i
            for i, entry in enumerate(log_entries)
            if entry.data_start <= start_time <= entry.data_end
        ),
        None,
    )
    if start_idx is None:
        return None

    current_end = log_entries[start_idx].data_end
    # read_log 已排序；这里按首尾衔接/重叠向后扩展
    for entry in log_entries[start_idx + 1 :]:
        if entry.data_start <= current_end:
            current_end = max(current_end, entry.data_end)
            continue
        if entry.data_start == current_end:
            current_end = max(current_end, entry.data_end)
            continue
        break

    return current_end


def get_ohlcv_with_cache(
    base_dir: Path,
    loc: DataLocation,
    start_time: int | None,
    count: int,
    fetch_callback: FetchCallback,
    fetch_callback_params: dict[str, object] | None = None,
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
    if count <= 0:
        return pl.DataFrame()

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
                cache_data = _trim_cache_tail(new_data)
                if not cache_data.is_empty():
                    save_ohlcv(base_dir, loc, cache_data, append_log_entry=False)
                    append_log(
                        data_dir,
                        data_start=cast(int, cache_data["time"].min()),
                        data_end=cast(int, cache_data["time"].max()),
                        count=len(cache_data),
                        source="api",
                    )
            return new_data

        # 先合并日志
        compact_log(data_dir)

        # 读取合并后的日志（全序）
        log_entries = read_log(data_dir)

        result = pl.DataFrame()
        network_data = pl.DataFrame()
        current_time = start_time
        remaining_count = count

        # 步骤1：缓存阶段（从 start_time 命中缓存段，顺序读取到连续末尾）
        if enable_cache:
            cache_end = _find_cache_span_end(log_entries, start_time)
            if cache_end is not None:
                cached_data = read_ohlcv(
                    base_dir=base_dir,
                    loc=loc,
                    start_time=start_time,
                    end_time=cache_end,
                    count=count,
                )
                result = cached_data.sort("time")
                if not result.is_empty():
                    current_time = cast(int, result["time"].max())
                    remaining_count = count - len(result)

        # 步骤2：连续网络请求（不再检查中间缓存）
        while remaining_count > 0:
            has_existing_data = not result.is_empty()
            batch_size = min(
                MAX_PER_REQUEST,
                remaining_count + (1 if has_existing_data else 0),
            )

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
            result = _merge_ohlcv(result, new_data)
            network_data = _merge_ohlcv(network_data, new_data)

            # 边界检查：去重后没有新数据（防止死循环）
            if len(result) == prev_len:
                break

            # 更新状态
            current_time = cast(int, result["time"].max())
            remaining_count = count - len(result)

            # 边界检查：网络返回不足
            if len(new_data) < batch_size:
                break

        # 截取到目标数量
        if len(result) > count:
            result = result.head(count)

        # 保存到缓存：只落盘网络增量，且默认去掉最后一根
        if enable_cache and not network_data.is_empty():
            cache_data = _trim_cache_tail(network_data)
            if not cache_data.is_empty():
                save_ohlcv(base_dir, loc, cache_data, append_log_entry=False)
                append_log(
                    data_dir,
                    data_start=cast(int, cache_data["time"].min()),
                    data_end=cast(int, cache_data["time"].max()),
                    count=len(cache_data),
                    source="api",
                )

        return result
