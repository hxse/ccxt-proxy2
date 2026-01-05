import polars as pl
from pathlib import Path
from typing import Callable

from .cache_fetcher import mock_fetch_ohlcv, fetch_ohlcv
from .cache_write_overlap_handler import handle_cache_write
from .cache_read_chunk import get_next_continuous_cache_chunk
from .cache_data_processor import merge_with_deduplication

from .cache_consolidator import consolidate_cache, check_for_overlaps
from .cache_utils import (
    get_chunk_slices,
    format_timestamp,
    convert_ms_timestamp_to_utc_datetime,
)
from filelock import Timeout, FileLock


def get_ohlcv_with_cache_lock(*args, **kargs):
    cache_dir = kargs.get("cache_dir") or (args[4] if len(args) > 4 else None)

    lock = FileLock(f"{cache_dir}/cache.lock")
    with lock:
        return get_ohlcv_with_cache(*args, **kargs)


def get_ohlcv_with_cache(
    symbol: str,
    period: str,
    start_time: int | None,
    count: int,
    cache_dir: Path,
    cache_size: int,
    page_size: int,
    enable_cache: bool = True,
    file_type: str = "parquet",
    fetch_callback: Callable = mock_fetch_ohlcv,
    fetch_callback_params: dict = {},
    enable_consolidate: bool = True,
) -> pl.DataFrame:
    """
    根据新的统一逻辑获取K线数据，支持缓存。
    start_time: 如果为None, 不会读取缓存, 但是会写入缓存=

    """
    cache_dir = Path(cache_dir)
    file_type = file_type = file_type.lstrip(".")

    fetched_data = pl.DataFrame()
    current_time = start_time

    print(
        "start_time",
        start_time,
        "count",
        count,
        "page_size",
        page_size,
        "cache_size",
        cache_size,
    )

    if count == 0:
        print("ℹ️ count为0, 返回空dataframe")
        return fetched_data

    if start_time is None:
        print(f"ℹ️ start_time为None, 开始请求新数据。{count}")

        new_data = fetch_callback(
            symbol, period, current_time, count, **fetch_callback_params
        )

        if enable_cache:
            print("ℹ️ start_time为None, 只写入缓存, 不读取缓存")
            handle_cache_write(
                symbol, period, new_data, cache_dir, cache_size, file_type
            )
        return new_data

    chunk_slices = get_chunk_slices(count, page_size)

    for slice in chunk_slices:
        start_slice, end_slice = slice
        current_count = end_slice - start_slice
        need_count = current_count

        print(
            f"\n🟡 目标数据量: {count}，已获取: {len(fetched_data)} pages索引: {start_slice}-{end_slice}"
        )

        # 1. 优先从缓存获取数据
        cached_chunk = pl.DataFrame()
        if enable_cache and current_time is not None:
            # 找到从 current_time 开始的连续缓存数据块
            cached_chunk = get_next_continuous_cache_chunk(
                cache_dir,
                symbol,
                period,
                current_time,
                current_count,
                file_type,
            )

        # 确保 current_time 不为 None
        if current_time is None:
            # This should not happen if logic is correct, but satisfies type checker
            raise ValueError("Unexpected None for current_time")

        _current_time = format_timestamp(
            convert_ms_timestamp_to_utc_datetime(current_time)
        )

        if not cached_chunk.is_empty():
            print(f"✅ 缓存命中，已加载 {len(cached_chunk)} 条数据。{_current_time}")
            fetched_data = merge_with_deduplication(fetched_data, cached_chunk)

            need_count = current_count - len(cached_chunk)
            need_count = 0 if need_count <= 0 else need_count + 1
            current_time = fetched_data.tail(1)["time"].item()

        if need_count > 0:
            print(f"ℹ️ 需要 {need_count} 条数据，开始请求新数据。{_current_time}")

            new_data = fetch_callback(
                symbol, period, current_time, need_count, **fetch_callback_params
            )

            if new_data.is_empty():
                print("❌ 数据源返回空，提前停止请求。")
                break

            fetched_data = merge_with_deduplication(fetched_data, new_data)
            current_time = fetched_data.tail(1)["time"].item()

            # 4. 如果启用缓存，将新数据写入缓存
            if enable_cache:
                handle_cache_write(
                    symbol, period, new_data, cache_dir, cache_size, file_type
                )

            if len(new_data) < need_count:
                print("❌ 请求数组不足，提前停止请求。")
                break

    if enable_consolidate:
        check_for_overlaps(cache_dir, cache_size, symbol, period, file_type)
        consolidate_cache(cache_dir, cache_size, symbol, period, file_type, start_time)

    return fetched_data
