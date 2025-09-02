import pandas as pd
from pathlib import Path
from typing import Callable

from .cache_fetcher import mock_fetch_ohlcv, fetch_ohlcv
from .cache_core import (
    handle_cache_write,
)
from .cache_read_chunk import get_next_continuous_cache_chunk
from .cache_data_processor import merge_with_deduplication

from .cache_consolidator import consolidate_cache


def get_ohlcv_with_cache(
    symbol: str,
    period: str,
    start_time: int,
    count: int,
    cache_dir: Path,
    cache_size: int,
    page_size: int,
    enable_cache: bool = True,
    file_type: str = "parquet",
    fetch_callback: Callable = mock_fetch_ohlcv,
    fetch_callback_params: dict = {},
) -> pd.DataFrame:
    """
    根据新的统一逻辑获取K线数据，支持缓存。
    """
    cache_dir = Path(cache_dir)

    fetched_data = pd.DataFrame()
    current_time = start_time
    remaining_count = count

    while remaining_count > 0:
        print(
            f"\n🟡 目标数据量: {count}，已获取: {len(fetched_data)}，待获取: {remaining_count}"
        )

        # 1. 优先从缓存获取数据
        cached_chunk = pd.DataFrame()
        if enable_cache and current_time is not None:
            # 找到从 current_time 开始的连续缓存数据块
            cached_chunk = get_next_continuous_cache_chunk(
                cache_dir, symbol, period, current_time, remaining_count, file_type
            )

        if not cached_chunk.empty and not (
            len(cached_chunk) == 1
            and len(fetched_data) > 0
            and cached_chunk.iloc[-1, 0] == fetched_data.iloc[-1, 0]
        ):
            print(f"✅ 缓存命中，已加载 {len(cached_chunk)} 条数据。")

            fetched_data = merge_with_deduplication(fetched_data, cached_chunk)

            remaining_count = count - len(fetched_data)
            print(len(fetched_data), remaining_count)
            if remaining_count <= 0:
                print("✅ 缓存完全命中，直接返回。")
                break

            current_time = fetched_data.iloc[-1, 0]
        else:
            # 2. 缓存不足或无缓存，发起网络请求
            print("ℹ️ 缓存不足或无缓存，开始请求新数据。")
            fetch_limit = min(remaining_count, page_size)

            # # 修复死循环问题：如果已获取数据，且最后一次请求的数据量可能因为重叠而不足，则多请求一条。
            if not fetched_data.empty and fetch_limit < page_size:
                fetch_limit += 1
                print("预防死循环", fetch_limit)

            new_data = fetch_callback(
                symbol, period, current_time, fetch_limit, **fetch_callback_params
            )
            print(f"fetch数据 {len(new_data)}")

            if new_data.empty:
                print("❌ 数据源返回空，停止请求。")
                break

            # 3. 将新数据与已获取数据合并，并处理重叠
            fetched_data = merge_with_deduplication(fetched_data, new_data)

            # 4. 如果启用缓存，将新数据写入缓存
            if enable_cache:
                handle_cache_write(
                    symbol, period, new_data, cache_dir, cache_size, file_type
                )

            if len(new_data) < fetch_limit:
                print("请求数组不足，停止请求。")
                break

            remaining_count = count - len(fetched_data)
            if remaining_count <= 0:
                print("✅ 数据请求完毕，返回结果。")
                break

            current_time = fetched_data.iloc[-1, 0]

    # 返回之前先清理与合并一下小文件
    consolidate_cache(cache_dir, cache_size, symbol, period, file_type)

    # 5. 返回最终数据，并确保数量正确
    return fetched_data.iloc[:count]
