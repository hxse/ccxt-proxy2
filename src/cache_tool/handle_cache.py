import pandas as pd
import numpy as np
from pathlib import Path
import shutil
from datetime import datetime, timezone
from typing import Callable


# ==================== 核心逻辑函数 ====================


def mock_fetch_ohlcv(
    symbol: str, period: str, start_time: int, count: int, exchange: any = None
) -> pd.DataFrame:
    """
    模拟生成 OHLCV 数据的 Pandas DataFrame。
    列: time, open, high, low, close, volume
    """
    time_step_ms = _period_to_ms(period)
    data = [
        [start_time + i * time_step_ms, 100 + i, 105 + i, 98 + i, 102 + i, 1000 + i]
        for i in range(count)
    ]
    return pd.DataFrame(
        data, columns=["time", "open", "high", "low", "close", "volume"]
    )


def fetch_ohlcv(
    symbol: str, period: str, start_time: int, count: int, exchange: any = None
) -> pd.DataFrame:
    if not exchange:
        return pd.DataFrame()
    data = exchange.fetchOHLCV(symbol, period, start_time, count)
    return pd.DataFrame(
        data, columns=["time", "open", "high", "low", "close", "volume"]
    )


def _handle_cache_write(
    symbol: str,
    period: str,
    new_data: pd.DataFrame,
    cache_dir: Path,
    cache_size: int,
    file_type: str = ".parquet",
) -> None:
    """
    处理新数据写入缓存的逻辑，包括处理重叠和覆盖情况。

    此函数采用“先处理后写入”的策略，通过记录重叠边界，最后一次性对新数据进行切片并写入。
    """
    if new_data.empty:
        return

    new_data_start = new_data.iloc[0, 0]
    new_data_end = new_data.iloc[-1, 0]

    sorted_cache_files = _get_sorted_cache_files(cache_dir, symbol, period)

    start_time_to_write = new_data_start
    end_time_to_write = new_data_end

    files_to_delete = []

    for f in sorted_cache_files:
        info = _get_file_info(f.name)
        if not info:
            continue
        old_data_start = info["start_time"]
        old_data_end = info["end_time"]

        # 1. 新数据完全在旧数据内部（完全被缓存） -> 无需写入，直接返回
        if new_data_start >= old_data_start and new_data_end <= old_data_end:
            print(
                f"✅ 新数据 ({new_data_start}-{new_data_end}) 已被缓存文件 ({old_data_start}-{old_data_end}) 完全覆盖，无需写入。"
            )
            return

        # 2. 新数据完全覆盖旧数据 -> 标记旧文件为删除
        elif new_data_start <= old_data_start and new_data_end >= old_data_end:
            print(
                f"🔄 新数据 ({new_data_start}-{new_data_end}) 完全覆盖旧缓存文件 ({old_data_start}-{old_data_end})，标记为删除。"
            )
            files_to_delete.append(f)

        # 3. 新数据在旧数据之前且有重叠 -> 调整写入的结束时间，使其不包含重叠部分
        elif new_data_end > old_data_start and new_data_start < old_data_start:
            end_time_to_write = min(end_time_to_write, old_data_start)
            print(
                f"⚠️ 新数据 ({new_data_start}-{new_data_end}) 与旧缓存 ({old_data_start}-{old_data_end}) 重叠，调整写入结束时间。"
            )

        # 4. 新数据在旧数据之后且有重叠 -> 调整写入的起始时间，使其不包含重叠部分
        elif new_data_start < old_data_end and new_data_end > old_data_end:
            start_time_to_write = max(start_time_to_write, old_data_end)
            print(
                f"⚠️ 新数据 ({new_data_start}-{new_data_end}) 与旧缓存 ({old_data_start}-{old_data_end}) 重叠，调整写入起始时间。"
            )

    # 执行删除操作
    if files_to_delete:
        for f in files_to_delete:
            if f.exists():
                print(f"🗑️ 删除旧缓存文件: {f.name}")
                f.unlink()

    # 最后根据调整后的时间范围进行切片和写入
    data_to_write = new_data[
        (new_data["time"] >= start_time_to_write)
        & (new_data["time"] <= end_time_to_write)
    ]

    if not data_to_write.empty:
        _write_to_cache(symbol, period, data_to_write, cache_dir, cache_size, file_type)
    else:
        print("❌ 经过处理，没有数据需要写入缓存。")


def get_ohlcv_with_cache(
    symbol: str,
    period: str,
    start_time: int,
    count: int,
    cache_dir: Path,
    cache_size: int,
    page_size: int,
    enable_cache: bool = True,
    file_type: str = ".parquet",
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
        # 1. 优先从缓存获取数据
        cached_chunk = pd.DataFrame()
        if enable_cache and current_time is not None:
            # 找到从 current_time 开始的连续缓存数据块
            cached_chunk = _get_next_continuous_cache_chunk(
                cache_dir, symbol, period, current_time, remaining_count, file_type
            )

        if not cached_chunk.empty and not (
            len(cached_chunk) == 1
            and len(fetched_data) > 0
            and cached_chunk.iloc[-1, 0] == fetched_data.iloc[-1, 0]
        ):
            print(f"✅ 缓存命中，已加载 {len(cached_chunk)} 条数据。")

            fetched_data = _merge_with_deduplication(fetched_data, cached_chunk)

            remaining_count = count - len(fetched_data)
            print(len(fetched_data), remaining_count)
            if remaining_count <= 0:
                print("✅ 缓存完全命中，直接返回。")
                break

            current_time = fetched_data.iloc[-1, 0]
        else:
            # 2. 缓存不足或无缓存，发起网络请求
            print("⚠️ 缓存不足或无缓存，开始请求新数据。")
            fetch_limit = min(remaining_count, page_size)

            # 修复死循环问题：如果已获取数据，且最后一次请求的数据量可能因为重叠而不足，则多请求一条。
            if not fetched_data.empty:
                fetch_limit += 1

            new_data = fetch_callback(
                symbol, period, current_time, fetch_limit, **fetch_callback_params
            )

            if new_data.empty:
                print("❌ 数据源返回空，停止请求。")
                break

            # 3. 将新数据与已获取数据合并，并处理重叠
            fetched_data = _merge_with_deduplication(fetched_data, new_data)

            # 4. 如果启用缓存，将新数据写入缓存
            if enable_cache:
                _handle_cache_write(
                    symbol, period, new_data, cache_dir, cache_size, file_type
                )

            if len(new_data) < fetch_limit:
                print("请求数组不足，停止请求。")
                break

            remaining_count = count - len(fetched_data)
            if remaining_count <= 0:
                print("✅ 数据请求完毕，返回结果。")
                break

            current_time = fetched_data.iloc[-1, 0] + 1000

    # 返回之前先清理与合并一下小文件
    consolidate_cache(cache_dir, cache_size, symbol, period, file_type)

    # 5. 返回最终数据，并确保数量正确
    return fetched_data.iloc[:count]


def _merge_with_deduplication(
    cached_data: pd.DataFrame, chunk: pd.DataFrame
) -> pd.DataFrame:
    """
    将新数据与现有数据合并，并处理首尾重叠的数据点。

    Args:
        cached_data (pd.DataFrame): 已有的缓存数据。
        chunk (pd.DataFrame): 新加载的数据块。

    Returns:
        pd.DataFrame: 合并并去重后的 DataFrame。
    """
    if cached_data.empty:
        return chunk

    if chunk.empty:
        return cached_data

    # 检查首尾是否相等
    if chunk.iloc[0, 0] == cached_data.iloc[-1, 0]:
        # 用新数据的第一行替换旧数据的最后一行
        cached_data.iloc[-1] = chunk.iloc[0]
        # 移除新数据的第一行，准备合并
        chunk = chunk.iloc[1:]

    # 如果新数据块处理后不为空，则进行合并
    if not chunk.empty:
        return pd.concat([cached_data, chunk], ignore_index=True)
    else:
        return cached_data


def _get_next_continuous_cache_chunk(
    cache_dir: Path,
    symbol: str,
    period: str,
    start_time: int,
    target_count: int,
    file_type: str = ".parquet",
) -> pd.DataFrame:
    """
    寻找并加载从指定时间开始的连续缓存数据块。
    """
    cached_data = pd.DataFrame()

    sorted_files = _get_sorted_cache_files(cache_dir, symbol, period, file_type)

    start_file = None

    # 遍历一次所有文件，根据优先级找到最佳匹配
    for f in sorted_files:
        info = _get_file_info(f.name)
        if not info:
            continue

        if info["start_time"] == start_time:
            # 优先级1：精确匹配，请求的起始时间等于文件的起始时间
            start_file = f
            print("优先级1")
            break
        elif info["start_time"] < start_time < info["end_time"]:
            # 优先级2：请求的起始时间位于文件时间区间内
            start_file = f
            print("优先级2")
            break
        elif info["start_time"] <= start_time <= info["end_time"]:
            # 优先级3：默认匹配，如果存在多个时间区间重叠的文件，找到第一个匹配的
            start_file = f
            print("优先级3")
            break

    if start_file is None:
        return cached_data

    # 从匹配的文件开始加载数据
    first_chunk = _read_cache_file(start_file, file_type)

    # 精确切片：找到 start_time 对应的行并切片
    try:
        start_index = first_chunk.index[first_chunk["time"] == start_time][0]
        cached_data = first_chunk.iloc[start_index:]
    except IndexError:
        # 如果找不到精确匹配的索引，则返回空 DataFrame
        return pd.DataFrame()

    # 将当前时间更新为已加载数据的最后一个时间点，用于后续的连续性检查
    current_time = cached_data.iloc[-1, 0]

    # 继续加载后续连续的缓存文件
    start_file_index = sorted_files.index(start_file)
    for i in range(start_file_index + 1, len(sorted_files)):
        filepath = sorted_files[i]
        info = _get_file_info(filepath.name)

        # 检查是否连续：下一个文件的起始时间是否等于当前已加载数据的结束时间
        if info["start_time"] == current_time:
            chunk = _read_cache_file(filepath, file_type)

            cached_data = _merge_with_deduplication(cached_data, chunk)

            # 更新当前时间为新合并数据的最后一个时间点
            current_time = cached_data.iloc[-1, 0]

            if len(cached_data) >= target_count:
                # 如果已加载的数据量达到目标数量，进行切片并停止加载
                cached_data = cached_data.iloc[:target_count]
                break
        else:
            # 文件不连续，停止查找
            break

    return cached_data


def _read_cache_file(filepath: Path, file_type: str) -> pd.DataFrame:
    """
    根据文件类型读取缓存文件。
    """
    try:
        if file_type == ".parquet":
            return pd.read_parquet(filepath)
        elif file_type == ".csv":
            return pd.read_csv(filepath)
        else:
            raise ValueError(f"Unsupported file type for reading: {file_type}")
    except Exception as e:
        print(f"❌ 无法读取文件 {filepath.name} ({file_type}): {e}")
        return pd.DataFrame()


def _write_cache_file(filepath: Path, data: pd.DataFrame, file_type: str) -> None:
    """
    根据文件类型写入缓存文件。
    """
    try:
        filepath.parent.mkdir(parents=True, exist_ok=True)  # 创建父目录
        if file_type == ".parquet":
            print("写入缓存文件", filepath)
            data.to_parquet(filepath, index=False)
        elif file_type == ".csv":
            print("写入缓存文件", filepath)
            data.to_csv(filepath, index=False)
        else:
            raise ValueError(f"Unsupported file type for writing: {file_type}")
    except Exception as e:
        print(f"❌ 无法写入文件 {filepath.name} ({file_type}): {e}")


def _convert_ms_timestamp_to_utc_datetime(ms_timestamp: int) -> datetime:
    """
    将毫秒级时间戳转换为带有 UTC 时区信息的 datetime 对象。
    """
    return datetime.fromtimestamp(ms_timestamp / 1000, tz=timezone.utc)


def _format_timestamp(dt: datetime) -> str:
    """
    格式化 datetime 对象为 'YYYYMMDDTHHMMSSZ' UTC 格式。
    """
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y%m%dT%H%M%SZ")


def _period_to_ms(period: str) -> int:
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


def _parse_timestamp_string(ts_str: str) -> int:
    """
    解析 'YYYYMMDDTHHMMSSZ' 格式的字符串为毫秒级时间戳。
    """
    dt = datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ")
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def _get_file_info(filename: str) -> dict | None:
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
            "start_time": _parse_timestamp_string(start_str),
            "end_time": _parse_timestamp_string(end_str),
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


def _get_sorted_cache_files(
    cache_dir: Path, symbol: str, period: str, file_type: str = ".parquet"
) -> list[Path]:
    """
    根据 symbol 和 period 获取并排序缓存目录下特定 symbol 和 period 的文件名。
    """
    if not cache_dir.exists():
        return []

    all_files = [f for f in cache_dir.iterdir() if f.suffix == file_type]

    valid_and_matched_files = [
        f
        for f in all_files
        if (info := _get_file_info(f.name))
        and info["symbol"] == sanitize_symbol(symbol)
        and info["period"] == period
    ]

    return sorted(
        valid_and_matched_files, key=lambda f: _get_file_info(f.name)["start_time"]
    )


def _write_to_cache(
    symbol: str,
    period: str,
    data: pd.DataFrame,
    cache_dir: Path,
    cache_size: int,
    file_type: str = ".parquet",
) -> None:
    """
    将数据写入缓存，并根据 cache_size 分割成多个文件。

    Args:
        symbol (str): 交易对。
        period (str): K线周期。
        data (pd.DataFrame): 待写入的 OHLCV 数据。
        cache_dir (Path): 缓存目录路径。
        cache_size (int): 每个缓存文件存储的数据行数。
    """
    if data.empty:
        return

    if not cache_dir.exists():
        cache_dir.mkdir(parents=True)

    start_index = 0
    total_rows = len(data)

    while start_index < total_rows:
        end_index = min(start_index + cache_size, total_rows)
        chunk = data.iloc[start_index:end_index]

        if chunk.empty:
            break

        chunk_start_ts = chunk.iloc[0, 0]
        chunk_end_ts = chunk.iloc[-1, 0]
        chunk_count = len(chunk)

        chunk_start_dt = _convert_ms_timestamp_to_utc_datetime(chunk_start_ts)
        chunk_end_dt = _convert_ms_timestamp_to_utc_datetime(chunk_end_ts)
        chunk_start_str = _format_timestamp(chunk_start_dt)
        chunk_end_str = _format_timestamp(chunk_end_dt)

        filename = f"{sanitize_symbol(symbol)} {period} {chunk_start_str} {chunk_end_str} {chunk_count:04d}{file_type}"
        filepath = cache_dir / filename

        _write_cache_file(filepath, chunk, file_type)
        print(f"  > 写入缓存文件: {filename}")

        start_index += cache_size


def consolidate_cache(
    cache_dir: Path,
    cache_size: int,
    symbol: str,
    period: str,
    file_type: str = ".parquet",
) -> None:
    """
    整理缓存目录中的文件。

    查找连续的、大小小于 cache_size 的缓存文件，将它们分组，然后合并后重新写入。
    """
    if not cache_dir.exists():
        print("❌ 缓存目录不存在，无需整理。")
        return

    sorted_cache_files = _get_sorted_cache_files(cache_dir, symbol, period, file_type)
    if not sorted_cache_files:
        print("✅ 缓存目录中没有需要整理的有效文件。")
        return

    # 使用二维列表来收集连续的文件块
    files_to_merge = []

    print(f"\n--- 开始分组 {symbol} {period} 的缓存文件 ---")

    current_group = []
    for i in range(len(sorted_cache_files)):
        current_file = sorted_cache_files[i]
        current_info = _get_file_info(current_file.name)
        if not current_info:
            continue

        # 只有当文件大小小于 cache_size 时才考虑将其加入待合并队列
        if current_info["count"] < cache_size:
            # 检查是否与前一个文件连续
            is_continuous = False
            if current_group:
                last_file_info = _get_file_info(current_group[-1].name)
                # 检查当前文件的开始时间是否与前一个文件的结束时间相等
                if (
                    last_file_info
                    and current_info["start_time"] == last_file_info["end_time"]
                ):
                    is_continuous = True

            if not current_group or is_continuous:
                current_group.append(current_file)
                print(f"✅ 将文件 {current_file.name} 添加到当前连续组。")
            else:
                # 遇到不连续的文件，保存当前组并开始新的组
                files_to_merge.append(current_group)
                current_group = [current_file]
                print(f"⚠️ 遇到不连续，新开一组，添加文件 {current_file.name}。")
        else:
            # 遇到大于或等于 cache_size 的文件，结束当前组
            if current_group:
                files_to_merge.append(current_group)
                current_group = []

    # 循环结束时，保存最后一个组
    if current_group:
        files_to_merge.append(current_group)

    print("\n--- 开始合并和重新写入缓存文件 ---")
    merged_count = 0

    for group in files_to_merge:
        # 只有当一个组包含多于一个文件时才进行合并
        if len(group) > 1:
            merged_count += 1
            print(f"--- 正在处理第 {merged_count} 个待合并文件块 ---")

            merged_data = pd.DataFrame()

            # 1. 加载并合并所有文件
            for f in group:
                try:
                    data_to_merge = _read_cache_file(f, file_type)
                    merged_data = _merge_with_deduplication(merged_data, data_to_merge)
                    print(f"📦 已加载并合并文件: {f.name}")
                except Exception as e:
                    print(f"❌ 无法读取文件 {f.name}: {e}")

            # 2. 删除旧文件
            for f in group:
                if f.exists():
                    print(f"🗑️ 删除旧缓存文件: {f.name}")
                    f.unlink()

            # 3. 写入新合并的数据
            _write_to_cache(
                symbol, period, merged_data, cache_dir, cache_size, file_type
            )

    if merged_count == 0:
        print("✅ 没有需要合并的文件块，缓存已是最优状态。")

    print("--- 缓存整理完成 ---")
