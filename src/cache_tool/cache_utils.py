import polars as pl
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Any

time_format = "%Y%m%dT%H%M%SZ"


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
    return dt.strftime(time_format)


def parse_timestamp_string(ts_str: str) -> int:
    """
    解析 'YYYYMMDDTHHMMSSZ' 格式的字符串为毫秒级时间戳。
    """
    dt = datetime.strptime(ts_str, time_format)
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

    files_with_info = []
    for f in all_files:
        info = get_file_info(f.name)
        if (
            info
            and info["symbol"] == sanitize_symbol(symbol)
            and info["period"] == period
        ):
            files_with_info.append((f, info))

    sorted_files = sorted(files_with_info, key=lambda x: x[1]["start_time"])
    return [f for f, info in sorted_files]


def get_chunk_slices(
    total_rows: int, cache_size: int, reverse: bool = False
) -> List[Tuple[int, int]]:
    """
    计算正向或反向写入时每个文件块的切片范围。

    Args:
        total_rows (int): 总数据行数。
        cache_size (int): 每个缓存文件存储的数据行数。
        forward (bool): 写入方向。True 为正向，False 为反向。

    Returns:
        List[Tuple[int, int]]: 包含每个文件块切片范围 (start_index, end_index) 的列表。
    """
    if total_rows <= 0 or cache_size <= 0:
        return []

    if cache_size == 1:
        # 特殊处理 cache_size=1 的情况
        slices = [(i, i + 1) for i in range(total_rows)]
        # 对于正向和反向，切片相同，无需去除任何切片
        return slices

    if not reverse:
        step = cache_size - 1
        slices = [
            (start, min(start + cache_size, total_rows))
            for start in range(0, total_rows, step)
        ]
        # 去除最后一个长度为1的切片（当切片数大于1时）
        if len(slices) > 1 and slices[-1][1] - slices[-1][0] == 1:
            slices = slices[:-1]
        return slices
    else:
        step = cache_size - 1
        first_chunk_size = (total_rows - 1) % step + 1

        # 确定剩余切片的数量
        remaining_rows = total_rows - first_chunk_size
        num_remaining_chunks = remaining_rows // step
        if remaining_rows % step > 0:
            num_remaining_chunks += 1

        # 构建第一个切片
        first_slice = [(0, first_chunk_size)]

        # 构建剩余切片
        remaining_slices = [
            (
                first_chunk_size - 1 + i * step,
                min(first_chunk_size - 1 + i * step + cache_size, total_rows),
            )
            for i in range(num_remaining_chunks)
        ]

        # 合并切片
        slices = first_slice + remaining_slices

        # 去除第一个长度为1的切片（当切片数大于1时）
        if len(slices) > 1 and slices[0][1] - slices[0][0] == 1:
            slices = slices[1:]

        return slices


def group_continuous_files(sorted_files: List[Path]) -> List[List[Path]]:
    if not sorted_files:
        return []

    file_groups = []

    for file in sorted_files:
        info = get_file_info(file.name)
        if not info:
            continue

        is_new_group = False
        if not file_groups:
            is_new_group = True
        else:
            last_group = file_groups[-1]
            last_file_in_group = last_group[-1]
            last_info = get_file_info(last_file_in_group.name)

            if not last_info or last_info["end_time"] != info["start_time"]:
                is_new_group = True

        if is_new_group:
            file_groups.append([file])
        else:
            file_groups[-1].append(file)

    return file_groups


def find_consecutive_sequences(data: List[Any]) -> List[Tuple[Any, int, int]]:
    """
    找出列表中所有连续重复的元素及其索引。

    Args:
        data: 任何类型的列表。

    Returns:
        一个元组列表，每个元组包含 (元素值, 起始索引, 结束索引)。
        结束索引是序列中最后一个元素的下一个位置，类似切片或 range 的惯例。
    """
    if not data:
        return []

    results = []
    start_index = 0

    for i in range(1, len(data)):
        if data[i] != data[i - 1]:
            # 连续序列中断，记录上一个序列。结束索引为 i。
            if i > start_index:
                results.append((data[start_index], start_index, i))
            start_index = i

    # 记录最后一个序列。结束索引为 len(data)。
    if len(data) > start_index:
        results.append((data[start_index], start_index, len(data)))

    return results


def find_cache_size_sequences(
    all_files: List[Any], cache_size: int
) -> List[Tuple[int, int, int]]:
    """
    从文件信息中提取 'count'，找出连续重复的行数序列，并筛选出等于 cache_size 的序列。

    Args:
        all_files (List[Any]): 文件信息对象的列表，每个对象都有 'name' 属性。
        cache_size (int): 要匹配的缓存大小。

    Returns:
        List[Tuple[int, int, int]]: 筛选后的 cache_size 序列。
    """
    # 从 all_files 中提取每个文件的 count 信息
    counts = []
    for file in all_files:
        info = get_file_info(file.name)
        if info:
            counts.append(info["count"])

    # 调用 find_consecutive_sequences 函数来找出所有连续重复的行数序列
    all_sequences = find_consecutive_sequences(counts)

    # 从这些序列中，筛选出那些行数等于 cache_size 的序列
    cache_size_sequences = [seq for seq in all_sequences if seq[0] == cache_size]

    return cache_size_sequences


def find_max_diff_sequence(
    sequences: List[Tuple[Any, int, int]],
) -> Tuple[Any, int, int] | None:
    """
    找出列表中 end - start 差值最大的元组。

    Args:
        sequences: 一个元组列表，每个元组的格式为 (值, 起始索引, 结束索引)。

    Returns:
        差值最大的元组。如果列表为空，则返回 None。
    """
    if not sequences:
        return None

    # 使用 max 函数和生成器表达式，以 end - start 作为比较依据
    return max(sequences, key=lambda seq: seq[2] - seq[1])
