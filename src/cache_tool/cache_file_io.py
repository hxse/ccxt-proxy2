import pandas as pd
from pathlib import Path
from .cache_utils import (
    convert_ms_timestamp_to_utc_datetime,
    format_timestamp,
    sanitize_symbol,
    get_chunk_slices,
)
from typing import List, Tuple


def read_cache_file(filepath: Path, file_type: str) -> pd.DataFrame:
    """
    根据文件类型读取缓存文件。
    """
    try:
        if file_type == "parquet":
            return pd.read_parquet(filepath)
        elif file_type == "csv":
            return pd.read_csv(filepath)
        else:
            raise ValueError(f"Unsupported file type for reading: {file_type}")
    except Exception as e:
        print(f"❌ 无法读取文件 {filepath.name} ({file_type}): {e}")
        return pd.DataFrame()


def write_cache_file(filepath: Path, data: pd.DataFrame, file_type: str) -> None:
    """
    根据文件类型写入缓存文件。
    """
    try:
        filepath.parent.mkdir(parents=True, exist_ok=True)  # 创建父目录
        if file_type == "parquet":
            print("写入缓存文件", filepath)
            data.to_parquet(filepath, index=False)
        elif file_type == "csv":
            print("写入缓存文件", filepath)
            data.to_csv(filepath, index=False)
        else:
            raise ValueError(f"Unsupported file type for writing: {file_type}")
    except Exception as e:
        print(f"❌ 无法写入文件 {filepath.name} ({file_type}): {e}")


def write_to_cache(
    symbol: str,
    period: str,
    data: pd.DataFrame,
    cache_dir: Path,
    cache_size: int,
    file_type: str = "parquet",
    reverse: bool = False,
) -> None:
    """
    将数据写入缓存，并根据 cache_size 分割成多个文件。
    可选择正向或反向写入。
    """
    if data.empty:
        return []

    if not cache_dir.exists():
        cache_dir.mkdir(parents=True)

    total_rows = len(data)
    slices = get_chunk_slices(total_rows, cache_size, reverse=reverse)
    print("cache slices", slices, total_rows, cache_size)

    files_arr = []
    for start, end in slices:
        chunk = data.iloc[start:end]

        chunk_start_ts = chunk.iloc[0, 0]
        chunk_end_ts = chunk.iloc[-1, 0]
        chunk_count = len(chunk)

        chunk_start_dt = convert_ms_timestamp_to_utc_datetime(chunk_start_ts)
        chunk_end_dt = convert_ms_timestamp_to_utc_datetime(chunk_end_ts)
        chunk_start_str = format_timestamp(chunk_start_dt)
        chunk_end_str = format_timestamp(chunk_end_dt)

        filename = f"{sanitize_symbol(symbol)} {period} {chunk_start_str} {chunk_end_str} {chunk_count:04d}.{file_type}"
        filepath = cache_dir / filename

        write_cache_file(filepath, chunk, file_type)

        files_arr.append(filepath)

    return files_arr
