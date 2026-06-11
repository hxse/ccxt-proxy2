import polars as pl
from pathlib import Path
from filelock import FileLock
from typing import cast
from .config import get_partition_key, get_data_dir
from .log_manager import append_log
from .models import DataLocation


def read_ohlcv(
    base_dir: Path,
    loc: DataLocation,
    start_time: int | None = None,
    end_time: int | None = None,
    count: int | None = None,
) -> pl.DataFrame:
    """读取 OHLCV 数据，支持时间范围过滤"""
    data_dir = get_data_dir(
        base_dir, loc.exchange, loc.mode, loc.market, loc.symbol, loc.period
    )

    if not data_dir.exists():
        return pl.DataFrame()

    # 找到所有 parquet 文件
    parquet_files = sorted(data_dir.glob("*.parquet"))
    if not parquet_files:
        return pl.DataFrame()

    # 优化路径：从 start_time 命中起始分区后按顺序读取，读够 count 即停
    # 使用 Polars Lazy 做候选文件内裁剪（filter/limit 下推）
    if start_time is not None and count is not None and count > 0:
        start_key = get_partition_key(start_time, loc.period)
        stems = [f.stem for f in parquet_files]
        start_idx = next((i for i, stem in enumerate(stems) if stem >= start_key), None)
        if start_idx is None:
            return pl.DataFrame()

        candidates = parquet_files[start_idx:]
        if not candidates:
            return pl.DataFrame()

        lf = pl.scan_parquet([str(p) for p in candidates])
        lf = lf.filter(pl.col("time") >= start_time)
        if end_time is not None:
            lf = lf.filter(pl.col("time") <= end_time)

        # 排序后 limit，保证返回顺序和 count 语义
        lf = lf.sort("time").limit(count)
        return lf.collect()

    # 默认路径：读取并合并后按时间过滤
    lf = pl.scan_parquet([str(p) for p in parquet_files])
    if start_time is not None:
        lf = lf.filter(pl.col("time") >= start_time)
    if end_time is not None:
        lf = lf.filter(pl.col("time") <= end_time)
    lf = lf.sort("time")
    if count is not None and count > 0:
        lf = lf.limit(count)
    return lf.collect()


def save_ohlcv(
    base_dir: Path,
    loc: DataLocation,
    new_data: pl.DataFrame,
    append_log_entry: bool = True,
    log_source: str = "api",
) -> None:
    """保存 OHLCV 数据，按时间分块"""
    if new_data.is_empty():
        return

    data_dir = get_data_dir(
        base_dir, loc.exchange, loc.mode, loc.market, loc.symbol, loc.period
    )
    data_dir.mkdir(parents=True, exist_ok=True)

    # 按分块 key 分组
    new_data = new_data.with_columns(
        pl.col("time")
        .map_elements(lambda t: get_partition_key(t, loc.period), return_dtype=pl.Utf8)
        .alias("__partition__")
    )

    for (partition_key,), group in new_data.group_by("__partition__"):
        file_path = data_dir / f"{partition_key}.parquet"

        # 移除临时列
        group = group.drop("__partition__")

        # 合并已有数据
        if file_path.exists():
            existing = pl.read_parquet(file_path)
            group = pl.concat([existing, group])

        # 去重并排序（保留新数据，最后一根K线可能未走完）
        group = group.unique(subset=["time"], keep="last").sort("time")

        # 写入
        group.write_parquet(file_path)

    if append_log_entry:
        append_log(
            data_dir,
            data_start=cast(int, new_data["time"].min()),
            data_end=cast(int, new_data["time"].max()),
            count=len(new_data),
            source=log_source,
        )


def save_ohlcv_with_lock(
    base_dir: Path,
    loc: DataLocation,
    new_data: pl.DataFrame,
    append_log_entry: bool = True,
    log_source: str = "api",
) -> None:
    """带文件锁的保存，防止并发冲突"""
    data_dir = get_data_dir(
        base_dir, loc.exchange, loc.mode, loc.market, loc.symbol, loc.period
    )
    data_dir.mkdir(parents=True, exist_ok=True)

    lock_path = data_dir / ".lock"
    with FileLock(lock_path):
        save_ohlcv(
            base_dir,
            loc,
            new_data,
            append_log_entry=append_log_entry,
            log_source=log_source,
        )
