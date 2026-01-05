# 核心代码示例

## 目录结构

```python
# src/cache_tool/
#   __init__.py
#   config.py        # 分块配置
#   storage.py       # 数据读写
#   log_manager.py   # 日志管理
#   continuity.py    # 连续性验证
#   entry.py         # 主入口
```

---

## config.py - 分块配置

```python
from pathlib import Path

# 不同周期的分块窗口配置
PARTITION_CONFIG = {
    "1m":  "month",   # 分钟K线按月分块，避免单文件过大
    "5m":  "month",
    "15m": "year",
    "1h":  "year",
    "4h":  "year",
    "1d":  "decade",  # 日K线数据量小，可以按10年分块
}

def get_partition_key(timestamp_ms: int, period: str) -> str:
    """根据时间戳和周期，返回分块的 key（用于文件名）"""
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    
    window = PARTITION_CONFIG.get(period, "year")
    
    if window == "month":
        return f"{dt.year}-{dt.month:02d}"
    elif window == "year":
        return str(dt.year)
    elif window == "decade":
        return f"{(dt.year // 10) * 10}s"
    else:
        return str(dt.year)

def get_data_dir(base_dir: Path, symbol: str, period: str) -> Path:
    """获取数据目录路径"""
    safe_symbol = symbol.replace("/", "_").replace(":", "_")
    return base_dir / f"{safe_symbol}_{period}"
```

---

## storage.py - 数据读写

```python
import polars as pl
from pathlib import Path
from filelock import FileLock
from .config import get_partition_key, get_data_dir

def read_ohlcv(
    base_dir: Path,
    symbol: str,
    period: str,
    start_time: int | None = None,
    end_time: int | None = None,
) -> pl.DataFrame:
    """读取 OHLCV 数据，支持时间范围过滤"""
    data_dir = get_data_dir(base_dir, symbol, period)
    
    if not data_dir.exists():
        return pl.DataFrame()
    
    # 找到所有 parquet 文件
    parquet_files = sorted(data_dir.glob("*.parquet"))
    if not parquet_files:
        return pl.DataFrame()
    
    # 读取并合并
    dfs = [pl.read_parquet(f) for f in parquet_files]
    df = pl.concat(dfs).sort("time")
    
    # 过滤时间范围
    if start_time is not None:
        df = df.filter(pl.col("time") >= start_time)
    if end_time is not None:
        df = df.filter(pl.col("time") <= end_time)
    
    return df


def save_ohlcv(
    base_dir: Path,
    symbol: str,
    period: str,
    new_data: pl.DataFrame,
) -> None:
    """保存 OHLCV 数据，按时间分块"""
    if new_data.is_empty():
        return
    
    data_dir = get_data_dir(base_dir, symbol, period)
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # 按分块 key 分组
    new_data = new_data.with_columns(
        pl.col("time").map_elements(
            lambda t: get_partition_key(t, period),
            return_dtype=pl.Utf8
        ).alias("__partition__")
    )
    
    for partition_key, group in new_data.group_by("__partition__"):
        partition_key = partition_key[0]  # unpack tuple
        file_path = data_dir / f"{partition_key}.parquet"
        
        # 移除临时列
        group = group.drop("__partition__")
        
        # 合并已有数据
        if file_path.exists():
            existing = pl.read_parquet(file_path)
            group = pl.concat([existing, group])
        
        # 去重并排序
        group = group.unique(subset=["time"], keep="first").sort("time")
        
        # 写入
        group.write_parquet(file_path)


def save_ohlcv_with_lock(
    base_dir: Path,
    symbol: str,
    period: str,
    new_data: pl.DataFrame,
) -> None:
    """带文件锁的保存，防止并发冲突"""
    data_dir = get_data_dir(base_dir, symbol, period)
    data_dir.mkdir(parents=True, exist_ok=True)
    
    lock_path = data_dir / ".lock"
    with FileLock(lock_path):
        save_ohlcv(base_dir, symbol, period, new_data)
```

---

## log_manager.py - 日志管理

```python
import json
import polars as pl
from pathlib import Path
from datetime import datetime, timezone
from filelock import FileLock

def get_log_path(data_dir: Path) -> Path:
    return data_dir / "fetch_log.jsonl"


def append_log(
    data_dir: Path,
    data_start: int,
    data_end: int,
    count: int,
    source: str = "api",
) -> None:
    """追加一条获取日志"""
    log_path = get_log_path(data_dir)
    
    entry = {
        "fetch_time": datetime.now(timezone.utc).isoformat(),
        "data_start": data_start,
        "data_end": data_end,
        "count": count,
        "source": source,
    }
    
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def read_log(data_dir: Path) -> pl.DataFrame:
    """读取日志为 DataFrame"""
    log_path = get_log_path(data_dir)
    
    if not log_path.exists():
        return pl.DataFrame()
    
    entries = []
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                entries.append(json.loads(line))
    
    if not entries:
        return pl.DataFrame()
    
    return pl.DataFrame(entries).sort("data_start")


def compact_log(data_dir: Path) -> None:
    """合并首尾相连的日志条目，减少日志行数"""
    log = read_log(data_dir)
    
    if log.is_empty() or len(log) < 2:
        return
    
    # 标记可合并的行（当前行的 start == 上一行的 end）
    log = log.with_columns(
        (pl.col("data_start") == pl.col("data_end").shift(1)).alias("can_merge")
    )
    
    # 分组：每次 can_merge=False 开始新组
    log = log.with_columns(
        (~pl.col("can_merge").fill_null(False)).cum_sum().alias("group_id")
    )
    
    # 按组聚合
    compacted = log.group_by("group_id").agg([
        pl.col("data_start").min().alias("data_start"),
        pl.col("data_end").max().alias("data_end"),
        pl.col("count").sum().alias("count"),
        pl.col("fetch_time").first().alias("fetch_time"),
        pl.lit("compacted").alias("source"),
    ]).drop("group_id")
    
    # 重写日志文件
    log_path = get_log_path(data_dir)
    with open(log_path, "w", encoding="utf-8") as f:
        for row in compacted.iter_rows(named=True):
            f.write(json.dumps(row) + "\n")


def rebuild_log_from_data(data_dir: Path, period_ms: int) -> None:
    """从数据文件重建日志（用于日志丢失时恢复）"""
    parquet_files = sorted(data_dir.glob("*.parquet"))
    if not parquet_files:
        return
    
    dfs = [pl.read_parquet(f) for f in parquet_files]
    df = pl.concat(dfs).sort("time")
    
    if df.is_empty():
        return
    
    # 找出时间断裂点
    df = df.with_columns(
        (pl.col("time").diff() != period_ms).fill_null(True).alias("is_break")
    )
    
    # 每个断裂点开始新 batch
    df = df.with_columns(
        pl.col("is_break").cum_sum().alias("batch_id")
    )
    
    # 按 batch 聚合
    log = df.group_by("batch_id").agg([
        pl.col("time").min().alias("data_start"),
        pl.col("time").max().alias("data_end"),
        pl.len().alias("count"),
    ]).drop("batch_id")
    
    # 写入日志
    log_path = get_log_path(data_dir)
    with open(log_path, "w", encoding="utf-8") as f:
        for row in log.iter_rows(named=True):
            entry = {
                "fetch_time": datetime.now(timezone.utc).isoformat(),
                "data_start": row["data_start"],
                "data_end": row["data_end"],
                "count": row["count"],
                "source": "rebuilt",
            }
            f.write(json.dumps(entry) + "\n")
```

---

## continuity.py - 连续性验证

```python
import polars as pl
from pathlib import Path
from typing import List, Dict
from .log_manager import read_log, compact_log

def check_continuity(data_dir: Path) -> List[Dict]:
    """
    检查数据连续性，返回断裂点列表。
    
    首尾衔接规则：log[n].data_end == log[n+1].data_start 即连续
    """
    log = read_log(data_dir)
    
    if log.is_empty() or len(log) < 2:
        return []
    
    gaps = []
    log_rows = log.to_dicts()
    
    for i in range(1, len(log_rows)):
        prev_end = log_rows[i - 1]["data_end"]
        curr_start = log_rows[i]["data_start"]
        
        if prev_end != curr_start:
            gaps.append({
                "gap_after": prev_end,
                "gap_before": curr_start,
                "missing_from": prev_end,
                "missing_to": curr_start,
            })
    
    return gaps


def get_data_range(data_dir: Path) -> Dict | None:
    """获取已有数据的时间范围"""
    log = read_log(data_dir)
    
    if log.is_empty():
        return None
    
    return {
        "start": log["data_start"].min(),
        "end": log["data_end"].max(),
    }


def find_missing_ranges(
    data_dir: Path,
    target_start: int,
    target_end: int,
) -> List[Dict]:
    """
    找出目标时间范围内缺失的数据段。
    用于增量下载。
    """
    data_range = get_data_range(data_dir)
    gaps = check_continuity(data_dir)
    
    missing = []
    
    # 1. 检查目标范围之前是否有缺失
    if data_range is None:
        # 完全没有数据
        missing.append({"start": target_start, "end": target_end})
        return missing
    
    if target_start < data_range["start"]:
        missing.append({"start": target_start, "end": data_range["start"]})
    
    # 2. 检查中间的断裂
    for gap in gaps:
        if gap["missing_from"] >= target_start and gap["missing_to"] <= target_end:
            missing.append({"start": gap["missing_from"], "end": gap["missing_to"]})
    
    # 3. 检查目标范围之后是否有缺失
    if target_end > data_range["end"]:
        missing.append({"start": data_range["end"], "end": target_end})
    
    return missing
```

---

## entry.py - 主入口

```python
import polars as pl
from pathlib import Path
from typing import Callable
from filelock import FileLock

from .config import get_data_dir, PARTITION_CONFIG
from .storage import read_ohlcv, save_ohlcv
from .log_manager import append_log, compact_log
from .continuity import check_continuity, find_missing_ranges

def period_to_ms(period: str) -> int:
    """将周期字符串转换为毫秒"""
    if period.endswith("m"):
        return int(period[:-1]) * 60 * 1000
    elif period.endswith("h"):
        return int(period[:-1]) * 3600 * 1000
    elif period.endswith("d"):
        return int(period[:-1]) * 24 * 3600 * 1000
    else:
        raise ValueError(f"Unsupported period: {period}")


def get_ohlcv_with_cache(
    base_dir: Path,
    symbol: str,
    period: str,
    start_time: int,
    count: int,
    fetch_callback: Callable,
    fetch_callback_params: dict = {},
    enable_cache: bool = True,
) -> pl.DataFrame:
    """
    获取 OHLCV 数据，优先从缓存读取，缺失部分从 API 获取。
    """
    data_dir = get_data_dir(base_dir, symbol, period)
    lock_path = data_dir / ".lock"
    data_dir.mkdir(parents=True, exist_ok=True)
    
    with FileLock(lock_path):
        period_ms = period_to_ms(period)
        end_time = start_time + (count - 1) * period_ms
        
        if not enable_cache:
            # 不使用缓存，直接从 API 获取
            new_data = fetch_callback(
                symbol, period, start_time, count, **fetch_callback_params
            )
            save_ohlcv(base_dir, symbol, period, new_data)
            if not new_data.is_empty():
                append_log(
                    data_dir,
                    new_data["time"].min(),
                    new_data["time"].max(),
                    len(new_data),
                )
            return new_data
        
        # 查找缺失的数据段
        missing_ranges = find_missing_ranges(data_dir, start_time, end_time)
        
        # 下载缺失的数据
        for missing in missing_ranges:
            # 计算需要下载的数量（会多算，不会少算）
            missing_count = (missing["end"] - missing["start"]) // period_ms + 1
            
            new_data = fetch_callback(
                symbol, period, missing["start"], missing_count,
                **fetch_callback_params
            )
            
            if not new_data.is_empty():
                # 过滤掉超出范围的数据
                new_data = new_data.filter(
                    (pl.col("time") >= missing["start"]) &
                    (pl.col("time") <= missing["end"])
                )
                
                save_ohlcv(base_dir, symbol, period, new_data)
                append_log(
                    data_dir,
                    new_data["time"].min(),
                    new_data["time"].max(),
                    len(new_data),
                )
        
        # 定期合并日志（可选，减少日志行数）
        compact_log(data_dir)
        
        # 返回请求范围内的数据
        return read_ohlcv(base_dir, symbol, period, start_time, end_time)
```
