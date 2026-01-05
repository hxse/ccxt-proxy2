# 新数据库架构设计理念

## 核心设计

### 目录结构
```
data/
  ohlcv/
    {symbol}_{period}/
      {year}.parquet       ← 数据，按时间分块
      fetch_log.jsonl      ← 日志，追加写入
```

### 连续性验证机制

**日志首尾衔接规则**：
```
log_entry_1: {data_start: t1,   data_end: t100}
log_entry_2: {data_start: t100, data_end: t200}  ← t100 == t100，连续 ✅
log_entry_3: {data_start: t300, data_end: t400}  ← t200 != t300，断裂 ❌
```

只要相邻日志的 `entry[n].data_end == entry[n+1].data_start`，即视为数据连续。

---

## 方案对比

### 旧方案（按大小分块 + 首尾重叠）

```
BTC_USDT 15m 20230101T060000Z 20230101T093000Z 0015.parquet
BTC_USDT 15m 20230101T093000Z 20230101T120000Z 0015.parquet
```

| 维度 | 复杂度 |
|------|--------|
| 分块边界计算 | 需要 `get_chunk_slices`，处理正向/反向 |
| 追加数据 | 需要 `handle_cache_write` 处理重叠 |
| 碎片整理 | 需要 `consolidator` 合并小文件 |
| 读取数据 | 需要找起始文件→加载多个→合并→去重 |
| **代码行数** | **~800 行** |

### 新方案（按时间分块 + 日志分离）

```
BTC_USDT_15m/
  2023.parquet
  2024.parquet
  fetch_log.jsonl
```

| 维度 | 复杂度 |
|------|--------|
| 分块边界计算 | 从时间戳提取年份，固定边界 |
| 追加数据 | 读取→合并→去重→写回 + 追加日志 |
| 碎片整理 | **不需要** |
| 读取数据 | 单文件读取 + filter |
| **代码行数** | **~150 行** |

---

## 复杂度对比图

```
                    写入    读取    追加    合并    验证    总复杂度
旧方案（按大小）     ████    ████    █████   █████   ██      ████████████████████
新方案（按时间）     ██      █       ██      -       ██      █████████
```

---

## 可维护性优势

### 1. 日志丢失可重建

即使 `fetch_log.jsonl` 丢失，可以从数据文件重建：

```python
def rebuild_log_from_data(data_path, period_ms):
    df = pl.read_parquet(data_path).sort("time")
    
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
    ])
    return log
```

**连续部分可重建，断裂部分无法重建**——但断裂部分会在日后查询时自然发现并补充。

### 2. 数据是主体，日志是衍生品

- 数据丢失 = 真正的数据丢失
- 日志丢失 = 只是丢失"获取历史"，可重建

### 3. 无碎片整理

按时间分块的边界是固定的（年初/年末），不会产生碎片，无需整理。

---

## 关键特性

### 按时间分块 + 可配置窗口

不同周期可配置不同的分块窗口：

```python
PARTITION_CONFIG = {
    "1m":  "month",   # 分钟K线按月分块
    "15m": "year",    # 15分钟K线按年分块
    "1h":  "year",    # 小时K线按年分块
    "1d":  "decade",  # 日K线按10年分块（或不分块）
}
```

### 日志合并

避免日志过大导致 Python 循环性能问题：

```python
def compact_log(log_entries):
    """合并首尾相连的日志条目"""
    # 相邻条目如果 end == next.start，合并为一条
```

### FileLock 并发安全

```python
from filelock import FileLock

def save_ohlcv_with_lock(symbol, period, data, data_dir):
    lock_path = data_dir / f"{symbol}_{period}.lock"
    with FileLock(lock_path):
        save_ohlcv(symbol, period, data, data_dir)
```
