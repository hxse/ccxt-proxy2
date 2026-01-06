# 数据库架构设计理念

## ⚠️ 核心禁忌

> [!CAUTION]
> **禁止核心代码以任何形式主动计算/预测时间间隔**
>
> 不能使用 `period_ms` 来：
> - 计算下一根 K 线的时间（如 `current_time = data_end + period_ms`）
> - 假设时间间隔恒定来检测断裂（如 `diff() != period_ms`）
> - 验证真实数据的时间序列是否"连续"
>
> **原因**：
> - 股市有休市（周末、节假日导致跳空）
> - 不同市场规则不同（加密货币 24/7，股票有休市）
> - 假设时间间隔恒定会导致数据遗漏或重复
>
> **违反此原则是严重的设计疏漏**
>
> **注意**：测试代码可以使用 `period_ms`，因为测试数据（mock 数据）本身就是理想连续的。

---

## 首尾衔接算法

由于禁止计算下一根K线的时间，网络请求必须从已有数据的末尾时间（`current_time`）开始。这会导致首条数据与已有数据重复，需要通过以下流程处理：

```
首尾重叠 → +1 补偿 → 合并 → 去重 → 截断
```

### 算法流程

1. **首尾重叠**：从已有数据的末尾时间（`current_time`）开始请求，首条数据会与已有数据重复。如果没有已有数据，则直接从起始时间请求
2. **+1 补偿**：**只有第二轮开始**需要多请求 1 条（`remaining_count + 1`），补偿首条重复。第一轮不需要，因为少的 1 条会被后续补回，如果没后续则直接返回
3. **合并**：将新数据与已有数据合并
4. **去重**：按时间戳去重，保留新数据（`keep="last"`，因为最新K线可能未走完，需要更新）
5. **截断**：最后截取到目标数量（`result.head(count)`）

### 示例

```
请求 20 条，每次最多 10 条，从无缓存开始

第一轮: 请求 min(10, 20) = 10 条（第一轮不+1）
→ 得到 t1-t10
→ result = 10 条，remaining = 10

第二轮: 从 t10 开始，请求 min(10, 10+1) = 10 条（第二轮+1）
→ 得到 t10-t19（首条 t10 重复）
→ 合并去重后 result = 19 条，remaining = 1

第三轮: 从 t19 开始，请求 min(10, 1+1) = 2 条（+1）
→ 得到 t19, t20（首条 t19 重复）
→ 合并去重后 result = 20 条 ✅
```

### 防死循环机制

```python
# 机制 1：网络返回空数据
if new_data.is_empty():
    break

# 机制 2：去重后无新数据（兜底保护）
if len(result) == prev_len:
    break

# 机制 3：网络返回不足（数据源耗尽）
if len(new_data) < batch_size:
    break
```

**说明**：
- **机制 1、3**：正常退出，数据源已无更多数据
- **机制 2**：兜底保护，理论上第二轮开始 +1 后不应触发

---

## 核心设计

### 目录结构
```
data/
  ohlcv/
    {exchange}/
      {mode}/                 ← live（实盘）或 demo（模拟）
        {market}/             ← future（合约）或 spot（现货）
          {symbol}/
            {period}/
              {partition}.parquet    ← 数据，按时间分块（多个文件）
              fetch_log.jsonl        ← 日志，不分块（单个文件）
```

**示例**：
```
data/ohlcv/binance/live/future/BTC_USDT/15m/
  2023-01.parquet
  2023-02.parquet
  fetch_log.jsonl
```

> **设计要点**：
> - **层级结构**：`exchange` / `mode` / `market` / `symbol` / `period`
> - **mode 参数**：`live`（实盘数据）或 `demo`（模拟数据）
> - **数据分块**：按时间分块（月/年/10年），避免单文件过大
> - **日志不分块**：每个组合使用一个日志文件，便于连续性验证

### 设计原则

1. **数据与日志分离**：数据文件只存储 OHLCV 数据，日志文件记录获取历史
2. **按时间分块**：使用固定的时间边界（年/月/10年），而非动态的大小边界
3. **日志首尾衔接**：通过日志的首尾连续性判断数据完整性

---

## 连续性验证机制

**日志首尾衔接规则**：

```
entry_1: {data_start: t1,   data_end: t100}
entry_2: {data_start: t100, data_end: t200}  ← t100 == t100，连续 ✅
entry_3: {data_start: t300, data_end: t400}  ← t200 != t300，断裂 ❌
```

判定规则：`entry[n].data_end == entry[n+1].data_start`

> **注意**：每条日志自身代表一段连续数据。例如 `{data_start: t1, data_end: t200}` 足以证明 t1→t200 这段数据是连续的。

### 多条日志证明连续性

当多条日志存在重叠或首尾衔接时，可以证明更大范围的连续性：

```
entry_1: {data_start: t100, data_end: t300}
entry_2: {data_start: t50,  data_end: t150}   ← 与 entry_1 重叠
entry_3: {data_start: t250, data_end: t350}   ← 与 entry_1 重叠

合并后可证明: t50 → t350 连续
```

---

## 关键特性

### 1. 按时间分块 + 可配置窗口

不同周期使用不同的分块窗口，避免单文件过大：

| 周期类型 | 分块窗口 | 文件名示例 |
|---------|---------|-----------|
| 分钟级（1m, 5m, 15m, 30m） | 月 | `2023-01.parquet` |
| 小时级（1h, 4h） | 年 | `2023.parquet` |
| 日线及以上（1d, 1w） | 10年 | `2020s.parquet` |

> **10年分块取整规则**：10-19年、20-29年，以此类推。例如 2023 年属于 `2020s`，2030 年属于 `2030s`。

```python
PARTITION_CONFIG = {
    # 分钟级 → 按月分块
    "1m":  "month",
    "5m":  "month",
    "15m": "month",
    "30m": "month",
    # 小时级 → 按年分块
    "1h":  "year",
    "4h":  "year",
    # 日线及以上 → 按10年分块
    "1d":  "decade",
    "1w":  "decade",
}
```

### 2. 日志合并

避免日志过大导致性能问题。

**合并规则**：

1. **首尾衔接**：`entry_a.data_end == entry_b.data_start`
2. **包含关系**：一条日志的时间范围完全包含另一条，或两条日志有重叠

```python
def can_merge(entry_a: LogEntry, entry_b: LogEntry) -> bool:
    """判断两条日志是否可以合并"""
    # 首尾衔接
    if entry_a.data_end == entry_b.data_start:
        return True
    if entry_b.data_end == entry_a.data_start:
        return True
    
    # 重叠或包含（任意方向有交集）
    if entry_a.data_start <= entry_b.data_end and entry_b.data_start <= entry_a.data_end:
        return True
    
    return False

def merge_entries(entry_a: LogEntry, entry_b: LogEntry) -> LogEntry:
    """合并两条日志，取最大范围"""
    return LogEntry(
        data_start=min(entry_a.data_start, entry_b.data_start),
        data_end=max(entry_a.data_end, entry_b.data_end),
        count=entry_a.count + entry_b.count,
        source="compacted",
    )
```

**合并示例**：
```
合并前:
  entry_1: {data_start: t100, data_end: t300}
  entry_2: {data_start: t50,  data_end: t150}
  entry_3: {data_start: t250, data_end: t350}

合并后:
  entry_merged: {data_start: t50, data_end: t350}
```

### 3. 读写时的连续性处理

> [!IMPORTANT]
> **每次读取缓存前，必须先运行日志合并算法**（`compact_log`）。
>
> 合并后的日志只存在两种关系：
> - **首尾衔接**：`entry_a.data_end == entry_b.data_start`
> - **断裂**：两条日志不相邻且不重叠
>
> 这确保了后续的缓存查找逻辑简单可靠，不需要处理复杂的重叠判断。

```python
def get_ohlcv_with_cache(...):
    with FileLock(...):
        # 1. 先合并日志（关键步骤）
        compact_log(data_dir)
        
        # 2. 读取合并后的日志
        log_entries = read_log(data_dir)
        
        # 3. 执行缓存查找和网络请求
        ...
```

**注意**：纯写入操作（如 `start_time=None`）不需要先合并日志，因为它不依赖日志状态进行决策。

### 4. FileLock 并发安全

```python
from filelock import FileLock

def save_ohlcv_with_lock(symbol, period, data, data_dir):
    lock_path = data_dir / ".lock"
    with FileLock(lock_path):
        save_ohlcv(symbol, period, data, data_dir)
```

---

## 可维护性优势

### 日志丢失可重建

即使 `fetch_log.jsonl` 丢失，可以从数据文件重建：

```python
def rebuild_log_from_data(data_path: Path, period_ms: int) -> list[LogEntry]:
    df = pl.read_parquet(data_path).sort("time")
    
    # 找出时间断裂点
    df = df.with_columns(
        (pl.col("time").diff() != period_ms).fill_null(True).alias("is_break")
    )
    
    # 每个断裂点开始新 batch
    df = df.with_columns(
        pl.col("is_break").cum_sum().alias("batch_id")
    )
    
    # 按 batch 聚合得到日志
    log = df.group_by("batch_id").agg([
        pl.col("time").min().alias("data_start"),
        pl.col("time").max().alias("data_end"),
        pl.len().alias("count"),
    ])
    return log
```

**连续部分可重建，断裂部分无法重建**——但断裂部分会在日后查询时自然发现并补充。

### 数据是主体，日志是衍生品

- 数据丢失 = 真正的数据丢失
- 日志丢失 = 只是丢失"获取历史"，可重建

### 无碎片整理

按时间分块的边界是固定的（月初/年初/10年初），不会产生碎片，无需整理。
