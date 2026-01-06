# 核心模块接口说明

本核心层设计遵循**数据与日志分离**、**首尾衔接优先**和**按时间分块存储**的原则。

## 目录结构

```
src/cache_tool/
  models.py        # Pydantic 模型定义
  config.py        # 分块配置与路径生成
  storage.py       # OHLCV 数据读写（Parquet）
  log_manager.py   # 日志管理（JSONL）
  continuity.py    # 连续性检查
  entry.py         # 统一入口与缓存算法
```

---

## 1. 模型定义 (`models.py`)

利用 Pydantic 进行严格的数据验证。

### 核心模型

- **`OHLCVRow`**: 单条 K 线数据。验证 `high >= low`, `high >= max(open, close)` 等逻辑。
- **`LogEntry`**: 单条获取日志。记录数据的时间范围 (`data_start`, `data_end`) 和条数。
    *   **关于 `count`**: 为 `Optional[int]`。单次写入(`append_log`)时准确，合并(`compact_log`)后置为 `None` 以避免误导。
- **`DataRange`** / **`Gap`**: 用于连续性检查的辅助模型。
- **`PartitionWindow`**: 分块窗口类型，值为 `"month"` / `"year"` / `"decade"`。
- **`DataLocation`**: 唯一定位数据的参数组合 (Exchange, Mode, Market, Symbol, Period)。

---

## 2. 配置与路径 (`config.py`)

负责文件系统的路径映射和分块策略。

### 关键配置

- **`MAX_PER_REQUEST`**: 硬编码为 1500（交易所限制）。
- **`PARTITION_CONFIG`**: 分块策略配置。
    - 分钟级 (`1m`-`30m`) → **按月** (`YYYY-MM`)
    - 小时级 (`1h`-`4h`) → **按年** (`YYYY`)
    - 日线及以上 → **按10年** (`2020s`)

### 核心函数

- `get_partition_key(timestamp_ms, period)`: 计算时间戳对应的分块文件名。
- `get_data_dir(...)`: 生成数据的标准存储路径。
- `period_to_ms(period)`: 将周期字符串（如 `"15m"`）转换为毫秒数。

---

## 3. 存储层 (`storage.py`)

底层数据 IO 操作，基于 Polars 和 Parquet。

### 核心函数

#### `read_ohlcv(base_dir, loc, start_time, end_time) -> pl.DataFrame`
读取指定范围的 OHLCV 数据。自动加载涉及的所有 Parquet 分块文件。

#### `save_ohlcv(base_dir, loc, new_data)`
保存数据。自动执行以下关键步骤：
1.  **按时间分块**：将数据分散到对应的 `.parquet` 文件中。
2.  **合并去重**：读取已有文件，追加新数据。
3.  **去重策略**：使用 `.unique(keep="last")`，保留最新的数据（应对 K 线未走完的情况）。
4.  **自动记录日志**：保存后自动调用 `append_log`。

#### `save_ohlcv_with_lock(...)`
带文件锁的保存操作，防止并发写入冲突。锁文件名为 `.lock`。

---

## 4. 日志管理 (`log_manager.py`)

管理 `fetch_log.jsonl`，这是判断缓存连续性的核心依据。

### 核心函数

#### `append_log(...)`
追加一条新的获取记录。

#### `compact_log(data_dir)`
**核心维护函数**。合并可连接的日志条目，减少碎片。
*   **合并条件**：首尾衔接 (`end == start`) 或 重叠/包含。
*   **运行时机**：每次读取缓存前必须运行。

#### `read_log(data_dir) -> list[LogEntry]`
读取日志文件为 `LogEntry` 列表。
*   **自动重建**：如果日志文件损坏（包含无法解析的行），会打印警告并自动调用 `rebuild_log_from_data` 重建。

#### `rebuild_log_from_data(data_dir)`
（灾难恢复）从数据文件重建日志。
*   **设计原则**：禁止预测时间间隔。
*   **保守策略**：由于不能假设数据连续，此函数将所有已知数据视为**一个连续的大段**。这是为了安全起见，避免错误的连续性假设。

---

## 5. 连续性检查 (`continuity.py`)

提供缓存状态的诊断工具。

- `check_continuity(data_dir) -> list[Gap]`: 返回所有数据断裂点。
- `find_missing_ranges(...)`: 计算目标范围内缺失的数据段（用于增量下载）。

---

## 6. 统一入口 (`entry.py`)

对外暴露的高级接口，封装了缓存策略。

### 核心函数

#### `get_ohlcv_with_cache(...)`

**简化缓存算法**实现：

1.  **日志整理**：首先调用 `compact_log`。
2.  **缓存检查**：只检查**起始时间** (`start_time`) 是否命中缓存。
    *   命中：从缓存读取起始段，更新 `current_time` 到缓存末尾。
    *   未命中：`current_time = start_time`。
3.  **连续请求**：从 `current_time` 开始循环请求网络，直到满足 `count`。
    *   **首尾衔接**：每次请求都从已有数据的**末尾时间**开始。
    *   **+1 补偿**：第二轮请求开始多请求 1 条，用于处理首尾重叠。
    *   **防死循环**：检测空返回、数量不足、去重后无增长等边界。
