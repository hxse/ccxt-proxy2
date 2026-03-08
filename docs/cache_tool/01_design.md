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

## 设计初衷

本设计的核心目标之一，是兼容**存在自然休盘/跳空的市场**，例如股票、期货和部分有交易时段限制的品种。

设计取舍如下：

- 如果只服务于加密货币（24/7 连续交易），可以直接用固定间隔推断连续性，算法会更简单。
- 本项目刻意不这么做，是为了兼容周末休市、节假日休市、盘中停牌等场景。
- 因此“连续”的定义不是“时间戳等间隔连续”，而是“由一次写入批次保证连续”，以及“多个批次可通过首尾重叠继续证明连续”。
- 这也是为什么网络续拉时总是从 `current_time` 再请求一次，而不是猜测下一根时间戳。

换句话说：

- 对加密货币，这套设计是偏保守但通用的。
- 对股票等非 24/7 市场，这套设计是必要的。

---

## 首尾衔接算法

由于禁止计算下一根K线的时间，网络请求必须从已有数据的末尾时间（`current_time`）开始。这会导致首条数据与已有数据重复，需要通过以下流程处理：

```
首尾重叠 → 条件 +1 补偿 → 合并 → 去重 → 截断 → 缓存去尾
```

### 算法流程

1. **首尾重叠**：从已有数据的末尾时间（`current_time`）开始请求，首条数据会与已有数据重复。如果没有已有数据，则直接从起始时间请求
2. **+1 补偿**：只要 `result` 中已经有数据（无论来自缓存或前一轮网络），下一次网络请求就使用 `remaining_count + 1`，用于补偿首条重叠；仅当 `result` 为空时使用 `remaining_count`
3. **合并**：将新数据与已有数据合并
4. **去重**：按时间戳去重，保留新数据（`keep="last"`，因为最新K线可能未走完，需要更新）
5. **截断**：最后截取到目标数量（`result.head(count)`）
6. **缓存去尾**：写入缓存时永远去掉最后一根 K 线（返回给用户仍保留最后一根）

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
- **机制 2**：兜底保护；若触发，表示本轮未产生新增数据，避免无限重试

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

1. **数据与日志分离**：数据文件只存储 `unique(keep="last")` 后的 OHLCV 数据，`fetch_log.jsonl` 才承担连续性证明
2. **按时间分块**：使用固定的时间边界（年/月/10年），而非动态的大小边界
3. **日志首尾衔接**：通过日志的首尾连续性判断数据完整性
4. **文件不自证连续**：单个 parquet 文件自身不证明连续；跨时间分区文件也不证明连续

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
>
> 反过来，**parquet 文件本身不承担这个证明职责**：
> - 单个 parquet 只表示“这些时间戳的数据当前存在于文件中”
> - 单个 parquet 内部只保证按 `time` 字段去重；若同一时间戳出现多次，采用 `unique(keep="last")` 保留最后写入版本
> - 多个时间分区文件并列存在，不代表它们之间连续
> - 文件之间是否可连，必须由日志证明，而不是由文件名、时间分区边界或 `min/max` 自动推出

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

### 1.1 缓存读取策略（日志确定范围 + 时间分区读取）

**这里的“分区”专指时间分区文件**，即：
- 分钟级：`YYYY-MM.parquet`
- 小时级：`YYYY.parquet`
- 日线及以上：`YYY0s.parquet`

说明：
- `exchange/mode/market/symbol/period` 是目录定位维度，不是分区键。
- 每个目录（一个 `exchange/mode/market/symbol/period` 组合）内部，只有“时间分区”这一种分区机制。

缓存阶段读取规则：
1. 先在日志中找到 `start_time` 命中的日志段。
2. 沿日志首尾衔接关系计算可连续到达的 `cache_end`（连续缓存 span）。
3. 只有日志能证明 `cache_end` 存在时，才以 `start_time -> cache_end` 为读取范围，按时间分区顺序读取数据文件。
4. 代码层面会先缩小候选分区范围，再交给 Polars Lazy 做过滤、排序和 `limit(count)`。
5. 若该连续缓存 span 读完仍不足，再进入网络阶段补齐。

如果出现以下任一情况，则缓存命中功能失效，必须直接走网络：

- `fetch_log.jsonl` 不存在
- `fetch_log.jsonl` 已损坏或不可解析
- 日志中没有覆盖 `start_time` 的证明段
- 日志无法证明从 `start_time` 向后连续可达

该策略不依赖“下一根时间戳预测”，只依赖实际数据行数与时间排序结果。

关于第 4 条，需要特别说明：

- 当前实现会先定位 `start_time` 所在的起始分区，只把该分区及其后的文件交给 Polars。
- 然后由 `scan_parquet(...).filter(...).sort(...).limit(count)` 执行读取。
- 因此这是“**缩小候选分区后，依赖 Polars 尽量下推过滤与 limit**”。
- 它不是“算法层面严格保证文件级早停”。

原因是：

- Polars Lazy 会根据执行计划尝试做谓词下推、投影裁剪、Parquet 统计信息过滤等优化。
- 但最终是否真的只读前几个文件、是否还要检查更多候选文件的元数据或 row group，取决于 Polars 版本、查询计划和 Parquet 文件统计信息。
- 所以文档只能承诺“减少候选文件并交由引擎优化”，不能承诺“读够后绝不触碰后续候选文件”。

读取示例（15m，按月分区）：
1. 请求：`start_time=2023-01-20`, `count=300`
2. 日志计算得到连续 span：`[2023-01-20, 2023-03-15]`
3. 分区读取：`2023-01.parquet` → `2023-02.parquet` → `2023-03.parquet`
4. 逻辑目标是在满足 300 条后返回结果；底层是否完全不再触碰后续候选文件，取决于 Polars 的执行计划与文件统计信息

终止条件：
1. 达到 `count`
2. 已无后续分区文件
3. 进入网络阶段（缓存不足时）

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

**无变化跳过写回（关键优化）**：
- `compact_log` 需要比较“合并前后是否完全一致”。
- 若一致（`changed=False`），直接返回，不重写 `fetch_log.jsonl`。
- 目标：避免读路径反复触发无意义写盘。

### 3. 读写时的连续性处理

> [!IMPORTANT]
> 读取流程允许调用 `compact_log`，但必须遵守：
> 1. 若合并结果无变化，不重写日志文件。
> 2. 不得在读路径产生无意义的反复写盘。
> 3. 算法必须保证日志条目的**唯一顺序（全序）**，避免“内容相同但顺序不同”导致误判变化。
>
> 合并后的日志只保留两类关系：
> - **首尾衔接**：`entry_a.data_end == entry_b.data_start`
> - **断裂**：两条日志不相邻且不重叠

唯一顺序约束（用于 `compact_log` 与 `changed` 判定）：
1. 比较前先做规范化排序，不依赖原始文件行顺序。
2. 排序键必须是全序键，例如：
   - `(data_start, data_end, fetch_time, source)`
3. `changed` 的比较对象是“规范化后条目序列”，不是原始读取顺序。
4. 相同输入必须得到相同输出顺序（确定性）。

```python
def get_ohlcv_with_cache(...):
    with FileLock(...):
        # 1. 日志按需合并（无变化则跳过写回）
        compact_log(data_dir)
        
        # 2. 读取合并后的日志
        log_entries = read_log(data_dir)
        
        # 3. 执行缓存查找和网络请求
        ...
```

**注意**：纯写入操作（如 `start_time=None`）不需要先合并日志，因为它不依赖日志状态进行决策。

### 3.1 日志语义（索引日志，不是调试日志）

`fetch_log.jsonl` 的语义是“**连续性证明日志（proof log）**”：
- 记录本轮网络数据在去尾后写入缓存的时间范围。
- 不记录纯缓存复用段。
- 如果缓存去尾后本轮无可写数据（例如仅 1 根返回），则不追加日志。

这里故意不把它定义成“精确的实际落盘增量”，但它仍然是**唯一有效的连续性证明载体**：

- 因为网络批次可能与本地缓存有重叠。
- 落盘时会做 `unique(keep="last")` 去重，最终真正发生变化的行，可能少于日志覆盖范围。
- 因此日志记录的是“本轮网络写入覆盖了这个范围”，而不是“这个范围内每一根都是全新写入”。
- 但只要该条日志来自当前写入算法，它仍可证明“这一批网络数据在逻辑上是连续获取的”。
- 这个证明不来自 parquet 文件，而来自 fetch 批次与写入规则本身。

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

### 日志丢失不可恢复证明

`fetch_log.jsonl` 一旦丢失或损坏，**连续性证明就丢失了**，不能从 parquet 文件中自动恢复。

原因是：

- parquet 文件只保存数据结果，不保存原始 fetch 批次边界
- 单个文件不证明连续
- 跨分区文件也不证明连续
- 因此“按文件扫描时间范围，再回写成 proof log”会把“数据存在”伪装成“连续性已被证明”，这是错误的

正确处理方式是：

- 不自动重建 `fetch_log.jsonl`
- 不把文件级扫描结果写回 proof log
- 没有 proof log 时，缓存命中功能失效，直接重新请求网络
- 新拿到的网络数据，才可以重新生成新的 proof log 条目
- 旧 parquet 文件可以继续留在磁盘上作为数据底座，但在没有 proof log 时不能被当成“已证明连续”的缓存

### 数据是主体，日志是衍生品

- 数据丢失 = 真正的数据丢失
- 日志丢失 = 丢失连续性证明，旧数据仍在，但缓存证明失效

### 无碎片整理

按时间分块的边界是固定的（月初/年初/10年初），不会产生碎片，无需整理。
