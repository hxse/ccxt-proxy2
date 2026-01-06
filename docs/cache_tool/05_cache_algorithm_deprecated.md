# 缓存算法（完整版 - 未采用 - 仅作参考）

> [!CAUTION]
> **已弃用方案**
>
> 这是一个设计更复杂但理论缓存利用率更高的方案。
> **本项目实际并未采用此算法**，而是采用了更易于维护的 [简化缓存算法](./05_cache_algorithm_simple.md)。
>
> 保留本文档仅作为设计思路的参考和存档。切勿照此实现。



## 概述

本缓存系统是一个智能的 OHLCV 数据缓存层，核心目标是：
- 减少网络请求次数
- 复用已有缓存数据（包括中间缓存）
- 处理数据断裂和缺失
- 保证数据连续性

---

## 核心参数

| 参数 | 说明 | 来源 |
|------|------|------|
| `start_time` | 请求起始时间戳 | 用户传入 |
| `count` | 请求 K 线数量 | 用户传入 |
| `max_per_request` | 单次网络请求最大数量 | 硬编码默认值（如 1500） |
| `period` | K 线周期 | 用户传入 |

> **注意**：`max_per_request` 硬编码为交易所的最大限制（如币安 1500），不暴露给用户，避免配置错误。

---

## 无起始时间的处理

当 `start_time` 为 `None` 时（请求"最新的N根K线"）：

1. **跳过缓存读取**：最新数据无法从缓存获取
2. **直接网络请求**：从交易所获取最新数据
3. **写入缓存和日志**：获取后保存供后续使用

```python
if start_time is None:
    # 跳过缓存读取，直接请求
    new_data = fetch_callback(symbol, period, None, count)
    
    # 只写入缓存和日志，不读取
    save_ohlcv(base_dir, loc, new_data)
    
    return new_data
```

---

## 算法流程

### 前置条件

**每次请求前，必须先运行同步的日志合并算法**，确保日志文件中：
- 无包含关系（重叠日志已合并）
- 只有首尾相连关系和断裂关系

这保证了算法可以高效运行，不需要处理复杂的重叠判断。

```python
# 伪代码
def get_ohlcv_with_cache(...):
    with FileLock(...):
        # 1. 先合并日志
        compact_log(data_dir)
        
        # 2. 再执行缓存算法
        return fetch_with_cache(...)
```

---

### 核心算法

```python
def fetch_with_cache(
    start_time: int,
    count: int,
    max_per_request: int = 1500,
    fetch_callback: Callable,
    ...
) -> pl.DataFrame:
    """
    智能缓存获取算法
    
    核心思想：
    1. 分批请求网络数据
    2. 每批请求后检查是否可以复用缓存
    3. 合并数据，直到达到目标数量或数据源耗尽
    """
    
    # 读取合并后的日志（只有首尾相连或断裂关系）
    log_entries = read_log(data_dir)
    
    result = pl.DataFrame()
    current_time = start_time
    remaining_count = count
    
    is_first_request = True
    while remaining_count > 0:
        # 只有第二轮开始才 +1 补偿首条重复
        if is_first_request:
            batch_size = min(max_per_request, remaining_count)
            is_first_request = False
        else:
            batch_size = min(max_per_request, remaining_count + 1)
        
        # 网络请求（从 current_time 开始）
        new_data = fetch_callback(symbol, period, current_time, batch_size)
        
        # 边界检查1：网络返回空数据
        if new_data.is_empty():
            break  # 数据源耗尽，退出循环
        
        # 合并网络数据到结果
        result = merge_data(result, new_data, keep="last")
        
        # 获取当前数据的末尾时间
        current_end_time = result["time"].max()
        
        # 检查末尾时间是否落在某个缓存日志范围内
        cache_entry = find_covering_log(log_entries, current_end_time)
        
        if cache_entry is not None:
            # 复用缓存：从缓存中读取该段数据并合并
            cached_data = read_ohlcv_range(
                data_dir, 
                cache_entry.data_start, 
                cache_entry.data_end
            )
            result = merge_data(result, cached_data, keep="last")
            
            # 更新当前时间为缓存数据的末尾
            current_end_time = cache_entry.data_end
        
        # 更新状态
        current_time = current_end_time  # 下次从末尾开始请求（首尾衔接）
        current_count = len(result)
        remaining_count = count - current_count
        
        # 边界检查2：网络返回数据不足
        if len(new_data) < batch_size:
            break  # 数据源耗尽（已到最新），退出循环
    
    # 截取到目标数量
    if len(result) > count:
        result = result.head(count)
    
    # 保存到缓存（按时间块分割）
    save_ohlcv(base_dir, loc, result)
    
    return result
```

---

### 辅助函数

```python
def find_covering_log(log_entries: list[LogEntry], time: int) -> LogEntry | None:
    """
    查找包含指定时间的日志条目
    
    判断条件：log.data_start <= time <= log.data_end
    """
    for entry in log_entries:
        if entry.data_start <= time <= entry.data_end:
            return entry
    return None


def merge_data(
    existing: pl.DataFrame, 
    new: pl.DataFrame, 
    keep: str = "last"
) -> pl.DataFrame:
    """
    合并数据并去重
    
    keep="last" 保留新数据（最后一根 K 线可能未走完，保留最新的）
    """
    if existing.is_empty():
        return new
    if new.is_empty():
        return existing
    
    merged = pl.concat([existing, new])
    return merged.unique(subset=["time"], keep=keep).sort("time")
```

---

## 边界情况处理

### 1. 网络返回数据不足

```
场景：请求 10 根，网络只返回 7 根
原因：已到达最新数据，交易所没有更多了
处理：退出循环，保存已获取的数据
```

### 2. 缓存完全覆盖请求范围

```
场景：请求 t=10 开始 5 根，日志 t=8-20 已覆盖
处理：直接从缓存读取，不发起网络请求

优化：在循环开始前检查，如果目标范围完全在缓存内，直接返回
```

### 3. 多段断裂的日志

```
场景：日志 t=8-15, t=100-120，请求 t=1 开始 50 根
处理：
  - 请求 1-10，发现 10 在 8-15 内，合并得 1-15
  - 请求 15-25，发现 25 不在任何日志内，继续
  - ...
  - 最终 16-99 从网络获取，8-15 复用缓存
```

### 4. 首尾刚好相等

```
场景：网络数据末尾 = 8，日志范围 8-15
判断：8 <= 8 <= 15 → True
处理：正确合并
```

### 5. 防止死循环

```python
# 关键退出条件（满足任一即退出）：
1. new_data.is_empty()           # 网络返回空
2. len(new_data) < batch_size    # 网络返回不足（数据源耗尽）
3. remaining_count <= 0          # 已达到目标数量
```

---

## 去重策略

**使用 `keep="last"`（保留新数据）**

原因：最后一根 K 线可能尚未走完，新数据更准确。

```python
merged.unique(subset=["time"], keep="last")
```

---

## 保存规则

### 数据保存

按时间块规则分割保存：

```python
def save_ohlcv(...):
    # 根据周期确定分块规则
    # 分钟级 → 按月分块
    # 小时级 → 按年分块
    # 日线级 → 按10年分块
    
    for partition_key, group in data.group_by(partition_column):
        # 分别保存到对应的 .parquet 文件
        save_to_file(f"{partition_key}.parquet", group)
```

### 日志保存

日志不分块，每个 `exchange/mode/market/symbol/period` 组合使用一个 `fetch_log.jsonl` 文件。

每次保存数据后，追加一条日志：

```python
append_log(data_dir, data_start, data_end, count)
```

---

## 首尾衔接原则

> [!CAUTION]
> **这是本系统最重要的设计原则之一**
>
> 违反此原则会导致数据遗漏或重复，且难以调试。

**网络请求必须从已有数据的末尾时间开始，不能自己计算下一个时间点。**

```
错误做法：已有 1-5，计算 period，请求从 6 开始
正确做法：已有 1-5，请求从 5 开始，合并时去重
```

原因：
- 不同市场的 K 线间隔不同（加密货币 24/7，股票有休市）
- 计算可能导致遗漏或重复
- 首尾衔接 + 去重更简单可靠

---

## 完整示例

```
输入：start_time=1, count=30, max_per_request=10
日志：t=8-15, t=20-27

执行过程：
1. 请求 1-10（10根）
   → 检查 10 在 8-15 内 ✓
   → 从缓存读取 8-15，合并得 1-15

2. 请求 15-25（10根，从15开始）
   → 检查 25 在 20-27 内 ✓
   → 从缓存读取 20-27，合并得 1-27

3. 请求 27-30（4根，因为只剩4根需求）
   → 合并得 1-30

4. 截取到 30 根，保存缓存和日志

结果：
- 网络请求 3 次（而非 3 次各10根）
- 复用了 8-15 和 20-27 的缓存数据
```

---

## 同步执行

**所有操作都是同步的**，不使用异步：
- FastAPI 路由层已自带异步
- 内部计算无需异步
- 避免并发问题
