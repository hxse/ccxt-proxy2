# 简化缓存算法

## 概述

这是一个简化版的缓存算法，核心特点是：
- **只在起始时检查一次缓存**
- 之后连续网络请求直到完成
- 不在中间检查和复用缓存

相比完整算法，逻辑更简单，边界问题更少。

---

## 与完整算法对比

| 特性 | 完整算法 | 简化算法 |
|------|---------|---------|
| 起始缓存检查 | ✅ | ✅ |
| 中间缓存复用 | ✅ | ❌ |
| 逻辑复杂度 | 较高 | 较低 |
| 网络请求次数 | 最少 | 可能略多 |
| 边界问题 | 需仔细处理 | 较少 |

---

## 核心参数

| 参数 | 说明 | 来源 |
|------|------|------|
| `start_time` | 请求起始时间戳 | 用户传入 |
| `count` | 请求 K 线数量 | 用户传入 |
| `max_per_request` | 单次网络请求最大数量 | 硬编码默认值（如 1500） |

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
- 无包含关系
- 只有首尾相连关系和断裂关系

```python
def get_ohlcv_with_cache(...):
    with FileLock(...):
        # 1. 先合并日志
        compact_log(data_dir)
        
        # 2. 再执行缓存算法
        return fetch_with_cache_simple(...)
```

---

### 核心算法

```python
def fetch_with_cache_simple(
    start_time: int,
    count: int,
    max_per_request: int = 1500,
    fetch_callback: Callable,
    ...
) -> pl.DataFrame:
    """
    简化缓存算法
    
    核心思想：
    1. 只在起始时检查缓存
    2. 之后连续网络请求
    3. 不在中间检查缓存
    """
    
    # 读取合并后的日志
    log_entries = read_log(data_dir)
    
    result = pl.DataFrame()
    current_time = start_time
    remaining_count = count
    
    # 步骤1：检查起始时间是否在缓存中
    cache_entry = find_covering_log(log_entries, start_time)
    
    if cache_entry is not None:
        # 从缓存读取起始段
        cached_data = read_ohlcv_range(
            data_dir,
            start_time,        # 从请求起始时间开始
            cache_entry.data_end  # 到缓存段结束
        )
        result = cached_data
        current_time = cache_entry.data_end  # 从缓存末尾继续
        remaining_count = count - len(result)
    
    # 步骤2：连续网络请求（不再检查中间缓存）
    is_first_request = True
    while remaining_count > 0:
        # 只有第二轮开始才 +1 补偿首条重复
        if is_first_request:
            batch_size = min(max_per_request, remaining_count)
            is_first_request = False
        else:
            batch_size = min(max_per_request, remaining_count + 1)
        
        new_data = fetch_callback(symbol, period, current_time, batch_size)
        
        # 边界检查：网络返回空数据
        if new_data.is_empty():
            break
        
        # 合并数据
        result = merge_data(result, new_data, keep="last")
        
        # 更新状态
        current_time = result["time"].max()
        current_count = len(result)
        remaining_count = count - current_count
        
        # 边界检查：网络返回不足
        if len(new_data) < batch_size:
            break
    
    # 截取到目标数量
    if len(result) > count:
        result = result.head(count)
    
    # 保存到缓存（按时间块分割）
    save_ohlcv(base_dir, loc, result)
    
    return result
```

---

## 示例

### 示例1：起始不在缓存中

```
输入：start_time=1, count=30, max_per_request=10
日志：t=8-15, t=20-27

执行过程：
1. 检查 t=1 是否在缓存中 → 否
2. 连续网络请求：
   - 请求 1-10（10根）
   - 请求 10-19（10根）
   - 请求 19-28（10根）
   - 请求 28-30（2根）
3. 合并得 1-30

结果：
- 网络请求 4 次
- 未复用缓存（起始不在缓存中）
```

### 示例2：起始在缓存中

```
输入：start_time=10, count=30, max_per_request=10
日志：t=8-15, t=20-27

执行过程：
1. 检查 t=10 是否在缓存中 → 是（在 8-15 中）
2. 从缓存读取 10-15（6根）
3. 连续网络请求（从 15 开始）：
   - 请求 15-24（10根）
   - 请求 24-33（10根）
   - 请求 33-40（4根）
4. 合并得 10-40

结果：
- 网络请求 3 次
- 复用了 10-15 的缓存
- 注意：20-27 虽然在缓存中，但未复用（简化算法不检查中间）
```

---

## 边界情况处理

### 1. 起始完全在缓存中

```
场景：start_time=10, count=5, 日志 8-20
处理：直接从缓存读取 10-15，不发起网络请求
```

### 2. 网络返回不足

```
场景：请求 10 根，网络只返回 7 根
处理：退出循环，返回已获取的数据
```

### 3. 防止死循环

```python
# 关键退出条件：
1. new_data.is_empty()           # 网络返回空
2. len(new_data) < batch_size    # 网络返回不足
3. remaining_count <= 0          # 已达到目标
```

---

## 去重策略

使用 `keep="last"`（保留新数据）：

```python
merged.unique(subset=["time"], keep="last")
```

---

## 首尾衔接原则

> [!CAUTION]
> **这是本系统最重要的设计原则之一**
>
> 违反此原则会导致数据遗漏或重复，且难以调试。

网络请求从已有数据的末尾时间开始，不计算下一个时间点：

```
已有数据 1-5 → 请求从 5 开始 → 合并时去重
```

---

## 适用场景

此算法适合以下场景：
- 数据通常是连续请求（增量更新）
- 中间缓存命中率较低
- 追求代码简单可维护

---

## 算法选择建议

| 场景 | 推荐算法 |
|------|---------|
| 频繁随机访问历史数据 | 完整算法（复用中间缓存） |
| 主要是增量更新 | 简化算法 |
| 追求最少网络请求 | 完整算法 |
| 追求代码简单 | 简化算法 |

---

## 本项目的选择

**本项目采用简化算法**。

### 理由

1. **使用场景分析**：
   - **回测**：从某个时间点往后取，连续获取 → 简化算法完全覆盖
   - **交易机器人**：从某个时间往后取到最新，请求不中断 → 简化算法完全覆盖
   - **看盘**：从某个时间往后取到最新，偶尔中断 → 交易机器人同时运行时缓存已填充
   - **无起始时间请求**：无论哪个算法都跳过缓存读取 → 无区别

2. **复杂算法的优势场景不存在**：
   - 复杂算法只在"跳着请求历史数据"时体现优势
   - 本项目的请求都是"从某点开始连续往后"
   - 即使看盘中断，也只是缺失最近几根，大部分历史数据已缓存

3. **维护成本**：
   - 简化算法代码量少，边界问题少
   - 测试用例简单，容易保证正确性
   - 个人项目，维护成本是关键考量

### 结论

对于本项目的使用场景，简化算法的缓存利用率与复杂算法几乎相同，但维护成本大幅降低。
