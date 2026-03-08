# 简化缓存算法

## 概述

这是一个简化版的缓存算法，核心特点是：
- **从起点开始，连续读取本地缓存（可跨多个连续缓存文件）**
- 若缓存不足，再进入连续网络请求直到完成
- **一旦进入网络阶段，不再回头检查和复用中间缓存**
- 默认返回最后一根 K 线给用户，但缓存写入时去掉最后一根

这个设计不是只为加密货币准备的。

- 对加密货币，直接用固定间隔推断连续性通常更简单。
- 本项目仍坚持“首尾重叠 + 去重”方案，是为了兼容股票、期货等存在周末休市、节假日休市、临时停牌的市场。
- 因此算法不依赖“下一根时间戳预测”，而依赖“已有尾部时间再次请求、首条允许重叠”的策略来续接数据。

相比完整算法，逻辑更简单，边界问题更少。

---

## 两阶段模型（强定义）

本算法被严格拆成两个阶段，且**单向流转，不回退**：

```
阶段A（缓存阶段）  ->  阶段B（网络阶段）  ->  结束
```

### 阶段A：缓存阶段（Cache Phase）

目标：
- 从 `start_time` 开始，尽可能连续复用本地缓存数据。

规则：
- 先在日志中计算从 `start_time` 可连续到达的缓存 span（`cache_end`）。
- 再按时间分区顺序读取 `start_time -> cache_end` 范围，读够 `count` 即停。
- 如果没有 proof log，或者 proof log 无法证明 `start_time` 命中缓存链，则阶段A直接跳过。
- 一旦遇到断裂，或已满足 `count`，阶段A结束。

退出条件：
1. `len(result) >= count`：直接结束，不进入网络阶段。
2. 缓存链断裂或缓存不足：进入阶段B。

### 阶段B：网络阶段（Network Phase）

目标：
- 用网络请求补齐剩余 `count`。

规则：
- 从当前 `current_time` 连续请求网络直到满足数量或数据源耗尽。
- **禁止**在阶段B重新检查本地中间缓存并做“部分复用+部分请求”的混合决策。
- 请求数量规则：
  - 当 `result` 为空：请求 `remaining_count`
  - 当 `result` 非空：请求 `remaining_count + 1`（补偿首尾重叠）

退出条件：
1. `remaining_count <= 0`
2. 网络返回空数据
3. 网络返回不足批次（源端暂时/永久无更多数据）
4. 去重后无新增（防死循环）

### 为什么必须这样拆分

如果在网络阶段继续动态检查本地缓存并做部分复用，会引入大量分支：
- 缓存/网络重叠区间裁剪
- 多来源拼接顺序和去重优先级
- 回退与前进的状态维护

这些分支会显著提高复杂度和出错概率，但实际收益有限。  
因此本设计选择：**阶段A尽量吃缓存，阶段B纯网络补齐**。

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
3. **缓存去尾后写入**：返回完整结果给用户，但落盘前去掉最后一根；无可写数据则跳过日志

```python
if start_time is None:
    # 跳过缓存读取，直接请求
    new_data = fetch_callback(symbol, period, None, count)

    # 返回完整数据；缓存默认去尾
    cache_data = new_data.head(max(0, len(new_data) - 1))
    if not cache_data.is_empty():
        save_ohlcv(base_dir, loc, cache_data)
        append_log_for_written_range(cache_data)

    return new_data
```

---

## 算法流程

### 前置条件

读取前可运行日志合并，确保日志关系简洁：
- 无包含关系
- 只有首尾相连关系和断裂关系

```python
def get_ohlcv_with_cache(...):
    with FileLock(...):
        # 1. 合并日志（若无变化则不重写文件）
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
    1. 缓存阶段：从 start_time 开始，顺序读取后续连续缓存段
    2. 网络阶段：若仍不足，连续请求网络补齐
    3. 网络阶段不再回头复用中间缓存（避免复杂度）
    """
    
    # 读取 proof log；如果日志不存在、损坏或不能证明起点命中，则跳过缓存阶段
    log_entries = read_log(data_dir)
    
    result = pl.DataFrame()
    current_time = start_time
    remaining_count = count
    network_batches = []
    
    # 阶段A：缓存阶段（只有 proof log 能证明 span 时才读取缓存）
    cache_end = find_cache_span_end(log_entries, start_time)
    if cache_end is not None:
        cached_part = read_ohlcv(
            base_dir,
            loc,
            start_time=start_time,
            end_time=cache_end,
            count=count,
        )
        result = merge_data(result, cached_part, keep="last")
        current_time = result["time"].max()
        remaining_count = count - len(result)

    # 注意：如果 cache_end is None，不代表磁盘上没有 parquet，
    # 只代表没有连续性证明，因此必须直接转入网络阶段。

    if remaining_count <= 0:
        result = result.head(count)
        cache_data = result.head(max(0, len(result) - 1))  # 缓存默认去尾
        if not cache_data.is_empty():
            save_ohlcv(base_dir, loc, cache_data)
            append_log_for_written_range(cache_data)
        return result

    # 阶段B：网络阶段（不再检查中间缓存）
    while remaining_count > 0:
        has_existing = not result.is_empty()
        batch_size = min(max_per_request, remaining_count + (1 if has_existing else 0))
        
        new_data = fetch_callback(symbol, period, current_time, batch_size)
        network_batches.append(new_data)
        
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
    
    # 返回完整数据给用户；缓存默认去掉最后一根
    cache_data = result.head(max(0, len(result) - 1))
    if not cache_data.is_empty():
        save_ohlcv(base_dir, loc, cache_data)
        # proof log 记录网络写入范围，不记录纯缓存复用，不记录被去尾丢弃的数据
        append_log_for_written_range(cache_data)
    
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
   - 请求 28 开始 3 根（去重后新增 2 根）
3. 合并得 1-30

结果：
- 网络请求 4 次
- 未复用缓存（起始不在 proof log 覆盖范围中，即使磁盘上可能已有 parquet）
```

### 示例2：起始在缓存中

```
输入：start_time=10, count=30, max_per_request=10
日志：t=8-15, t=15-22, t=22-27, t=35-40

执行过程：
1. 缓存阶段先由日志计算连续 span：
   - `t=8-15, 15-22, 22-27` 可连成 `8-27`
   - 后续 `35-40` 与 `8-27` 断裂，不纳入 span
   - 从 span 内读取 `10-27` 共 18 根，仍不足 30
2. 进入网络阶段（从 27 开始）：
   - 请求 27-36（10根）
   - 请求 36-39（4根）
3. 合并得 10-39

结果：
- 先连续复用起点后的缓存段（10-27）
- 网络阶段 2 次请求补齐
- 注意：网络阶段即使本地存在 35-40，也不回头复用
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

## 缓存与日志语义（默认策略）

1. 返回值：返回完整 `result`（包含最后一根，便于用户观察最新行情）。
2. 缓存落盘：默认去掉最后一根后再写缓存。
3. 日志记录：仅记录“实际写入缓存”的区间；无写入则不记日志。

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
