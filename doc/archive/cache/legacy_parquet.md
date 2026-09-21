# 已删除：旧 Parquet/Proof-log Cache

> **Status: Deleted implementation archive.** 对应源码和测试已经删除；本文只用于历史追溯。

## 1. 删除前的代码形状

```text
src/cache_tool/
  models.py
  config.py
  storage.py
  log_manager.py
  continuity.py
  entry.py
```

`entry.py` 中的 `get_ohlcv_with_cache()` 同时管理 cache proof、Provider callback 和 network pagination，正是目标重构要删除的耦合。

## 2. 物理布局

```text
data/ohlcv/{exchange}/{mode}/{market}/{symbol}/{period}/
  {time-partition}.parquet
  fetch_log.jsonl
  .lock
```

时间分区：

| Timeframe | Legacy partition |
| --- | --- |
| 分钟级 | `YYYY-MM.parquet` |
| 小时级 | `YYYY.parquet` |
| 日线及以上 | `YYY0s.parquet` |

Parquet 只存 rows，不自证连续。Polars scan/filter/sort/limit 可以做 predicate/projection pushdown，但代码无法承诺读够 rows 后绝不触碰其他 candidate files。

## 3. Proof log

`fetch_log.jsonl` 记录 network batch 去尾后的 `data_start/data_end/count/source`，是唯一连续性证明。

可合并：

```text
end == next.start
或时间范围 overlap/contain
```

Compact 前使用全序 key 规范化排序；合并后无变化时不重写文件。

Proof log 丢失或损坏不能从 Parquet min/max 重建；否则会把“rows 存在”伪装成“fetch chain 已证明”。缺 proof 时旧 Parquet 保留，但 cache read 失效，只有新 network batch 可生成新 proof。

## 4. Legacy 两阶段算法

```text
Phase A: 从 start_time 命中的 proof span 读连续 cache prefix
→ 读够 count 则返回
→ 不足则进入 Phase B

Phase B: 从当前 tail 含首连续请求 network
→ 进入 network 后不再读中间 cache
```

请求数量：

```text
result 为空   → remaining_count
result 非空 → remaining_count + 1（补偿 overlap）
```

例子：需要 20，单页最多 10：

```text
page1: t1..t10                    → 10 rows
page2: 从 t10 取 t10..t19      → 19 unique
page3: 从 t19 取 t19,t20       → 20 unique
```

这套算法的有价值原则会保留到新 Client：不计算下一 timestamp、使用 inclusive overlap、合并去重和 no-progress guard。

## 5. Legacy tail policy

对外返回完整 result，落盘统一 `result[:-1]`。去尾后无 rows 就不写 Parquet/日志。

缺点是无条件删尾：无法使有 successor 证据的 `SinceLimit` 完整落盘。目标方案改为 Client 返回 `last_bar_completion_confirmed`，Cache 只在 unknown 时删尾。

## 6. Legacy lock

旧 entry/storage 使用 per-directory `FileLock`，且长时间把 cache planning、network 和本地 IO 包在同一个锁中。这使 Provider/cache 继续耦合，也降低并发。

目标：

- CCXT per-client process-local lock 只保护单个 HTTP attempt；
- DuckDB per-file process-local lock 只保护 write transaction；
- TQ 保留独立 `FileLock`。

## 7. Legacy failure behavior

- Network empty：正常停止；
- 去重无新 row：no-progress 停止；
- Network 少于 batch：当作数据源耗尽；
- Proof 损坏：废弃 proof，network-only；
- Parquet duplicate：`unique(keep="last")`。

“少于 batch 就耗尽”由旧 callback contract 隐式决定。新 Client 必须将 Provider terminal/failure 明确区分，不得把 retry/network failure 当作空区间。

## 8. Legacy tests

旧 tests 覆盖 storage partitions、proof append/compact/corruption、two-phase cache hit、+1 overlap、market gap、large request batching 和 trim-tail。

Cutover 后：

- 保留行为原则的测试意图；
- 删除 Parquet filename、Polars plan、proof JSONL、FileLock 等旧实现测试；
- 用 DuckDB segment/transaction/successor/capacity tests 取代。

## 9. 删除策略

旧 cache 不迁移、不兼容读、不双轨。旧数据是可重建 cache，可在确认精确目录且停止相关进程后删除。不使用宽泛环境变量、glob 或未解析 recursive target 删数据。
