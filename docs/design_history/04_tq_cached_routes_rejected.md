# TQ、Route 与代码收口

> **Status: Rejected design archive. Do not implement.** 本文从早期 `todo/` 完整迁入正式文档区，只保留历史讨论；现行规范以 [`docs/README.md`](../README.md) 标记的 Implemented design 为准。


> **文档状态：Ideas archive，非实施计划。** 这是一个未来可能考虑、但由于复杂度过高而更可能不会实施的 TODO；本文仅保留设计思路，不代表当前架构决策或开发排期。

## 1. Canonical OHLCV schema

统一缓存第一版只缓存：

```text
time: Int64 milliseconds
open: Float64
high: Float64
low: Float64
close: Float64
volume: Float64
```

TQ canonical adapter：

```text
datetime(ns) → time(ms)
选择单 symbol
选择 open/high/low/close/volume
清理 placeholder
校验时间轴
转换为统一 schema
```

第一版不缓存：

- open_oi/close_oi；
- 多 symbol 的 open1/close1/symbol1；
- 其他 raw-only 字段。

未来需要持仓量时应建立明确 schema variant，不能将可变列混入现有 Parquet。

---

## 2. TQ FetchOperation adapter

当前 `get_kline_serial` 只有最新固定窗口，最大 data_length=10000。

### FullQuery

| RouteIntent | TQ 当前能力 |
| --- | --- |
| LatestLimit | 可完整支持，受 10000 限制 |
| SinceLimit | since 位于 serial window 内时可支持 |
| SinceLatest | since 位于 serial window 内时可支持 |

### AfterCount/BeforeCount

TqClient 使用自适应 data_length：

```text
读取 serial
→ 过滤 anchor
→ 检查实际 count
→ 不足则扩大 data_length
→ 最大 10000
```

例如：

```text
256 → 512 → 1024 → 2048 → ... → 10000
```

如果最大窗口仍不包含 anchor，返回 `UNAVAILABLE`，不能返回空数据并伪装成该区间无 K 线。

本地缓存运行足够长后可以积累超过 10000 根，但冷启动能力仍受上游窗口限制。

---

## 3. Route 设计

建议新增：

```text
GET /ccxt/fetch_ohlcv/from-start
    since + limit

GET /ccxt/fetch_ohlcv/latest
    limit

GET /ccxt/fetch_ohlcv/from-start-to-latest
    since
```

公共参数：

- exchange_name；
- market；
- mode；
- symbol；
- timeframe；
- include_last；
- enable_cache。

Route 映射：

```python
client = provider_registry.get(exchange_name, market, mode)

intent = SinceLimitIntent(
    series=client.series_key(...),
    since=since,
    limit=limit,
    include_last=include_last,
)

if enable_cache:
    result = cache_engine.resolve_since_limit(
        intent=intent,
        source=client,
    )
else:
    result = client.execute(FullQuery(intent)).data
    if not intent.include_last:
        result = result[:-1]
```

另外两个路由分别调用 `resolve_latest_limit` 和 `resolve_since_latest`。

现有 `/ccxt/fetch_ohlcv` 临时兼容：

```text
有 since + limit → SinceLimitIntent
无 since + limit → LatestLimitIntent
```

旧路由不得把“有 since、无 limit”静默改成 SinceLatest，避免破坏旧默认行为。

TQ 可逐步增加对称 cached route；原始 `/tq/fetch_ohlcv` 继续作为 raw thin-forward compatibility route。

---

## 4. 目标代码结构

```text
src/
  providers/
    registry.py
    binance_client.py
    kraken_client.py
    tq_client.py

  ohlcv/
    intent.py          # RouteIntent
    operation.py       # FullQuery/AfterCount/BeforeCount
    result.py          # FetchResult/FetchStatus
    protocol.py        # OhlcvSource
    cache_engine.py    # 三个公开入口
    walkers.py         # ForwardWalker/BackwardWalker
    estimator.py       # CacheEstimator
    validation.py

  cache_tool/
    models.py          # SeriesKey/ProofSpan
    storage.py         # read_rows/write_rows
    proof.py
    statistics.py
    config.py

  router/
    trader_router.py
    extended_router.py
    tq_router.py
```

Provider Client 内部可以使用 private helper，但不能重新建立与 Client 平行的 `*_utils` 或 `*_adapter` facade。

---

## 5. 最终删除的冗余代码

完成 cutover 后删除：

- `src/tools/ccxt_utils.py`；
- `src/tools/ccxt_utils_extended.py`；
- `src/tools/binance_adapter.py`；
- 返回裸 CCXT instance 的旧调用路径；
- cache entry 中 Provider page limit 和网络分页；
- Route 中 `if exchange_name == "binance"` 分支。

应迁移而不是删除：

- request/response model；
- authentication；
- logging 和 exception mapping；
- Parquet/proof/storage tests；
- Binance 特殊订单行为；
- TQ lifecycle 和锁。

迁移期间允许短暂并存，最终 release 不保留两套可调用实现。
