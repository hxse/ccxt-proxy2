# DuckDB OHLCV Cache 数据模型

> **Status: Implemented design.** 本文描述当前 DuckDB logical storage 与证明语义。

## 1. 边界

`DuckDbOhlcvCache` 只负责：

- embedded DuckDB schema/lifecycle；
- parameterized SQL；
- 查找/读取最佳 prefix segment；
- 按 exact timestamp overlap 新建/合并 segment；
- successor-aware row selection；
- transaction、capacity 和 eviction。

禁止：

- import CCXT、TQSDK 或 FastAPI；
- network/retry/pagination；
- Provider page limit/params；
- callback 或 Provider client reference；
- 用 timeframe/本机时间估算连续性或尾根状态。

## 2. Canonical row

```text
time: BIGINT      # UTC epoch milliseconds, K-line open time
open: DOUBLE
high: DOUBLE
low: DOUBLE
close: DOUBLE
volume: DOUBLE
```

不保存 Provider raw response、`__batch_id__`、动态列或 TQ open-interest 字段。`time` 在 segment 内唯一，读取结果严格升序。

仅接受全部 rows 均完整合法的 incoming batch。如新 row 与旧 row 同 timestamp，新 row 原子覆盖全部 OHLCV；不逐字段 coalesce。batch 中出现 NULL/NaN/Infinity/invalid row 时，本次 cache write 整体 no-op，旧值不变，避免为被拒绝的真实 row 建立虚假 gap proof。

## 3. Series identity

`series_key` 至少包含：

```text
provider / mode / market / symbol / timeframe / variant
```

它必须编入所有影响数据内容的参数，例如 Binance mark/index/premium-index variant。不同 series 在同 timestamp 上的 rows 是不同 identity。

Canonical encoding 使用字段排序、无多余空白的 JSON；schema version 为 `1`。编码 deterministic，且不包含与数据无关的用户请求参数。

## 4. Logical schema

### `cache_segments`

```text
segment_id: BIGINT PRIMARY KEY
series_key: VARCHAR
covered_from: BIGINT
first_time: BIGINT
last_time: BIGINT
row_count: BIGINT
created_at: TIMESTAMP
updated_at: TIMESTAMP
```

### `ohlcv_rows`

```text
segment_id: BIGINT
time: BIGINT
open: DOUBLE
high: DOUBLE
low: DOUBLE
close: DOUBLE
volume: DOUBLE
PRIMARY KEY(segment_id, time)
```

`cache_segments` 是业务模型，不是外部 JSON index/proof log。DuckDB physical row group 可以重组，但 `segment_id` 业务边界不得因此改变。

## 5. `segment_id`

`segment_id` 是对 Route/用户不可见的 `BIGINT`，由 DuckDB sequence 产生，PRIMARY KEY 保证 live uniqueness。

ID 不表达：

- 时间顺序；
- freshness/优先级；
- 数据供应方；
- 用户 request identity。

ID 不要求连续。正常运行不主动复用；若未来复用，必须先在同一 transaction 删除旧 `ohlcv_rows` 和 metadata。

## 6. Logical segment

一个 segment 代表一条经过 Client 分页、inclusive overlap 和完整性校验的 fetch chain。Cache schema 本身不编码 `timeframe` 邻接证明，只信任 Client 交付的 rows。

当前可写 Cache 的 Binance/Kraken Futures 完整分页会在 Client 层拒绝 `m/h/d/w` 内部异常 gap。Cache 不重复校验、不解释 gap 原因，也不使用 `timeframe` 修复 rows。

## 7. `covered_from`

`covered_from` 是 fetch chain 已证明的请求下界：

```text
covered_from <= first_time
[covered_from, first_time) 没有 Provider rows
```

示例：

```text
用户 since：品种上市前 12:00
Provider first row：品种上市时 09:00

covered_from = 上市前 12:00
first_time   = 上市时 09:00
```

不需要单独的 redirect/gap table。后续请求落在该半开区间时，直接在该 segment 读 `time >= since` 的 rows。

如更早的 verified request 与 segment 通过 exact overlap 连接：

```python
covered_from = min(
    existing_covered_from,
    verified_request_since,
)
```

例如原证明为上市前 12:00—上市时 09:00，新请求从更早的 11:00 仍首次返同一根 09:00，则向前扩展为 11:00—09:00。

只有能保证“遵循 since 并返回起点后最早 rows”的 network method 可以提供 `verified_covered_from`。`LatestLimit` 没有 since 证明，非空结果使用 `covered_from=first_time`。Kraken Spot thin-forward 不写 cache。

## 8. Segment merge

只有两个 timestamp 集合交集非空才算 overlap。Min/max 时间范围交叉不等于 overlap，因为内部 gap 可能是自然休盘。

Incoming 同时 overlap 多个 segments 时，在一个 transaction 中全部合并为 canonical segment。Canonical 优先选 `row_count` 最大者以减少移动，相同时使用 deterministic tie-breaker。

无 exact overlap 的 segments 继续分开。Read path 不跨 segment 解多个缺口。

## 9. Historical revision contract

目标 cache 是保守的 append/upsert cache，不是 Provider mirror：

```text
同 timestamp 出现完整合法新 row → 整行覆盖
network 未出现旧 timestamp     → 不视为删除
incoming batch 含 NULL/invalid row → 整批不写，旧值不变
```

完整 cache hit 不访问 Provider，因此历史修订只有在未来 network fetch 重新覆盖该 timestamp 时才能发现。Provider 删除或 gap 内回填不保证自动传播；必要时人工重置 cache。

## 10. DuckDB 物理边界

DuckDB 是 embedded library，不需要后台 process/container。目标使用 native database file，不使用 Parquet 作 online cache。

WAL 是 crash recovery log，不是时间旅行/历史备份。保留默认 WAL/automatic checkpoint，不启用 `RECOVERY_MODE no_wal_writes`；关闭 WAL 会失去崩溃恢复，且不解决主文件的空间回收。

参考：[DuckDB concurrency](https://duckdb.org/docs/stable/connect/concurrency.html)、[checkpoint](https://duckdb.org/docs/current/sql/statements/checkpoint)、[reclaiming space](https://duckdb.org/docs/current/operations_manual/footprint_of_duckdb/reclaiming_space)。
