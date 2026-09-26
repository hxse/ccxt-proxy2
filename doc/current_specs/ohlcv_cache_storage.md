# OHLCV 缓存存储与覆盖证明

## 边界

`DuckDbOhlcvCache` 只负责：

- embedded DuckDB schema/lifecycle；
- parameterized SQL；
- 查找/读取最佳 prefix segment；
- 按 exact timestamp overlap 新建/合并 segment；
- 统一尾根资格筛选；
- transaction、capacity 和 eviction。

禁止：

- import CCXT、TQSDK 或 FastAPI；
- network/retry/pagination；
- Provider page limit/params；
- callback 或 Provider client reference；
- 用 timeframe/本机时间估算连续性或尾根状态。

## 标准数据行

```text
time: BIGINT      # K-line open time；CCXT 毫秒，TQ 纳秒
open: DOUBLE
high: DOUBLE
low: DOUBLE
close: DOUBLE
volume: DOUBLE
```

TQ 另存可空 sdk_id、open_oi、close_oi；symbol 和秒制 duration 从序列身份恢复。不保存 Provider raw response、`__batch_id__` 或任意动态 JSON 列。`time` 在 segment 内唯一，读取结果严格升序。

仅接受全部 rows 均完整合法的 incoming batch。如新 row 与旧 row 同 timestamp，新 row 原子覆盖全部 OHLCV；不逐字段 coalesce。batch 中出现 NULL/NaN/Infinity/invalid row 时，本次 cache write 整体 no-op，旧值不变，避免为被拒绝的真实 row 建立虚假 gap proof。

## 数据序列身份

`series_key` 至少包含：

```text
provider / mode / market / symbol / timeframe / variant
```

它必须编入所有影响数据内容的参数，例如 Binance mark/index/premium-index variant。不同 series 在同 timestamp 上的 rows 是不同 identity。

Canonical encoding 使用字段排序、无多余空白的 JSON；schema version 为 `4`。编码 deterministic，且不包含与数据无关的用户请求参数。

## 逻辑表结构

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
data_kind: VARCHAR  # 普通行情为 ohlcv
time_unit: VARCHAR  # ms 或 ns
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
sdk_id: BIGINT NULL
open_oi: DOUBLE NULL
close_oi: DOUBLE NULL
PRIMARY KEY(segment_id, time)
```

`cache_segments` 是业务模型，不是外部 JSON index/proof log。DuckDB physical row group 可以重组，但 `segment_id` 业务边界不得因此改变。

## `segment_id`

`segment_id` 是对 Route/用户不可见的 `BIGINT`，由 DuckDB sequence 产生，PRIMARY KEY 保证 live uniqueness。

ID 不表达：

- 时间顺序；
- freshness/优先级；
- 数据供应方；
- 用户 request identity。

ID 不要求连续，正常运行不主动复用。

## 逻辑片段

一个 segment 代表经过验证且可以连续复用的数据链，可能由多次完整获取通过 inclusive overlap 合并形成。它不是单次请求身份；仅贴上同一个 segment_id 不能为任意输入补上完整性证据。Cache schema 本身不编码 `timeframe` 邻接证明，只信任 Client 交付的有效 rows。三类证据的分界见[行情数据契约](market_data_contract.md)。

当前可写 Cache 的 Binance/Kraken Futures 完整分页会在 Client 层拒绝 `m/h/d/w` 内部异常 gap。Cache 不重复校验、不解释 gap 原因，也不使用 `timeframe` 修复 rows。

## `covered_from`

`covered_from` 是 fetch chain 已证明的请求下界：

```text
covered_from <= first_time
[covered_from, first_time) 没有 Provider rows
```

示例：

```text
用户 since：2026-09-01 12:00 UTC
Provider first row：2026-09-02 09:00 UTC（品种上市）

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

例如原证明从 2026-09-01 12:00 UTC 覆盖至次日上市时刻，新请求从 9 月 1 日 11:00 UTC 仍首次返回次日同一根数据，则覆盖下界向前扩展一小时。

只有能保证“遵循 since 并返回起点后最早 rows”的 network method 可以提供 `verified_covered_from`。固定周期 `LatestLimit` 以推导起点进入 since＋limit 链，可沿该链保存已证明下界；仅最新窗口没有 since 时只能以 first_time 作为下界。Kraken Spot thin-forward 不写 cache。

## 片段合并

只有两个 timestamp 集合交集非空才算 overlap。Min/max 时间范围交叉不等于 overlap，因为内部 gap 可能是自然休盘。

Incoming 同时 overlap 多个 segments 时，在一个 transaction 中全部合并为 canonical segment。Canonical 优先选 `row_count` 最大者以减少移动，相同时使用 deterministic tie-breaker。

无 exact overlap 的 segments 继续分开。Read path 不跨 segment 解多个缺口。

## 历史修订契约

当前缓存是保守的 append/upsert cache，不是 Provider mirror：

```text
同 timestamp 出现完整合法新 row → 整行覆盖
network 未出现旧 timestamp     → 不视为删除
incoming batch 含 NULL/invalid row → 整批不写，旧值不变
```

完整 cache hit 不访问 Provider，因此历史修订只有在未来 network fetch 重新覆盖该 timestamp 时才能发现。Provider 删除或 gap 内回填不保证自动传播；必要时人工重置 cache。

## DuckDB 物理边界

DuckDB 是 embedded library，不需要后台 process/container。当前使用 native database file，不使用 Parquet 作 online cache。

WAL 是 crash recovery log，不是时间旅行/历史备份。保留默认 WAL/automatic checkpoint，不启用 `RECOVERY_MODE no_wal_writes`；关闭 WAL 会失去崩溃恢复，且不解决主文件的空间回收。

自动 checkpoint 可回收部分删除空间供后续写入复用；这不保证每次删除后主文件立即缩至最小。逻辑保留数量、实际文件占用与运行空闲空间预算须分开说明，不把预留空间计成常驻 WAL 开销。当前容量淘汰不执行整库复制或自动物理压缩。

参考：[DuckDB concurrency](https://duckdb.org/docs/stable/connect/concurrency.html)、[checkpoint](https://duckdb.org/docs/current/sql/statements/checkpoint)、[reclaiming space](https://duckdb.org/docs/current/operations_manual/footprint_of_duckdb/reclaiming_space)。

## TQ 类型承载与升级

TqOhlcvSeries 固定 provider=tq、mode=live、market=future，完整 symbol 独立成序列；timeframe 为秒数加 s。空复权为 default，FORWARD/F 为 F，BACK/B 为 B。TqOhlcvBatch 提交稳定 records 和末根资格；SDK id 非负整数，同一批次严格递增且相邻 id 差一；nullable OI 可落盘，核心价格/volume 不完整时整批不写。普通 TQ HTTP 通过高级接口接入此存储能力。

schema 1→2 在一个事务内增列并补旧片段为 ohlcv/ms，保留全部旧行、segment_id、covered_from、索引及 sequence；失败回滚，未知版本拒绝，不自动删库重建。series_key 的 kind/unit 冲突拒绝提交。所有普通行情计数、合并、刷新、淘汰限定 ohlcv，不能触碰其他数据类型。

日期片段由 schema 2→3 新增，继续共享片段选择、事务、读写生命周期；表结构与类型化证明见 [TQ 元数据](tq_metadata.md)。旧版不能写新版 schema，升级失败不删库重建。

过渡窗口由 schema 3→4 增加独立类型化表，与普通行情共享尾根筛选，但采用整窗原子替换，不通过普通片段吸收拼长。字段和证据见 [TQ 换月过渡](tq_transition.md)。

当前 schema 为 5，兼容已生成但尚无逐日 roll_date 关联的 schema 3/4。升级在同一事务中补列和节点复合主键，保留原行、片段、覆盖、源事实及过渡窗口。旧映射行无法证明其真实节点时保留 NULL 关联，完整读取按 miss 重新获取官方结果后原子补齐，不根据旧上下文猜节点。已有正确关联保持不变；失败回滚版本和结构，不删除或重建整个库。
