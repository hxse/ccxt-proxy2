# 破坏性迁移、测试与验收

> **Status: Implemented migration record.** `CcxtClient + DuckDbOhlcvCache` 已按本文完成破坏性切换；本文保留实施顺序与验收边界。

## 1. 迁移原则

- 这是破坏性 cutover，不保留旧 cache compatibility layer。
- 先固定 domain/HTTP contract，再改 Client，最后替换 storage。
- 迁移期内可以有短期 private 过渡 helper，最终 release 不保留两套 public 路径。
- 任何正确性都不依赖旧 Parquet/proof-log 数据。
- 代码实施后才将 target 标记改为 implemented。

## 2. 实施阶段

### Phase 1：HTTP/domain contract

1. 定义 `SinceLimit`、`SinceLatest`、`LatestLimit` request model。
2. 定义 `OhlcvResult(rows, last_bar_completion_confirmed)` response envelope。
3. 固定“response 始终包含目标尾根”与 `max_response_rows=100_000`。
4. 定义 `NOT_SUPPORTED`、`ResponseRowLimitExceeded`和 cache capacity 错误。

### Phase 2：CcxtClient 收口

1. `ExchangeManager` 改为返回长期存活的 `CcxtClient`。
2. 实现 Binance USDⓈ-M linear Futures 三个 OHLCV network method 和手动 overlap pagination，明确拒绝 inverse。
3. Kraken Futures 与 Binance 共用完整三模式；固定周期非连续 page/满页 no-progress 均 fail-fast，不自适应补拉；Spot 只 best-effort，Kraken Spot sandbox 不支持。
4. 迁移 balance、ticker、market、order、trade、position、leverage/margin method。
5. 将 Binance normal/conditional order 合并与 fallback 迁入 Client private method。
6. 加入 per-client request lock；只读 retry，非只读不自动 retry。

### Phase 3：DuckDB cache

1. 建立 schema/version 初始化。
2. 实现 `read_best_prefix()` 和 exact/bracketed/leading-gap 命中。
3. 实现 successor-aware `write_segment()`。
4. 实现 exact-overlap multi-segment merge。
5. 实现 per-series/total capacity 和最旧前缀 eviction。
6. 加入 per-database write lock、thread-local connection/pool 与 transaction rollback。

### Phase 4：Route cutover

1. 新三路由只调用 `CcxtClient`。
2. `enable_cache=false` 同时禁止 read/write cache。
3. 固定 response metadata 与 error mapping。
4. 更新 Bruno、OpenAPI description 和调试入口；operational debug 只调用 `CcxtClient`，隔离的 Provider research 也必须复用生产 registry/factory，不复制 exchange construction。

### Phase 5：TQ DataFrame engine

1. TQ Route 与 request/response 不变。
2. `tq_data_source` 直接处理 TqSdk Pandas DataFrame。
3. 保留 TqManager singleton 和 FileLock。
4. 新增 `pandas` direct dependency，删除 TQ 对 Polars 的依赖。

### Phase 6：删除旧路径

删除：

- 整个旧 `cache_tool` Parquet/proof-log 实现；
- `get_ohlcv_with_cache`、`FetchCallback`、`LogEntry` 和 continuity/compact/corruption flow；
- `src/tools/ccxt_utils.py`、`ccxt_utils_extended.py`、`binance_adapter.py` 等已迁移 facade；
- cache 内的 Provider page limit、network callback 和 pagination；
- Polars/Parquet/Arrow 仅为旧 cache 保留的 import/dependency；
- 过时 cache tests 和已被正式文档取代的临时设计材料。

删除 dependency 前必须 `rg` 全项目实际 import。`filelock` 因 TQ 仍然保留。

## 3. 数据迁移

- 旧 Parquet 和 proof log 不导入 DuckDB。
- 不读旧格式，不提供 migration command。
- 旧 cache 是可重建数据，可在确认精确目录后废弃。
- 不在代码切换中自动递归删除未解析目录。
- DuckDB schema 需要独立 schema version，但第一版不做 legacy upgrade chain。

## 4. 测试分层

### Client/network

- 三类 Binance route，冷 cache 与自分页。
- 每页 inclusive overlap、duplicate 新值胜出、no-progress、retry。
- Binance/Kraken Futures 固定周期连续页成功，异常 gap/满页 no-progress 返回 `NETWORK_INCOMPLETE`；只有支持 `1M` 的 Provider 跳过固定毫秒校验。
- Provider 不支持的 timeframe 返回 `NOT_SUPPORTED`；Binance Futures inverse 和 Kraken Spot sandbox 明确拒绝。
- Binance Futures 的 OHLCV、market、order、trade、position 和 settings method 共用 linear symbol-scope 校验；不能让 inverse 在非 OHLCV Route 退化成泛化 500。
- `mark/index/premiumIndex` 既验证 Provider params，也验证 cache series identity；任意未知 variant 在 Client boundary 拒绝。
- `SinceLimit`/`SinceLatest` proof successor 不返回用户。
- `SinceLatest` 固定 snapshot，不追赶更新 rows。
- Kraken capability error 和非只读 no-retry。
- 并发底层 CCXT call 由 per-client lock 串行，pages 之间可插入其他 call。
- Client close 与 attempt 共用同一 lock；旧 Client 引用在 close 后稳定返回 503，不会重新发 Provider request。

### Cache

- exact、bracketed、leading-gap 命中；predecessor 不返回。
- 完整 hit、single prefix、overlap mismatch fallback。
- 两段无 exact timestamp overlap 不合并；一批 overlap 多段时原子合并。
- valid incoming row 原子覆盖；含 NULL/invalid row 的 batch 不写且不删除旧值。
- 容量计数、90% watermark、per-series/global eviction。
- eviction 只删前缀，重置 `covered_from`；失败 rollback/507。
- 并发 read snapshot 和串行 write transaction。
- DuckDB close 阻止新 reader，并等待 active reader 完成后再关闭 thread-local connections。

### Router

- 三路由构造正确 Client call。
- 100,000 行限制在 cache read/network accumulation 中都生效。
- `last_bar_completion_confirmed` 只描述 response 尾根证据，不改变 row count。
- 三路由始终返回完整目标窗口；是否消费未知尾根由调用方决定。
- Cache read/write failure、capacity failure、network failure 映射稳定。
- Provider exception taxonomy 映射稳定且 HTTP message 脱敏；非只读 network failure 返回 `OPERATION_STATUS_UNKNOWN`。

### TQ

- Pandas placeholder trim、dtype、time axis、NaN/Infinity、wide-to-long。
- TQ FileLock 和 singleton lifecycle 不回归。
- TQ 不调用 DuckDB cache。

## 5. 验收清单

1. Route 不再访问裸 CCXT instance。
2. 所有 CCXT public 能力只有一个 Client 入口。
3. Cache 不 import CCXT/TQSDK/FastAPI，也不接受 callback。
4. 三类 Route 的 read/write 策略与 response 语义与规范一致。
5. fixed interval 只作 Binance/Kraken Futures page 的 fail-fast validation；不构造 cursor，不用本机时间证明尾根完成。
6. Cache 中没有无 successor 证据的尾根。
7. DuckDB merge/eviction 在单 transaction 内原子完成。
8. CCXT 和 DuckDB 使用两把不同的 process-local lock。
9. TQ 对外行为不变，内部不再依赖 Polars。
10. 旧 facade、Parquet/proof、callback 和过时测试被删除。
11. `uv lock --check`、默认离线测试和目标新增测试全部通过。
12. 上线前至少对已启用的 Binance/Kraken Futures live identity 运行一次三模式 public market-data online smoke test；private account credential 不作为该 suite 的通过条件，sandbox 只用于 `just debug*`，非只读 online test 不属于本次验收。
13. Lifespan shutdown/reinitialize 会关闭 CCXT、DuckDB、TQ 和 Telegram resources；重复 whitelist identity 在配置阶段拒绝。
14. 裸 `pytest`/`just test` 只运行 offline suite；只读 live online 与会修改 sandbox 的 stateful debug suite 必须使用各自显式 opt-in 入口。

## 6. 已固定的实施细节

- HTTP Route：`/ccxt/fetch_ohlcv/since-limit`、`/ccxt/fetch_ohlcv/since-latest`、`/ccxt/fetch_ohlcv/latest-limit`。
- `series_key`：字段排序的 compact JSON；DuckDB schema version 为 `1`。
- 默认容量：per-series `2_000_000`、global `20_000_000`。
- canonical segment tie-breaker：`row_count DESC, segment_id ASC`。
- DuckDB connection：thread-local connection；同 database path 共享 process-local write lock。
- database path：`ohlcv_cache.database_path`，默认 `./data/cache/ohlcv.duckdb`。
