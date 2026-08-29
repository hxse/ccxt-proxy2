# DuckDB OHLCV Cache 测试规范

> **Status: Maintained test contract.** 旧 Parquet/proof-log tests 已删除；本文定义当前 Client/Cache/Route 的必要回归覆盖，Provider 实际行为由显式 online smoke test 补充。

## 1. 原则

- 默认测试全部离线，不访问 Binance/Kraken/TQ/Telegram。
- Provider 测试使用单页 raw API response mock，验证 Client 自分页。
- Cache 测试使用独立临时 DuckDB file，不共享 state。
- Cache 正确性断言基于实际 timestamp/order/overlap，不使用 timeframe adjacency。
- Binance/Kraken Futures network mock 在 `m/h/d/w` 上必须包含连续成功页与异常 gap 失败页；只有 Provider 实际支持的 `1M` 才可跳过固定毫秒邻接断言。
- Online/debug tests 必须显式启用，不得进入默认 CI。

## 2. CcxtClient/network tests

### 三类 Route

- `SinceLimit`：数据足够、少于 limit、恰好 page boundary、多页。
- `SinceLatest`：固定 snapshot、分页期间出现新 row 不追加。
- `LatestLimit`：无 since，Provider 最新 N 根。

### Pagination

- 第二页从上一页 tail 含首请求。
- Overlap same timestamp 合并只保留新 row。
- Binance/Kraken Futures 固定周期 page 出现非连续 timestamp 时触发 `NETWORK_INCOMPLETE`。
- 连续性校验不搜索 gap、不扩大时间窗口、不返回 partial rows。
- Page head 不含预期 anchor 时报错。
- Empty/短页 anchor-only 可作为边界；满页 no-progress 必须返回 `NETWORK_INCOMPLETE`。
- Network 中途失败不返回 partial result。
- Read-only retry 按次数生效；create/cancel/close/leverage 不自动 retry。
- Authentication/BadRequest 等 Provider rejection 不重试；非网络型 write rejection 不得误标为 `OPERATION_STATUS_UNKNOWN`。
- Binance `1M` LatestLimit 只用 `until` overlap anchor 向后分页，不使用固定 30 天窗口；Kraken 未声明 `1M` capability 时在 Client boundary 明确拒绝。

### Tail evidence

- `SinceLimit` 存在严格更晚 successor → metadata `true`。
- Lookahead 只返回 overlap anchor → `false`。
- Proof successor 不占 limit、不进 response、不作为未证明 cache tail。
- `SinceLatest` 休盘无 successor → 返回 tail + metadata `false`。
- `LatestLimit` 非空结果 metadata 始终保守 `false`。
- 分页 overlap same timestamp 不能作为 successor。

## 3. Cache read tests

- Exact since 命中。
- Bracketed since 命中，predecessor 不返回。
- Leading gap：`covered_from <= since < first_time` 命中。
- `since < covered_from` 和 `since > last_time` miss。
- 多 candidate 选可复用 rows 最多者；同数时按 freshness/tie-breaker。
- 只读一个 segment，不自动拼第二段。
- `max_rows` 硬限制 read result；`SinceLatest` 可用 100,001 检出超限。
- Candidate 和 rows 在同一 snapshot。

## 4. `covered_from` tests

- 品种上市前 since/上市时 first row 产生 leading gap proof。
- 后续更晚但仍早于 first row 的 since 复用同一 segment。
- 更早 verified request 将 coverage 从 12:00 扩到 11:00。
- `LatestLimit` 新 segment 设 `covered_from=first_time`。
- 不支持 since 权威语义的 Provider 不能创建 leading proof。
- Eviction 裁掉 segment 前缀后重置 `covered_from=new first_time`。

## 5. Write/merge tests

- 无 overlap 创建新 segment。
- 存在一个 exact timestamp overlap 即可合并。
- 只有 min/max 范围交叉、但无相同 timestamp 时不合并。
- Incoming 同时 overlap 多 segment 时合并成一个。
- Incoming valid same-time row 原子覆盖。
- Incoming missing timestamp 不删除旧 row。
- Incoming batch 含 NULL/NaN/Infinity/invalid row 时整批不写，旧值不变。
- Metadata 与 rows 同 transaction 提交，任一失败无半成品。
- 合并后 first/last/count/covered_from 与实际 rows 一致。

## 6. Cacheability/response tests

- Metadata `true` → cache 写入全部 user rows。
- Metadata `false` → cache 写 `rows[:-1]`，response 仍返回全部 rows。
- `limit=10` 在数据足够时 true/false 都返回 10 根。
- 空 result/null metadata 和单 row false 都不写空 segment。
- 完整 cache hit 的 response tail metadata 为 `true`。
- 三个 Route 始终返回完整目标 rows；completion metadata 不改变用户 row count。
- Client partial hit 必须用 network overlap revision 覆盖旧 row；若 network tail 未确认，只更新已确认前缀。
- Client leading-gap proof 可服务同一区间内更晚的 since；`SinceLatest` overlap 失败必须丢弃 prefix 后完整重拉。

## 7. Capacity/eviction tests

- Startup config 满足 `100_000 < per_series <= total`。
- Series count 按 series 内 distinct time；total 按 live `(series_key,time)`。
- Per-series 超限后淘汰到 90% watermark。
- Global 超限后跨 series 按 `(time,segment_id)` 淘汰到 90%。
- Eviction 只形成 segment prefix delete，不制造中间缺口。
- Incoming 过旧时可在同 transaction 被淘汰。
- 空 segment metadata 删除，剩余 segment metadata 重算。
- Eviction 失败整个 transaction rollback，Route 映射 507，服务继续。
- `enable_cache=false` 不读、不写、不淘汰。

## 8. Concurrency tests

- 多 reader 看到 commit 前或后的一致 snapshot，不看中间状态。
- 多 writer 通过同 database-path lock 串行，不是每个 object 各一把锁。
- 不同 Python threads 不共享同一 DuckDB connection。
- Cache close 阻止新 reader，并等待 active reader 完成后才关闭 connections。
- Network latency 不持 DuckDB lock。
- 并发 CCXT calls 经 per-client lock 串行底层 attempt，pagination 页间不持锁。
- CCXT close 与底层 attempt 使用同一 lock；close 后旧 Client 引用返回 `PROVIDER_CLIENT_CLOSED`，不得调用 Provider。

## 9. Router tests

- 三个 request schema 不能产生模糊组合。
- `limit <= 100_000`；`SinceLatest` 第 100,001 根失败且无 partial response。
- Provider capability error 映射稳定。
- Binance inverse 在所有带 symbol 的 public method 前置拒绝；其他未知 symbol 映射 `INVALID_PROVIDER_REQUEST`。
- `mark/index/premiumIndex` 传给 Provider，并使用互不混淆的 cache series；未知 variant 在 Client boundary 拒绝。
- Cache read failure fallback network；普通 write failure 不丢 network response；capacity failure 返回 507。
- Route 只调 `CcxtClient`，没有裸 CCXT/SQL/Provider branch。

### Lifecycle/error mapping

- `ExchangeManager` reinitialize/shutdown 关闭旧 Client 和 DuckDB connections；close 幂等。
- 重复 whitelist identity 在配置阶段拒绝。
- CCXT request/order/auth/funds/operation error 映射为稳定、脱敏的 HTTP code；Provider 原文不进入 response。
- `fetch_market_info.leverage` 没有可靠 position 证据时为 `null`，position fetch 失败不得伪装成 leverage 1。
- Binance normal/conditional order list 合并时按 ID 去重；Kraken Futures 与 Binance Spot 不进入该分支。
- Exchange factory 固定 Binance Futures `linear` market filter、sandbox demo mode，以及 Kraken Futures/Spot 对应的不同 CCXT class。

### Repository/Bruno safety contracts

- Bruno GET URL query 与 `params:query` 完全一致，request-specific 值不得回流 environment。
- OHLCV Request model、Client signature、OpenAPI 和 Bruno 均不得重新暴露服务端删尾参数。
- 每个 Bruno method/path 必须对应现有 FastAPI route；Just 引用的 `.bru` 路径必须存在。
- Mutating Bruno request 必须带 `[STATEFUL]`；`bru-readonly-basic` 只能引用 GET request。
- `Test/online` 禁止 create/cancel/close/set/send 等 mutating call，`test-online` 不得包含 Telegram。
- Cache package 不 import CCXT/TQSDK/FastAPI，公开 API 只保留 `read_best_prefix`、`write_segment`、`close`。
- 生产模块保持每文件不超过 400 行；已删除的平行 CCXT module 不得重新出现。

## 10. Migration tests

- 新系统不读旧 Parquet/proof log。
- 不存在新旧 public cache entry 并行。
- 旧 cache tests 只在 cutover 前保留，不得为新实现伪造 compatibility。
- `uv lock --check`、默认 offline tests 和新增 target tests 通过后才完成 cutover。

## 11. Read-only online smoke

`just test-ccxt-online` 只调用已启用 Binance/Kraken Futures 的只读能力：三种 OHLCV 模式、Binance `mark/index/premiumIndex`、ticker、market info、balance、positions 以及订单/成交历史查询。公开行情覆盖全部 whitelist identities；私有账户查询每个 Provider 选择一个已启用 identity，并优先 sandbox，避免同一 Provider 的备用账户配置重复决定代码集成测试结果。OHLCV 强制 `enable_cache=false`，测试进程使用独立临时 DuckDB，不争用运行中服务的 cache file。该入口不调用下单、撤单、平仓、设置杠杆或设置保证金模式；Online test 不进入默认 CI。

## 12. Test entry boundaries

- 裸 `pytest` 和 `just test` 都只运行 `Test/` 中的 offline tests，并忽略 `Test/online`。
- `just test-online` 是只读 online 聚合入口，仅执行 CCXT 与 TQ 查询；按 Provider 可使用 `just test-ccxt-online`、`just test-tq-online`。
- Telegram send 不属于 online test；真实发送只能通过 `just debug-telegram-stateful` 显式执行。
- `debug/route_tests` 会撤单/下单或修改 sandbox settings，标记为 `stateful`，不属于默认或普通 online suite。即使显式传给 pytest，也必须先设置 `CCXT_STATEFUL_DEBUG=1` 才会创建应用 Client；只应通过 `just debug-route-test(s)` 等明确入口运行。
- Stateful order test 只取消本测试创建的订单，不调用全账户 `cancel_all_orders`；closed-order history 只作 route smoke，不依赖短时间最终一致性。

完整迁移验收见 [迁移与验收](../architecture/02_migration_and_acceptance.md)。
