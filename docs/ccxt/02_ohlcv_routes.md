# CCXT OHLCV Route 与 Network Contract

> **Status: Implemented design.** 三个 Route、参数与 response envelope 已落地。

## 1. 三类用户语义

| 逻辑名 | 必填参数 | 结果语义 | 读 Cache | 写 Cache |
| --- | --- | --- | --- | --- |
| `SinceLimit` | `since + limit` | 从起点向后最多 N 根 | 最佳单 prefix | 是 |
| `SinceLatest` | `since` | 从起点到请求开始时的 latest snapshot | 最佳单 prefix | 是 |
| `LatestLimit` | `limit` | 最新倒数 N 根 | 否 | 是 |

固定 URI 为 `/ccxt/fetch_ohlcv/since-limit`、`/ccxt/fetch_ohlcv/since-latest`、`/ccxt/fetch_ohlcv/latest-limit`。旧 `/ccxt/fetch_ohlcv` 已删除，不提供兼容转发。

## 2. 公共参数

- `exchange_name`；
- `market`；
- `mode`；
- `symbol`；
- `timeframe`；
- `variant`，默认 `default`；Binance Futures 额外支持 `mark/index/premiumIndex`；
- `enable_cache`，默认 `true`。

`enable_cache=false` 同时禁止 cache read 和 cache write，但不取消 network completeness 和 completion metadata 的计算。

## 3. Response envelope

```python
OhlcvResult(
    rows: list[OhlcvRow],
    last_bar_completion_confirmed: bool | None,
)
```

HTTP 示例：

```json
{
  "rows": [[1718000000000, 100.0, 105.0, 99.0, 104.0, 12.5]],
  "last_bar_completion_confirmed": false
}
```

Metadata 不是 OHLCV 第七列：

- `true`：已获得返回尾根完成的正面证据；
- `false`：无法确认，不代表已确认未完成；
- `null`：`rows` 为空，没有可描述的尾根。

字段本身不改变用户 row count。Provider 数据足够时，`limit=10` 总是返回包含目标尾根的 10 根。

## 4. Response row budget

三条 Route 共用：

```text
max_response_rows = 100_000
```

- `SinceLimit`/`LatestLimit` 在 Route 入口拒绝 `limit > 100_000`。
- Client public method 也维持该不变量，避免被非 HTTP 调用绕过。
- `SinceLatest` 在 cache read 和 network pagination 中累计去重后的用户 rows，第 100,001 根触发 `ResponseRowLimitExceeded`。
- 超限时不返回、不缓存 partial result。
- overlap anchor 和 proof successor 不计入用户行数。

Cache 可通过多次请求累积到远高于 100,000 行；response budget 与 cache capacity 不联动。

## 5. Canonical OHLCV

```text
time: Int64 UTC epoch milliseconds, K-line open time
open/high/low/close/volume: Float64
```

结果必须按 `time` 严格升序且唯一。价格/volume 不得为 NaN/Infinity/NULL，volume 非负；不保存 Provider raw response 或动态列。

## 6. 手动分页

不使用 CCXT automatic pagination。Client 只调用单页 API，自己负责：

```text
Provider page
→ 校验 Provider timeframe capability
→ 校验 schema/order/fixed-interval continuity
→ 下一页从已验证 tail timestamp 含首请求
→ merge
→ same time 使用新 row
→ no-progress/terminal check
```

Binance 和 Kraken Futures 的完整分页明确以“正常 crypto OHLCV 在固定周期上连续”为支持前提。对 `m/h/d/w` 固定周期，canonical page 中相邻 timestamp 的差必须等于 `timeframe`；出现跳跃立即返回 `NETWORK_INCOMPLETE`，不做缺口搜索、时间窗口扩张或自适应补拉。非空满页未产生新 row 同样视为 `NETWORK_INCOMPLETE`；短页只含 overlap anchor 则可作为历史/最新边界。

Client 先检查 `timeframe in exchange.timeframes`。`1M` 是自然月，长度不固定；只有 Provider 本身支持时才可请求，且不做固定毫秒邻接校验。当前 Kraken Futures/Spot 均不支持 `1M`。

`timeframe` 只用于上述 fail-fast validation，不用 `tail + timeframe_ms` 构造下一页 cursor，也不用于证明尾根完成。同 timestamp 的 overlap row 只用于连接校验，绝不能冒充“更晚 successor”。

单个 network method 内部可有多个 page，但 Client 只在完整合并和校验后向 cache/Route 返回一个 result。不存在“每页 pending tail 落盘”。

## 7. Tail completion evidence

唯一正面证据是同一已验证 Provider sequence 中存在：

```text
successor.time > response_tail.time
```

这证明尾根已不再是 active interval，不保证 Provider 永不修订历史数值。

### SinceLimit

对用户目标 N 根做 one-row lookahead。由于 request 从 target tail 含首 overlap，底层容量至少为 `anchor + 1`。去重后存在严格更晚 row 则 metadata 为 `true`；否则为 `false`。

### SinceLatest

请求开始时先固定 `snapshot_tail`，分页只追到它。到达后再以 snapshot tail 含首 lookahead；严格更晚 row 只作证据，不追加到已固定 response。

### LatestLimit

Provider 可确认返回尾根是查询当时的 latest，但无法证明它已完成，因此非空结果 metadata 为 `false`。此路由的 `limit + 1` 只会多取一根更老 row，不能作为 successor。

休盘时尾根可能实际已完成，但没有 successor 仍标记 `false`；response 正常返回它，cache 不保存它。下次开盘出现 successor 后旧尾根自然进入 cache。

## 8. Proof successor 的去向

Proof row：

- 不计入用户 limit；
- 不返回用户；
- 不作为本次新的未证明 cache tail 保存。

Cache 使用 metadata 选择：

```python
cache_rows = rows if confirmed else rows[:-1]
```

Route 始终返回完整 `rows` 与对应 metadata。Cache 是否保存尾根与用户是否消费尾根是两个维度；调用方可根据 `last_bar_completion_confirmed` 自行决定如何使用最后一根。

## 9. 固定的完整 response

三个 Route 都不存在服务端删尾参数：

```text
Provider/Client 生成完整目标窗口
→ Cache 根据 completion metadata 决定是否保存尾根
→ Route 原样返回完整 rows + metadata
```

因此数据足够时 `limit=N` 始终返回 N 根。需要“只使用有完成证据的 rows”的调用方，可以在 metadata 为 `false` 时自行忽略最后一根；服务端不替调用方执行这一策略。

## 10. Provider matrix

| Provider | SinceLimit | SinceLatest | LatestLimit | Cache |
| --- | --- | --- | --- | --- |
| Binance USDⓈ-M linear Futures | 保证 | 保证 | 保证 | 按 Route policy |
| Kraken Futures | 保证 | 保证 | 保证 | 按 Route policy |
| Binance Spot | best-effort | best-effort | best-effort | 按当前 Route policy，无可用性承诺 |
| Kraken Spot live | Provider window 内 best-effort thin-forward | `NOT_SUPPORTED` | best-effort thin-forward | 否 |
| Kraken Spot sandbox | 配置身份不允许 | 配置身份不允许 | 配置身份不允许 | 否 |
| TQ | 不属于 CCXT Route | 不属于 CCXT Route | 不属于 CCXT Route | 否 |

Binance Futures 仅支持 linear symbols，例如 `BTC/USDT:USDT`；`BTC/USD:BTC` 等 COIN-M/inverse symbol 明确拒绝。Binance/Kraken Futures 的完整分页只承诺上述连续 crypto 数据域；校验失败是显式错误，不返回 partial rows。Kraken Spot nonempty response 的 completion metadata 保守返回 `false`，空结果为 `null`。

## 11. Error contract

- 参数/row budget 超限：稳定 4xx，无 partial response。
- 未知 `variant` 或其他 Provider request 参数错误：422 `INVALID_PROVIDER_REQUEST`；Client boundary 与 HTTP schema 双重校验。
- Provider method/timeframe/market subtype capability 缺失：`NOT_SUPPORTED`。
- Kraken Spot 不额外推算 history window，只返回 CCXT thin-forward 结果。
- Binance/Kraken Futures 固定周期 page 出现非连续 timestamp 或满页 no-progress：502 `NETWORK_INCOMPLETE`，不做自动修复。
- Read-only network retry 后仍失败或未达到已固定 snapshot：返回 502，不缓存 partial rows。
- Cache read failure：warning 后当作 miss，完整走 network。
- 普通 cache write failure：记录 error，已成功 network response 仍可返回。
- Capacity eviction failure/淘汰后仍超限：rollback 并返回 HTTP 507 `CACHE_CAPACITY_EXCEEDED`，服务进程不退出。
