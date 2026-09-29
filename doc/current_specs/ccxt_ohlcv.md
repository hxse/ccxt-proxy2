# CCXT OHLCV 路由与网络契约

## 三类用户语义

| 逻辑名 | 必填参数 | 结果语义 | 读 Cache | 写 Cache |
| --- | --- | --- | --- | --- |
| `SinceLimit` | `since + limit` | 从起点向后最多 N 根 | 最佳单 prefix | 是 |
| `SinceLatest` | `since` | 从起点到请求开始时的 latest snapshot | 最佳单 prefix | 是 |
| `LatestLimit` | `limit` | 固定 S 后计算起点获取最新 N 根 | 最佳单 prefix | 是 |

固定 URI 为 `/ccxt/fetch_ohlcv/since-limit`、`/ccxt/fetch_ohlcv/since-latest`、`/ccxt/fetch_ohlcv/latest-limit`。旧 `/ccxt/fetch_ohlcv` 已删除，不提供兼容转发。

## 公共参数

- `exchange_name`；
- `market`；
- `is_live`，必填布尔值，true 为 live、false 为 sandbox；
- `symbol`；
- `timeframe`；
- `variant`，默认 `default`；Binance Futures 额外支持 `mark/index/premiumIndex`；
- `enable_cache`，默认 `true`。

全部 CCXT 路由（包括三条 OHLCV）必须显式传入 is_live，没有默认环境。所选身份必须在服务白名单中启用，不自动回退；旧 mode 输入拒绝，详见 [统一环境选择](environment_selection.md)。

`enable_cache=false` 同时禁止 cache read 和 cache write，但不取消 network completeness 和 completion metadata 的计算。

公开 CCXT `since` 必须是 13 位 UTC epoch milliseconds，允许范围为 `1_000_000_000_000..9_999_999_999_999`。少一位的秒/错误毫秒值和多一位的微秒值均在 Route validation 阶段返回 422；不要求 `since` 与 timeframe bar open time 对齐，Provider 仍按其原生 since 语义选择第一根数据。

所有 CCXT GET Route 使用 strict Query Model。未声明参数（包括已删除的 `include_last` 或拼错的 `enable_cach`）统一返回 422 `extra_forbidden`，不得静默忽略。交易类 POST 的参数只允许出现在 JSON body，未知 query 参数同样返回 422；明确允许的 Provider 扩展字段仍按各 request body model 的 `extra` policy 处理。

## 响应结构

三条查询 Route 固定返回 JSON，没有 CSV/Parquet 导出或流式响应分支。CSV 可由调用方从 JSON 机械转换。

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

## 响应行数上限

三条 Route 共用：

```text
max_response_rows = 100_000
```

- `SinceLimit`/`LatestLimit` 在 Route 入口拒绝 `limit > 100_000`。
- Client public method 也维持该不变量，避免被非 HTTP 调用绕过。
- `SinceLatest` 在 cache read 和 network pagination 中累计去重后的用户 rows，第 100,001 根触发 `ResponseRowLimitExceeded`。
- 超限时不返回、不缓存 partial result。
- overlap anchor 去重后只计一次；溢出检测容量不作为成功响应多余行。

Cache 可通过多次请求累积到远高于 100,000 行；response budget 与 cache capacity 不联动。

## 标准 OHLCV 数据

```text
time: Int64 UTC epoch milliseconds, K-line open time
open/high/low/close/volume: Float64
```

结果必须按 `time` 严格升序且唯一。价格/volume 不得为 NaN/Infinity/NULL，volume 非负；不保存 Provider raw response 或动态列。

## 手动分页

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

项目唯一请求方式为 since＋limit；需要最新边界时只取一次 since=None、limit=1 得到 S。LatestLimit 用固定 interval 推导 start 后复用同一个 S 的内部查询；不使用项目 until/to/endTime、倒序或自动分页。续页始终从真实末根含首请求，不用 tail＋interval。

非空短页有合法进展时继续；Kraken Futures SDK 可能将 since＋limit 转为有限时间窗口，不能把短页直接当作历史结束。数量查询只含重叠点的短页可正常结束；固定快照查询尚未到 S 时无进展、空页或缺连接点均失败。完整算法见[查询规范](ohlcv_cache_resolution.md)。

## 尾根持久化资格

三个普通查询均不为尾根额外请求后继。网络成功结果裁到本次边界后，目标末根统一 unknown：非空 metadata=false，空结果 null。网络窗口前面的每根由同序列更晚行证明可以持久化；分页只在整体结果上排除一次末根。

纯可信缓存命中返回 metadata=true，不重新去尾或写回。未知网络末根若已在库中，不删除、降级或用未确认新值覆盖旧行。响应始终保留完整目标窗口。

## 完整响应

三个 Route 都不存在服务端删尾参数：

```text
Provider/Client 生成完整目标窗口
→ Cache 根据 completion metadata 决定是否保存尾根
→ Route 原样返回完整 rows + metadata
```

因此数据足够时 `limit=N` 始终返回 N 根。需要“只使用有完成证据的 rows”的调用方，可以在 metadata 为 `false` 时自行忽略最后一根；服务端不替调用方执行这一策略。

## 交易所能力矩阵

| Provider | SinceLimit | SinceLatest | LatestLimit | Cache |
| --- | --- | --- | --- | --- |
| Binance USDⓈ-M linear Futures | 保证 | 保证 | 保证 | 按 Route policy |
| Kraken Futures | 保证 | 保证 | 保证 | 按 Route policy |
| Binance Spot | best-effort | best-effort | best-effort | 按当前 Route policy，无可用性承诺 |
| Kraken Spot live | Provider window 内 best-effort thin-forward | `NOT_SUPPORTED` | best-effort thin-forward | 否 |
| Kraken Spot sandbox | 配置身份不允许 | 配置身份不允许 | 配置身份不允许 | 否 |
| TQ | 不属于 CCXT Route | 不属于 CCXT Route | 不属于 CCXT Route | 单独规范 |

Binance Futures 仅支持 linear symbols，例如 `BTC/USDT:USDT`；`BTC/USD:BTC` 等 COIN-M/inverse symbol 明确拒绝。Binance/Kraken Futures 的完整分页只承诺上述连续 crypto 数据域；校验失败是显式错误，不返回 partial rows。Kraken Spot nonempty response 的 completion metadata 保守返回 `false`，空结果为 `null`。

重点维护周线及以下固定周期。Provider 已声明的 >1w 周期（含 1M），SinceLimit/LatestLimit 只允许单页且 limit 不超单页上限，不使用缓存；SinceLatest 返回 NOT_SUPPORTED。Kraken Spot 保持原能力。

项目不重点维护上市前数据。普通链路自然获得合法短历史则返回，不能完成时明确拒绝；不新增上市时间查询、起点搜索、空窗推进。LatestLimit 推导非法负起点返回 422 INVALID_PROVIDER_REQUEST，不用当前合约上市日裁掉主连历史。

## 错误契约

- 参数/row budget 超限：稳定 4xx，无 partial response。
- 未知 `variant` 或其他 Provider request 参数错误：422 `INVALID_PROVIDER_REQUEST`；Client boundary 与 HTTP schema 双重校验。
- Provider method/timeframe/market subtype capability 缺失：`NOT_SUPPORTED`。
- Kraken Spot 不额外推算 history window，只返回 CCXT thin-forward 结果。
- Binance/Kraken Futures 固定周期 page 出现非连续 timestamp 或满页 no-progress：502 `NETWORK_INCOMPLETE`，不做自动修复。
- Read-only network retry 后仍失败或未达到已固定 snapshot：返回 502，不缓存 partial rows。
- Cache read failure：warning 后当作 miss，完整走 network。
- 普通 cache write failure：记录 error，已成功 network response 仍可返回。
- Capacity eviction failure/淘汰后仍超限：rollback 并返回 HTTP 507 `CACHE_CAPACITY_EXCEEDED`，服务进程不退出。
