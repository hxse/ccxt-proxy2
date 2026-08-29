# 三类 OHLCV Route 的简化缓存算法

> **Status: Implemented design.** 当前算法最多复用一个起点 prefix segment，不使用通用多缺口 Resolver。

## 1. 共同原则

```text
read at most one cache prefix
→ network-only continuation
→ validate/merge/deduplicate
→ cache decision
→ return full rows + completion metadata
```

- 一旦进入 network 阶段，不再搜索第二个 cache segment。
- Network 从已有 tail timestamp 含首请求，不计算 `tail + timeframe`。
- Binance/Kraken Futures 的 fixed interval 只用于 network page fail-fast validation，不用于 cursor 或 cache proof。
- Merge 按 time 去重，network row 在相同 timestamp 胜出。
- 完整 network operation 成功才能返回或缓存；中途 partial 必须丢弃。
- 所有用户 rows 最多 100,000，proof/overlap rows 不占用户计数。

## 2. Prefix 命中

Cache 从 candidate segments 选一个可复用 rows 最多的 segment，返回其中 `time >= since` 的 rows。

命中可以是：

```text
Exact       : since 恰好是某 row
Bracketed   : predecessor < since < successor
Leading gap : covered_from <= since < first_time
```

Leading gap 示例：

```text
segment.covered_from = 品种上市前 12:00
segment.first_time   = 品种上市时 09:00
用户 since           = 上市前稍晚的 11:00

返回从品种上市时 09:00 开始的 rows
```

无需修改用户 since 或查询独立 mapping table。

## 3. `SinceLimit`

用户意图：

```text
从 since 向 timestamp 增大方向取最多 limit 根
```

流程：

```text
prefix = cache.read_best_prefix(series, since, limit)

prefix rows >= limit
→ 返回 prefix[:limit]，metadata=true

0 < prefix rows < limit
→ 从 prefix tail 含首补拉缺少 rows + one successor

prefix 为空
→ 从原始 since 抓 limit rows + one successor
```

部分命中时，network head 必须包含 cached tail timestamp。失败时：

```text
放弃整个 prefix
→ 从原始 since 完整重拉一次
```

不得在 fallback 中无限重试同一 overlap mismatch。

### One-row lookahead

目标尾根为 `target_tail`。Lookahead 从它的 timestamp 含首请求，底层容量至少包含 anchor + 1：

```text
去重后存在 time > target_tail.time
→ last_bar_completion_confirmed=true
→ 用户目标 rows 全部可缓存

不存在严格更晚 row
→ last_bar_completion_confirmed=false
→ cache 只写 rows[:-1]
```

Proof successor 不属于用户 N 根，不返回。

### 例子

```text
用户 limit=10
返回 rows=t1..t10

观察到 t11 → response 10 根，cache t1..t10，metadata=true
未观察到 t11 → response 10 根，cache t1..t9，metadata=false
```

Metadata 只影响 cache，不会把用户请求的 10 根变成 9 根。

## 4. `SinceLatest`

用户意图：

```text
从 since 到本次请求开始时的 latest snapshot
```

为防止边拉边产生新 K 线导致无法终止，先向 Provider 获取当前最新一根：

```text
snapshot_tail
```

流程：

```text
读取一个最佳 prefix（最多 100,001 rows）
→ 有 prefix：从 cached tail 含首补到 snapshot_tail
→ 无 prefix：从原始 since 抓到 snapshot_tail
→ 抓到 snapshot_tail 才算 complete
→ 再对 snapshot_tail 做一次 lookahead
```

分页期间出现更晚 rows 不追加到 response。如果 lookahead 存在严格更晚 successor，snapshot tail metadata 为 `true`；否则 `false`。

去重后出现第 100,001 个用户 row 时立即失败，不返回/缓存 partial result。不增加 streaming 或后台任务。

### 休盘

休盘尾根可能实际已完成，但无 successor 时仍是 unknown：

```text
response 包含尾根
metadata=false
cache 不保存尾根
```

下次开盘出现 successor 后，旧尾根即成为安全 cache row。

## 5. `LatestLimit`

用户意图：

```text
取 Provider 最新倒数 limit 根
```

流程：

```text
不读 cache
→ Provider latest-limit network method
→ response 返回全部 rows
→ cache 写 rows[:-1]
```

Provider latest semantics 只证明尾根是查询当时最新 row，不证明它已完成，因此非空结果 metadata 为 `false`。

`limit+1` 会在倒数语义中多取更老 row，不能作为 latest tail successor，因此不使用。

## 6. 固定的完整 response

Cache 和 Route 使用同一个未删尾 `OhlcvResult`：

```text
metadata=true  → cache 写全部 rows，Route 返回全部 rows
metadata=false → cache 写 rows[:-1]，Route 仍返回全部 rows
metadata=null  → rows 为空
```

服务端不提供删尾开关。调用方根据 `last_bar_completion_confirmed` 自行选择是否消费未知状态的最后一根，不能让这一消费策略反向改变 cache decision 或 route row count。

## 7. Terminal/no-progress

Network method 只有在能证明语义完成时才成功返回。下列情况需要区分：

- Provider 正常返回少于 limit：可能已达当前边界；
- 请求/重试失败：异常，不得当作边界；
- 短页仅返回 overlap anchor：可以作为无 successor/terminal 证据，不是新 row；
- 非空满页合并去重后毫无进展：返回 `NETWORK_INCOMPLETE`，不将 partial rows 当作成功。

详细 Provider retry/error mapping 见 [OHLCV Route contract](../ccxt/02_ohlcv_routes.md)。
