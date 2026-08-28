# 总体架构与 Provider Client

> **文档状态：Ideas archive，非实施计划。** 这是一个未来可能考虑、但由于复杂度过高而更可能不会实施的 TODO；本文仅保留设计思路，不代表当前架构决策或开发排期。

## 1. 背景与目标

当前项目中，交易所请求、OHLCV 分页、缓存命中判断、缓存写入和路由适配存在明显耦合：

- Binance、Kraken 调用逻辑分散在多个平行模块；
- ExchangeManager 返回裸 CCXT instance；
- 当前 cache entry 同时决定缓存缺口、Provider page limit 和网络分页；
- 当前缓存入口主要表达 `start_time + count`；
- TQ 的时间单位、字段和数据能力与 CCXT 不同。

本次重构目标：

1. Provider 请求与本地缓存彻底解耦；
2. Binance、Kraken、TQ 通过统一 FetchOperation protocol 接入 CacheEngine；
3. Provider Client 负责网络、分页、只读重试、上游限制和规范化；
4. CacheStore 只负责 Parquet、proof、分区、统计和本地锁；
5. CacheEngine 原生支持三个 RouteIntent；
6. CacheEngine 支持任意数量的离散 proven spans，尽量复用全部片段；
7. 不依赖固定时间间隔证明连续性；
8. 所有交易所方法收口到各自 Client class，删除平行 facade。

---

## 2. 核心模型

本方案明确区分：

```text
RouteIntent != FetchOperation
```

### 2.1 RouteIntent

描述用户最终想得到什么：

```text
SinceLimitIntent
LatestLimitIntent
SinceLatestIntent
```

### 2.2 FetchOperation

描述 CacheEngine 当前需要 Provider 完成什么：

```text
FullQuery
AfterCount
BeforeCount
```

CacheEngine 的决策关系是：

```text
RouteIntent × CacheTopology
        ↓
FetchOperation sequence
```

不能只靠 callback identity 猜测 RouteIntent，因为部分缓存时的缺口操作通常与原始路由需求不同。

---

## 3. 已确认的行为原则

### 3.1 Provider 自己处理原始 API

不使用 CCXT automatic pagination。CCXT 只作为单次上游 API adapter。

Provider Client 负责：

- 将 `FullQuery/AfterCount/BeforeCount` 转为具体 API 参数；
- 根据单页限制自行分页；
- 分页首尾重叠；
- 合并并按 time 去重；
- API 无法精准表达逻辑能力时，执行估算和补拉；
- 返回明确的完成状态。

CacheEngine 可以为多个缓存缺口多次调用 FetchOperation，但不参与 Provider 原始 page limit 和 retry。

### 3.2 不逐个原始 API page 写缓存

Provider 的一次 FetchOperation 内部可以包含多个上游 page。Provider 合并完成后返回一个 FetchResult，CacheEngine 再统一处理。

不引入“每个原始请求页保存 pending tail”机制。

### 3.3 `include_last` 只做机械删除

`include_last` 属于顶层 RouteIntent。

```python
result = merged_final_result

if not intent.include_last:
    result = result[:-1]
```

不判断最后一根是否已结束，不根据交易时间、interval 或 Provider 类型做复杂判断。

内部 FetchOperation 必须保留完整首尾边界，最终结果合并后只应用一次 `include_last`。

### 3.4 持久缓存不保存最终网络尾部

CacheEngine 在一次 resolve 中累计所有 network data，最终统一排除最后一根后持久化。

```text
network_data：[1,2,3,4]
cache_data：[1,2,3]
```

中间 FetchOperation 不单独删除尾部，否则可能破坏缓存片段之间的 overlap。

### 3.5 连续性由重叠证明

严禁以如下规则判断连续：

```python
next_time == previous_time + interval
```

正确原则：

```text
上一段最后一根 == 下一段第一根
```

因此周末、节假日、午间休盘和停牌不会被误判为缓存缺口。

---

## 4. 目标架构

```text
FastAPI Router
    │
    ├── 账户/订单/持仓
    │       └── ProviderRegistry → Provider Client
    │
    └── OHLCV
            └── OhlcvCacheEngine 三个公开入口
                    │
                    ├── RouteIntent
                    ├── ProofSpanIndex / CacheStore
                    ├── ForwardWalker / BackwardWalker
                    ├── CacheEstimator
                    └── ProviderClient.execute(FetchOperation)
```

| 层 | 负责 | 不负责 |
| --- | --- | --- |
| Router | HTTP 参数、鉴权、response、异常映射 | 缓存规划、Provider 分页 |
| Provider Client | 网络、认证、API translation、分页、只读 retry | Parquet、proof、缓存命中 |
| CacheEngine | RouteIntent、缓存拓扑、FetchOperation、合并、最终 include_last | CCXT/TQSDK 细节 |
| CacheStore | 本地 rows、proof spans、统计、锁 | 网络、Provider、HTTP |

---

## 5. Provider Client

建议使用：

- `BinanceClient`
- `KrakenClient`
- `TqClient`

每个 `(provider, market, mode)` 对应一个 Client instance。ProviderRegistry 返回 Client，不再返回裸 exchange。

### 5.1 BinanceClient

```python
class BinanceClient:
    # 三个完整 Route fetch 能力
    def fetch_since_limit(...): ...
    def fetch_latest_limit(...): ...
    def fetch_since_latest(...): ...

    # CacheEngine operation dispatcher
    def execute(self, operation: FetchOperation) -> FetchResult: ...

    # 其他行情、账户和交易方法
    def fetch_tickers(...): ...
    def fetch_balance(...): ...
    def fetch_positions(...): ...
    def create_market_order(...): ...
    def create_limit_order(...): ...
    def close_position(...): ...
    def cancel_order(...): ...
```

现有 Binance stop-order patch、订单合并和 fallback 查询全部迁入 BinanceClient。

### 5.2 KrakenClient

KrakenClient 保持对称外部 API，但不伪造底层不具备的能力。无法满足 FetchOperation 时返回明确 `UNAVAILABLE`。

共享层中不得再出现：

```python
if exchange_name == "binance":
    ...
```

### 5.3 TqClient

TqClient 保留 TqApi lifecycle、进程锁、wait_update 和 raw thin-forward，同时提供 canonical OHLCV 和 FetchOperation adapter。

---

## 6. Retry policy

第一阶段只对 read-only 操作重试：

- OHLCV；
- ticker；
- balance；
- positions；
- fetch orders/trades；
- market info。

第一阶段不对以下操作自动重试：

- create order；
- close position；
- cancel order/all orders；
- set leverage/margin mode。

非只读请求失败时：

```text
记录结构化错误
→ 向上抛出异常
→ 当前请求失败
→ 服务进程继续运行
```

不得吞掉异常后返回成功，也不得因为单次交易请求失败终止服务进程。创建订单 timeout 应明确记录 `operation status unknown`。
