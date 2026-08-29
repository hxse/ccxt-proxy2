# CcxtClient 与 Provider 架构

> **Status: Implemented design.** 本文定义当前 CCXT 请求、分页、交易方法和并发的唯一入口。

## 1. Client lifecycle

`ExchangeManager` 不再向 Route 返回裸 CCXT exchange，而是按以下 identity 管理长期存活的 `CcxtClient`：

```text
exchange / market / mode / credential identity
```

长期复用是为了保留 CCXT markets、HTTP session 和 instance-local rate limiter。Client 必须在 application lifespan 中初始化，不得每次请求新建 exchange instance。

```python
client = exchange_manager.get_client(
    exchange_name,
    market,
    mode,
)
```

## 2. Public API 收口

### OHLCV

```python
fetch_ohlcv_since_limit(...)
fetch_ohlcv_since_latest(...)
fetch_ohlcv_latest_limit(...)
```

### 市场、账户和交易

Client 同时封装项目已有的全部 CCXT 能力：

- tickers 和 market info；
- balance、positions、orders、trades；
- create market/limit/stop/take-profit order；
- fetch/cancel one order、cancel all orders；
- close position；
- leverage 和 margin mode。

Route 不得同时存在直接 CCXT call、`*_utils` facade 或 exchange-specific branch。可以在 `CcxtClient` 内使用 private helper，但只有 Client method 是 public provider boundary。

## 3. Capability-first

“接口名相同”不代表 Provider 能力相同。每个 public method 在请求前检查 exchange/market capability：

```text
supported   → 执行并返回 canonical result
unsupported → NOT_SUPPORTED
```

禁止为形式对称而通过 trades/CSV 临时重建一个 Provider 不具备的 OHLCV 能力。

### Binance

- 正式保证 Binance USDⓈ-M linear Futures；COIN-M/inverse 返回 `NOT_SUPPORTED`。
- Futures 三类 OHLCV Route 均由 Client 自分页完整实现。Binance Spot 保留 best-effort 路由，不作可用性承诺。
- normal order 和 conditional/stop order 需要双路查询或 fallback 的特殊行为收入 Client private method，仅对 Binance Futures 启用。
- `fetch_open_orders`、`fetch_closed_orders`、`cancel_all_orders` 需合并 normal/stop 结果。
- `fetch_order`/`cancel_order` 可在 normal call 返回明确“不存在”时改用 stop params；这是 Provider translation，不是通用 retry。

### Kraken

- 交易与账户 method 按实际 capability 开放。
- Kraken Futures 使用与 Binance 共用的手动 overlap pagination，支持三类 OHLCV Route 与 DuckDB cache。
- 两者的完整分页都以固定周期 crypto OHLCV 连续为支持前提；出现异常时间跳跃时 fail-fast，不为 Kraken 建设自适应补拉器。
- Kraken Spot OHLCV 仅 thin-forward，不自分页、不读写 DuckDB cache。
- `LatestLimit` 和 Provider window 内的 `SinceLimit` 可返回；`SinceLatest` 返回 `NOT_SUPPORTED`。
- Kraken Spot 不重建超出原生 window 的历史，不做额外 range 推断；整个 Spot 能力只是 best-effort。
- Kraken Spot 没有 sandbox API；`kraken/spot/sandbox` 在配置阶段明确拒绝，不暗中转到 live。

## 4. Retry policy

第一版只对 read-only operation 自动 retry：

- OHLCV/ticker/market info；
- balance/position；
- fetch order/trade/history。

下列操作不自动 retry：

- create order；
- close position；
- cancel order/all orders；
- set leverage/margin mode。

非只读请求失败时记录结构化上下文并向上抛出，当前 HTTP request 失败，服务进程继续。Create-order timeout 必须明确标记 `operation status unknown`，不得盲目重试造成重复下单。

## 5. CCXT request lock

同一 CCXT instance 中的 rate limiter、HTTP session 和 request state 是 mutable state。每个 Client 拥有独立：

```python
ccxt_request_lock = threading.Lock()
```

持锁范围是一次底层 attempt：

```text
acquire
→ CCXT throttle
→ sign
→ HTTP request/response
→ release
```

一次多页 OHLCV 操作不整体持锁，retry backoff 也不持锁。这使 order call 可以在 OHLCV pages 之间执行。第一版不做请求优先级队列。

`FileLock` 不适合此处：它不会共享多进程内存中的 rate-limiter timestamp/weight state。当前 single-process 部署使用 process-local `threading.Lock`。

## 6. OHLCV network boundary

Client 内部三个 network-only method 必须：

1. 将 CCXT 只当作单页 Provider adapter；
2. 自己处理 page limit、inclusive overlap 和 retry；
3. 排序、去重并转换 canonical six-column OHLCV；
4. 检测 empty/no-progress/overlap mismatch；
5. 请求前检查 `timeframe in exchange.timeframes`，不支持时报 `NOT_SUPPORTED`；
6. 对 Binance/Kraken Futures 的 `m/h/d/w` page 校验相邻 timestamp 等于固定 `timeframe`；
7. 发现非连续 page 或满页 no-progress 时报 `NETWORK_INCOMPLETE`，不修复、不返回 partial rows；
8. 返回完整语义结果或抛出异常，不把中途 partial pages 当成成功。

`1M` 自然月只在 Provider 本身支持时可用，不执行固定毫秒邻接校验。连续性校验只是 Provider 异常的 fail-fast guard，不参与 pagination cursor 或 tail-completion 证明。

Cache 只接触合并后的 `OhlcvResult`，不得读取 Provider page 或控制重试。

## 7. Series identity

Cache key 必须包含所有会改变数据身份的参数：

```text
provider / mode / market / symbol / timeframe / variant
```

`variant` 覆盖 mark/index/premium-index 等价格系列。任何未编入 identity 的影响数据内容参数都会导致 cache 串数据，必须在实施时列入 canonical encoding。

## 8. 删除的平行入口

已删除 `ccxt_utils.py`、`ccxt_utils_extended.py`、`binance_adapter.py` 及任何直接返回裸 CCXT instance 给 Route 的入口；不存在 deprecated forwarding wrapper。`ccxt_trading.py` 只是为遵守单文件 400 行限制的 private mixin，Route 仍只看到 `CcxtClient`。

研究出的 Binance/Kraken 订单差异仍然保留，只是实现位置收口到 Client，见 [订单行为对比](../ccxt_research/order_behavior_comparison.md)。
