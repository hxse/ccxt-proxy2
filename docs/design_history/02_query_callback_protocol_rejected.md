# RouteIntent、FetchOperation 与 Callback 协议

> **Status: Rejected design archive. Do not implement.** 本文从早期 `todo/` 完整迁入正式文档区，只保留历史讨论；现行规范以 [`docs/README.md`](../README.md) 标记的 Implemented design 为准。


> **文档状态：Ideas archive，非实施计划。** 这是一个未来可能考虑、但由于复杂度过高而更可能不会实施的 TODO；本文仅保留设计思路，不代表当前架构决策或开发排期。

## 1. 设计边界

本方案不再使用一个 `OhlcvQuery` 同时表达用户意图和缓存缺口动作。

```text
RouteIntent：最终结果目标
FetchOperation：为满足目标而执行的一次上游逻辑动作
```

缓存入口必须显式接收 RouteIntent，不能通过不同 callback 函数猜测路由类型。

---

## 2. SeriesKey

```python
@dataclass(frozen=True)
class SeriesKey:
    provider: str
    environment: str
    market: str
    symbol: str
    timeframe: str
    variant: str = "default"
    qualifiers: tuple[tuple[str, str], ...] = ()
    schema_version: int = 1
```

所有影响数据内容的参数必须进入 identity，例如：

- Binance `mark/index/premiumIndex`；
- TQ `adj_type`；
- 未来 adjustment 或 continuous-contract variant。

统一 timestamp 使用 UTC milliseconds。

---

## 3. RouteIntent

```python
@dataclass(frozen=True)
class SinceLimitIntent:
    series: SeriesKey
    since: int
    limit: int
    include_last: bool = True


@dataclass(frozen=True)
class LatestLimitIntent:
    series: SeriesKey
    limit: int
    include_last: bool = True


@dataclass(frozen=True)
class SinceLatestIntent:
    series: SeriesKey
    since: int
    include_last: bool = True


RouteIntent = SinceLimitIntent | LatestLimitIntent | SinceLatestIntent
```

三个 RouteIntent 对应三个缓存公开入口和三个 HTTP 路由。

### 3.1 `include_last`

`include_last` 的唯一语义是：最终结果是否直接删除最后一根。

```python
if not intent.include_last and not result.is_empty():
    result = result[:-1]
```

不做 terminal bar 状态判断。所有内部 FetchOperation 都必须返回完整边界，`include_last` 只在最终合并结果上执行一次。

---

## 4. FetchOperation

为消除方向歧义，使用 `AfterCount/BeforeCount`，不使用 Forward/Backward 命名。

```python
@dataclass(frozen=True)
class FullQuery:
    intent: RouteIntent


@dataclass(frozen=True)
class AfterCount:
    series: SeriesKey
    anchor: int
    count: int


@dataclass(frozen=True)
class BeforeCount:
    series: SeriesKey
    anchor: int | None
    count: int


FetchOperation = FullQuery | AfterCount | BeforeCount
```

语义：

```text
AfterCount(anchor=30, count=100)
= 从 30 开始，向 timestamp 增大方向取最多 100 根
= 30 是 inclusive overlap

BeforeCount(anchor=40, count=100)
= 截止 40，向 timestamp 减小方向取最多 100 根
= 40 是 inclusive overlap

BeforeCount(anchor=None, count=100)
= 获取真实 latest 倒数 100 根
```

FetchOperation 不包含 `include_last`，因为内部缺口抓取必须保留 overlap 边界。

---

## 5. FetchResult

仅返回 DataFrame 不足以区分“数据源耗尽”和“Provider 无法访问”。

```python
class FetchStatus(Enum):
    COMPLETE = "complete"
    EXHAUSTED = "exhausted"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class FetchResult:
    data: pl.DataFrame
    status: FetchStatus
```

### COMPLETE

FetchOperation 已满足。例如 AfterCount 请求 100 根并得到所需数量。

### EXHAUSTED

Provider 能权威确认到达真实历史或实时边界。结果可以少于 count。

### UNAVAILABLE

Provider 当前 API 无法访问该 anchor/range，例如 TQ anchor 早于最新 10000 根 serial window。

CacheEngine 不得把 UNAVAILABLE 解释为“该范围没有 K 线”。

`NO_PROGRESS` 由 CacheEngine 检测：去重后没有产生新 timestamp 时停止，避免死循环。

---

## 6. Source protocol

不传多个裸 callback，传一个 capability object 或一个 operation dispatcher。

```python
class OhlcvSource(Protocol):
    def execute(self, operation: FetchOperation) -> FetchResult: ...
```

Provider 内部：

```python
def execute(self, operation):
    match operation:
        case FullQuery():
            return self.fetch_full_query(operation.intent)
        case AfterCount():
            return self.fetch_after_count(
                operation.anchor,
                operation.count,
            )
        case BeforeCount():
            return self.fetch_before_count(
                operation.anchor,
                operation.count,
            )
```

一个 source object 同时提供：

- 三个完整 Route fetch 能力；
- 两个缓存缺口方向能力；
- Provider-specific estimation 和补拉。

---

## 7. 零缓存与片段缓存调用不同能力

### 7.1 零缓存

```text
SinceLimitIntent  → FullQuery → Provider.fetch_since_limit
LatestLimitIntent → FullQuery → Provider.fetch_latest_limit
SinceLatestIntent → FullQuery → Provider.fetch_since_latest
```

Provider 可以使用针对完整路由优化过的分页算法。

### 7.2 片段缓存

CacheEngine 根据缓存拓扑选择：

```text
缺口位于时间增大方向 → AfterCount
缺口位于时间减小方向 → BeforeCount
需要真实 latest anchor → BeforeCount(anchor=None, count=1)
```

中间 bounded gap 不要求单独的 FetchBetween。CacheEngine 可以重复 AfterCount 或 BeforeCount，直到实际结果包含另一侧缓存 anchor。

以后 Provider 若能高效实现 FetchBetween，可以作为 optional optimization，但不能成为正确性的前置条件。

---

## 8. Provider adapter 不等于原始 API

FetchOperation 是逻辑能力，不能假设 Binance、Kraken、TQ API 可以直接完美表达。

### 8.1 Binance

```text
AfterCount
→ since + limit
→ 必要时自行首尾重叠分页

BeforeCount
→ endTime/until + limit
→ 必要时自行反向首尾重叠分页
```

### 8.2 TQ

```text
选择 data_length
→ 读取 serial
→ 过滤 anchor
→ 检查逻辑 count
→ 不足则扩大 data_length
→ 最大 10000
```

如果最大窗口仍不包含 anchor，返回 UNAVAILABLE。

### 8.3 Range 型 API

Provider 可以根据 interval 估算时间窗口：

```text
估算 window
→ 请求
→ 按实际 rows 验证
→ 不足则扩大 window
```

Provider estimate 只用于 API translation，不能作为连续性证明。

---

## 9. FullQuery 的作用

FullQuery 用于：

- 零缓存；
- `enable_cache=False`；
- 缓存片段极度碎片化、复用成本高于完整重抓时的 fallback。

FullQuery 是性能路径，不替代 RouteIntent，也不承担 CacheEngine 的片段规划。
