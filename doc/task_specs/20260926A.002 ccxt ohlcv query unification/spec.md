# CCXT OHLCV 查询统一

## 任务边界

共同规则见[主任务共同契约](<../20260926A market data caching and maintenance/spec_01_contract.md>)；缓存基础见[前置任务](<../20260926A.001 shared cache foundation/spec.md>)。

保留三个公开 URI、默认 mode=live、variant、鉴权、白名单、100000 根预算、返回结构及错误映射。Kraken Spot 保持原有无缓存与 since-latest 不支持的能力；交易相关方法不改。

允许变化：取消普通后继确认；抽取前缀和向后分页复用；非空短页有合法进展时继续普通分页；latest-limit 使用固定 S 与计算起点；增加最终结果校验；周线以上按本规范有限薄转发。移除项目 _backward_request、until/to 构造、普通 lookahead 和对应过时测试，不恢复旧 route 或兼容模块。

上市前相关请求只作现有链路能直接完成的处理，不能完成就明确结束；不新增上市日期查询、起点探测、跨空窗口补拉或另一套倒序回退。不得将某次空页推断为可靠上市日期，也不承诺所有合法 N 都能取得全部可用上市历史。

停止线：三类查询各自语义明确、调用方式唯一、缓存利用充分，含首连接与失败行为不被抽象破坏。

## 任务规范

### 分层复用

网络层只维护 since＋limit 向后分页，接收两类结束目标：固定数量，或固定快照 S。Client 复用一个缓存前缀、网络补齐、连接检查、一次完整回退、结果校验和持久化的编排。

SinceLatest 与 LatestLimit 调用接受既定 S 的同一内部查询，不通过公开路由互相调用。HTTP 路由仅组装参数和响应。TQ 不调用这些算法。

### SinceLimit

1. 校验参数、能力和身份；缓存开启时读一个最多 N 根的最佳前缀。
2. 足够 N 根，最终校验后直接返回 true，不调用网络、不重复写库。
3. 不足时从前缀真实末端含首请求，所需用户行数加一份重叠容量；无前缀从原始 since 开始。
4. 网络未包含缓存连接点，丢弃整个前缀，从原始 since 完整重拉一次，不循环寻找其他缓存。
5. 整体成功后合并、去重、限制最多 N 根，校验并按普通网络未知尾根提交；响应保留尾根。

取消用于确认目标尾根的额外取数。含首分页重叠和预算溢出检测所需容量仍保留。正常链路取得的合法短结果与顺带建立的 since 空白前缀证明保留；不为扩大上市前覆盖或缓存满固定历史 N 根而额外请求。

### SinceLatest

先取得最新一根的 S；无数据或 since>S 返回空。再读一个前缀，所有候选裁到 S，最多用 100001 根检测超限。缓存已经到 S 时返回可信缓存 true。

不足时从真实末端含首向后取到 S。非空短页有合法进展时继续；空页或无进展且尚未到 S 时为 NETWORK_INCOMPLETE，不跨空窗口寻找行情。缺少缓存连接点仍从原始 since 完整重拉一次。

到 S 即结束，不再执行后继确认；网络结果尾根未知。多页成功后统一提交，任何中途失败不返回或保存部分结果。出现第 100001 根用户记录报超限，不截成成功的十万根。

### LatestLimit

先以 since=None、limit=1 固定最新时间戳 S。固定周期计算 start=S−(N−1)×interval_ms；起点不合法时明确拒绝并提示减小 limit，不将它归零后继续寻找历史。合法起点以同一 S 进入快照内部查询，结合连续缓存与 since＋limit 向后分页。

公开 since 的既定十三位毫秒校验保持。正常链路自然取得合法短历史并到达 S 时可以少于 N；正常完整范围仍须满足 N。起点过早导致空窗口或无法到 S 时明确失败，不能为保证短历史成功增加搜索，也不能用内部缺口解释数量不足。

不重新获取 S，不从全库缓存最新点追赶，不使用 until/to/endTime。网络后出现 S 之后的新行，缓存并发加入更新数据，均裁掉；这些行不用于写库或确认 S 完成。

### 普通分页的停止条件

Kraken Futures 的 since＋limit 可被 SDK 转成有限时间窗口，不能仅用 len(page)<request_limit 判断数据结束。项目仍只使用统一 SDK 调用，保留实际重叠与进展检查，不增加 Provider 专属空窗搜索。

| 本次结果 | 数量目标 SinceLimit | 固定 S 的查询 |
| --- | --- | --- |
| 已达到 N / 已到 S | 立即完成 | 立即完成 |
| 非空短页且新增合法行 | 未够 N 时沿真实末根含首继续 | 未到 S 时沿真实末根含首继续 |
| 无缓存前缀的首窗为空 | 按原语义返回空，不继续找历史 | 已知 S 存在却到不了，NETWORK_INCOMPLETE |
| 短页仅含预期重叠点，无新行 | 可作为正常数量终点，返回已取得短结果 | 未到 S 则 NETWORK_INCOMPLETE |
| 满页无进展、应有连接点却缺失、内部断档 | 沿原准确错误/单次前缀回退规则 | 沿原准确错误/单次前缀回退规则 |

后续页为空却缺少预期重叠点时不能当作完整终点。继续短页是为完成用户 N 或固定 S，不是为确认尾根多取 N＋1。所有游标来自真实返回时间，不以 interval 跳过空白。

### 校验与尾根

将固定上界裁剪封装为共享纯函数，作用于页、缓存候选和最终结果。先取得合法时间并裁到本次边界，再对目标数据执行相应数值、间隔和数量校验；不因边界之外新增 K 线改变本次结果。

每页保留原 canonical 校验、固定周期邻接检查、实际连接点和进展检查。最终纯网络、纯缓存和混合结果都验证：字段、有限数值、价格关系、非负 volume、时间唯一升序、起点、行数上限、固定 interval 及适用的 S。

合法同时间戳修订以本次后取得网络值为准；S 冻结时间而非价格。缓存始终通过公开 API 调用，不在重构中访问底层或读第二段缓存。

网络尾根 false，缓存内部排除其新增持久化；已经存过的可信末行不删除。纯缓存非空 true，空结果 null。保留未知不等于确认未完成的含义。

### 能力表与错误

| 数据域 | SinceLimit | SinceLatest | LatestLimit | 项目缓存 |
| --- | --- | --- | --- | --- |
| Binance/Kraken Futures 支持的固定周期≤1w | 完整分页 | 固定 S 完整分页 | 固定 S 推导起点 | 是 |
| Binance Spot 固定周期≤1w | 沿现有尽力支持 | 沿现有尽力支持 | 同一算法，保持尽力范围 | 沿现有身份 |
| 上述 Provider 声明支持的周期>1w，包括 1M | 单页 since＋limit | NOT_SUPPORTED | 单页 since=None、limit=N | 否 |
| Kraken Spot | 原窗口薄转发 | NOT_SUPPORTED | 原薄转发 | 否 |

周线以上单页 N 不得超过当前 Provider 单页上限；超出返回 NOT_SUPPORTED，不隐式分页或截断用户请求。enable_cache=true 在明确不支持缓存的数据域不产生读写；未知周期仍先按 exchange.timeframes 拒绝。

1M 不按三十天计算；删除原 _timeframe_seconds 的月秒数捷径。Kraken Spot 的既有能力和参数限制不套新单页规则改变。

保留普通缓存失败、容量 507、网络重试、无进展和 invalid provider data 的对应错误，不以强行修补间隔或删除中间坏行求成功。

## 公开接口与用户写法

```text
GET /ccxt/fetch_ohlcv/since-limit?exchange_name=binance&market=future&symbol=BTC%2FUSDT%3AUSDT&timeframe=5m&since=1790000100000&limit=100
GET /ccxt/fetch_ohlcv/since-latest?exchange_name=binance&market=future&symbol=BTC%2FUSDT%3AUSDT&timeframe=5m&since=1790000100000
GET /ccxt/fetch_ohlcv/latest-limit?exchange_name=binance&market=future&symbol=BTC%2FUSDT%3AUSDT&timeframe=5m&limit=100&enable_cache=true
```

mode 省略为 live；相应身份仍须已启用。返回保持：

```json
{"rows":[[1790000100000,100,102,99,101,12]],"last_bar_completion_confirmed":false}
```

单根未知网络结果仍返回一根，缓存无新行；同一根已存在可信缓存时，不删除旧值。enable_cache=false 不读写缓存，但仍执行分页和最终校验。

反例：传 until 或 include_last 为 422 未声明参数；支持 1M 的 Provider 使用 since-latest 为 NOT_SUPPORTED；latest-limit 推导出负起点为 422 INVALID_PROVIDER_REQUEST；分页未到 S 或固定周期缺口为 NETWORK_INCOMPLETE。边界失败可提示调整 since 或减小 limit，不凭空断言准确上市日。正常链路已经完成查询语义的短结果仍可成功。

## 测试、验证与阶段过渡

文档阶段只核对契约。实现阶段通过 just test、just lint、just check；更新三类查询、Client 缓存、尾根、路由与 online 文件里的旧断言，但此阶段不执行在线入口。

离线覆盖三类查询纯网络/完整命中/部分命中/缓存关闭；单页边界、多页含首、同时间修订、缺连接点后的单次完整回退；分页中新增 S 之后数据以及缓存已有更晚数据。

验证只取一次 S，不出现项目终点参数或专门确认调用；第 100001 根溢出和重叠容量不被删掉。非空短页有进展继续后到 S 必须成功；短页后空页/无进展仍未到 S、错误起点、间隔缺口、满页无进展和网络中途失败验证准确错误与无部分写入。

Kraken 离线数据必须反映 SDK 的真实时间窗口语义，可运行已安装 SDK 并替换底层 HTTP，或使用核实过的等价原生 fixture；不能只用“取 since 之后 N 行”的替身。覆盖起点较早但首窗非空且能自然续到 S，以及首窗为空直接失败；不要求增加跨空窗补拉使后者成功。

尾根测试改为普通网络末根不新增落盘、缓存末根保留、多页只排整体尾根；不保留“固定历史请求必须通过后继确认全量入库”的旧期望。

周线以上能力表、原 Kraken Spot、variant 身份、公开 since 边界、推导非法起点的拒绝都覆盖。保留正常返回即有的 covered_from 证明，空结果不创建片段；不将专项上市前恢复列为必测目标。同步 ccxt_client、ccxt_ohlcv、ohlcv_cache_resolution、market_data_contract、verification 以及受影响 Bruno 描述。

本阶段一次替换三种查询及其旧分支，不保留兼容 wrapper；共享缓存基础保持可用，TQ 仍走独立阶段。最终在线执行由集成阶段承担。
