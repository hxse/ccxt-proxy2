# TQ 普通行情接入缓存

## 任务边界

遵循[共同契约](<../20260926A market data caching and maintenance/spec_01_contract.md>)和[缓存基础](<../20260926A.001 shared cache foundation/spec.md>)。只实现普通 TQ OHLCV 缓存与失败后状态处理，保留 Tick；日历和映射此阶段仍使用其原正式链路。

退出 OHLCV 多 symbol、内部列表请求和联合宽表缓存设想；重复同值 query symbol 也拒绝。保留单合约字段名、纳秒精度、adj_type、合法前置占位清理和 JSON null 语义，不转换成 CCXT 数组响应。

不增加 since、时间向后分页、interval 连续性推算、窗口试探或连续数量达标检查。停止线是合法单次获取可自然积累、补充连续缓存并正确处理休市分支，关闭缓存时完全不读写。

## 任务规范

### 获取与处理职责

TqClient 保持 SDK 所属线程中的数据获取职责，提供稳定的单合约 serial 副本；TqManager/业务查询层编排数据库与状态分支。数据库操作不放在 SDK FileLock 或消息泵内，缓存内部不调用 SDK。

原始 SDK 获取入口与带缓存业务入口分开：前者仅获取和清洗，没有落盘副作用；后续过渡可复用前者，但不能调用自动写普通行情的公开业务入口。

请求 N 根仅调用一次 get_kline_serial，窗口 min(N,10000)，保持原 adj_type。等待更新后固定副本，再离开 SDK 线程处理存储。相同订阅可使用 SDK 内存 serial，不承诺每次 HTTP 重下载。

前置占位按原规则裁剪；中间占位、非法时间或倒序不得删除后继续。原生 id 在本次有效单合约序列中须为连续递增整数，不能以固定时间差替代。datetime 通过整数转换保持纳秒，禁止先转 float。

### 成功流程

1. 校验身份、输入、白名单与 SDK 结果；合法短结果属于成功。
2. 缓存启用且周期支持时，完整批次标记末根未知，调用 write_tq_segment；不在外层删尾。
3. SDK 数据已达到 N，则直接组织本次结果；不为返回内容再读数据库。
4. SDK 不足 N，则调用 read_connected_history，以本批有效证据读取相连历史；只根据高级结果合并去重，网络同时间值优先。
5. 按本次窗口上界裁剪，取最新最多 N 根，升序并最终校验，响应保留网络尾根。

可持久化部分不满足完整价格/volume 契约时整批不写，不用删除坏行制造连接；仍按原 TQ nullable 响应契约处理合法网络结果。辅助数值 NaN/Infinity 序列化为 null，不能写入非法浮点。

响应允许原契约中的合法 null，但已有非空数值须满足可检查的 OHLC 大小关系和非负数量；关系矛盾返回 422 TQ_INVALID_OHLCV_VALUES。nullable 只表示缺值，不能用它放过已知错误价格；前置和最终输出复用该校验，持久化另外要求核心字段完整。

成功分支不查状态。SDK 空结果或短结果不转为休市分支。没有相连历史返回本次可用行，不抓第二个窗口；仅一根未知尾根不能凭时间接到旧缓存。读取出来的可信末根不重复去尾。

缓存普通写失败记录 error，普通读失败记录 warning；已有成功网络结果仍可返回，响应可能较短。容量处理失败沿用 507。网络失败与格式错误不能被缓存故障掩盖。

### 失败与真实状态

只对明确的上游网络/服务获取失败执行此分支；参数、数据格式、权限/认证拒绝及缓存错误不归类为可兜底网络失败。enable_cache=false 或不支持缓存周期时直接返回原失败。

实际合约检查自身。KQ.m 通过本次当前标的查询取得实际合约；KQ.i 以同交易所、同品种 KQ.m 解析实际合约，保持大小写。解析失败不使用历史映射或本机日历代替。

随后只读一次既有交易状态快照：

| 状态 | 行为 |
| --- | --- |
| NOTRADING 且 is_open=false | 读取该请求原始序列的最新连续缓存，最多 N 根；空缓存可返回 [] |
| AUCTIONORDERING 或 is_open=true | 返回原行情获取错误 |
| is_open=null、断线、未收到或无法解析实际合约 | 明确失败，不返回缓存成功 |
| 状态权限/查询异常 | 保留其稳定错误，不重试等待开市/休市 |

状态检查对象不改变缓存身份：加权仍读加权缓存，主连仍读主连缓存。读取休市缓存失败时没有成功网络结果，返回 500 TQ_CACHE_READ_FAILED；未知状态为 502 TQ_TRADING_STATUS_UNAVAILABLE；不要覆盖已有 403 权限错误。

不得提前订阅状态作为所有正常行情请求的必要条件，不根据年龄阈值、节假日或小时表推断状态。状态现有断线失效和独立短锁读取保持。

### 等待与生命周期

行情操作采用有限的 10 秒等待预算，包含队列等待与本次序列就绪；SDK 消息泵继续按原短 wait_update 推进。实现必须使用能返回控制权的 SDK 订阅路径，在单调 deadline 下等待结果，不能进入不受该预算控制的同步内部等待后只给外层 Future 加超时。

超时为 504 TQ_DATA_TIMEOUT，可按上述状态规则判断是否兜底。排队任务到期后不得再启动获取；已经退出的业务操作不得迟到写库。SDK 已建立的共享 serial 仍由实例管理，不因一个调用超时关闭全局连接。

关闭停止接单、结束在途等待并在所属线程释放 SDK；保持状态消息推进。此预算只用于本次新增行情获取流程，不重做 Tick、CTP 或 SDK 全部认证流程。

## 公开接口与用户写法

GET /tq/fetch_ohlcv，继续 Bearer 鉴权：

| 参数 | 默认/范围 |
| --- | --- |
| symbol | 必填单个完整代码；重复 query 参数拒绝 |
| duration_seconds | 原合法正整数；超过一天必须为整天倍数 |
| data_length | 默认 10000；1..100000，表示最多响应数量 |
| adj_type | 原 F/B/FORWARD/BACK/空；空按 None |
| enable_cache | true；false 同时禁项目读写和兜底 |

```text
GET /tq/fetch_ohlcv?symbol=KQ.m@SHFE.rb&duration_seconds=300&data_length=20000
GET /tq/fetch_ohlcv?symbol=KQ.i@SHFE.rb&duration_seconds=3600&data_length=10000&enable_cache=false
```

响应仍为 records；示意一根数据：

```json
[{"id":120,"datetime":1790211600000000000,"open":3500,"high":3510,"low":3490,"close":3505,"volume":100,"open_oi":2000,"close_oi":2010,"symbol":"KQ.m@SHFE.rb","duration":300}]
```

不增加 completion 或 overlap 状态包装。SDK 一万根加上相连缓存可以返回两万；未连接则最多本次一万。最后一根仍可显示，但不新落盘。

duration_seconds>604800 时只走既有单窗口薄转发，最多 SDK 可用数量；enable_cache=true 也不读写或兜底，不为大周期增加分页。后台四种周期均属于重点支持范围。

非法数量沿用 400 TQ_INVALID_DATA_LENGTH，新多值输入为 400 TQ_MULTIPLE_SYMBOLS_NOT_SUPPORTED；内部列表模型也拒绝。未知参数为 422；id 不连续为 422 TQ_INVALID_TIME_AXIS。Tick 的一万上限与参数保持，不误用新的用户 OHLCV 数量上限。

## 测试、验证与阶段过渡

实现时通过 just test、just lint、just check；文档阶段不运行。离线以稳定 Pandas 序列、fake SDK 和临时数据库验证 N=1、5000、10000、20000、正常短结果及空结果，断言 SDK 只调用固定窗口一次。

验证无 interval 推算、午休/周末仍合法、id 缺口失败；足够时不为返回重复读库、不足时只用高级连接读取；未知尾根不持久化、不二次删除缓存尾根、无重叠保持分段、重复请求可以修订与桥接。

成功不查状态；失败分支覆盖实际/主连/加权、NOTRADING、竞价、未知、无权限、解析失败和空库。缓存关闭不得读取映射缓存或行情缓存作兜底，非法数据不能变成闭市成功。

验证 queue deadline、首次订阅、在途取消/超时、消息推进和关闭；不能仅断言 HTTP 已超时而忽略后台仍阻塞。保留 Tick、状态权限与断线失效回归。

同步 TQ 请求模型、OpenAPI、Bruno、单合约成功与多值拒绝测试、tq_data/tq_processing、market_data_contract 和 verification。移除“整个 TQ 不使用磁盘缓存”的旧全局断言，保留 Tick 和 SDK 获取层不直接操作数据库的边界。

映射多值/n 与旧 SDK 元数据入口在其阶段再同时迁移，本阶段不得提前禁用导致日历/映射断路。在线文件可同步新构造入口，实际联网只留最终手动入口。
