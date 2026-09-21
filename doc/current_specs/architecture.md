# 项目架构与模块边界

ccxt-proxy2 是带 Bearer 鉴权的 FastAPI 服务，统一提供 CCXT 交易所接口、CTP 交易、CFB HTTP 转发、TQ 行情和 Telegram 消息发送。本目录规定当前有效的接口、行为、约束和验证规则；详细 HTTP 参数与响应类型同时由 /openapi.json、/docs、/redoc 和 /scalar 展示。

## 正式调用链

| 能力 | 正式链路 | 职责 |
| --- | --- | --- |
| CCXT 行情、账户与交易 | Route → ExchangeManager → CcxtClient → CCXT | 管理长期实例、交易所能力、分页与错误转换 |
| CCXT OHLCV 缓存 | CcxtClient → DuckDbOhlcvCache → DuckDB | 读取一个最佳前缀，按完成证据写入和合并 |
| TQ 行情、日历和状态 | Route → TqManager → TqWorker/TqClient → TqSdk | 专用线程处理 SDK，状态接口读取内存快照 |
| CTP 交易与查询 | Route → CtpManager → session/SPI → vnpy_ctp 交易扩展 | 原生请求、回调关联和账户生命周期 |
| CFB 业务接口 | Route → CfbProxy → cn-futures-bridge HTTP | 原样转发参数、状态码与正文，只增加鉴权和超时 |
| Telegram 文本消息 | Route → TelegramManager → Telegram Bot API | 从配置解析目标 chat，逐目标返回发送结果 |
| 公共时间 | System Route → 币安公共时间 HTTP | 每次请求读取上游时间，返回原始毫秒时间戳 |

路由负责 HTTP 契约和入口鉴权，业务规则由对应模块承担。中国期货行情与价格继续走 TQ。CFB 自身的终端、账户及业务能力由独立项目维护。

## 共同运行约束

服务使用单个 Uvicorn 进程。同一 DuckDB 文件只允许一个进程读写；增加 worker 也会创建多份 TQ/CTP 实例和进程内状态，不能靠多 worker 扩展当前部署。

TOML、白名单、启动和健康检查遵循[配置与生命周期](configuration.md)。CCXT、TQ、CTP 和 CFB 只初始化白名单中的服务，配置只在启动时读取。每个已启用身份长期复用自己的客户端，请求不触发 SDK 初始化。

各模块的锁只保护自己的资源：CCXT 对单次底层请求加锁，DuckDB 对写事务加锁，TQ 通过专用线程和 FileLock 管理 SDK，CTP 串行处理同账户操作。网络请求不得在 DuckDB 写锁内执行。

TQ/CTP 交易状态接口通过异步 HTTP 入口读取独立短锁保护的快照，鉴权不占用同步线程池。CFB 状态查询仍原样转发，由 CFB 决定其执行方式；不承诺三个来源具有相同延迟或未知状态表达。

应用本身不注册响应压缩中间件。需要压缩时由部署入口按内容类型处理。资源在应用退出时关闭，关闭规则由各模块的现行规范定义；不能只依赖进程退出回收连接。

## CCXT 与缓存边界

CCXT 正式保证的交易所范围是 Binance USDⓈ-M linear Futures 和 Kraken Futures。Binance COIN-M/inverse 不支持；Binance/Kraken Spot 只保留现有接口的尽力支持范围，Kraken Spot sandbox 不支持。

只有 CcxtClient 是路由可用的 CCXT 入口。它编排缓存读取、完整网络查询、缓存写入和返回转换；私有 mixin 不构成第二套公开调用链。ExchangeManager 不向路由返回裸 CCXT 实例。

DuckDbOhlcvCache 只进行计算、本地 SQL 和文件操作，不导入 CCXT/TqSdk/FastAPI，不持有 Provider，不接受网络 callback，不决定分页或重试。

三个 OHLCV 路由保持以下不变量：

1. 分页从上一页尾部时间戳包含起点地继续请求，按时间去重，新数据覆盖同时间旧值。
2. 固定周期只用于支持范围内的网络数据连续性校验，不构造下一根游标，也不结合本机时间推断尾根完成。
3. 响应始终保留目标窗口；是否可写缓存由尾根完成证据单独决定。
4. 持久缓存中的每一根数据都获得过严格更晚的数据作为完成证据。
5. 每次最多复用一个最佳缓存前缀，进入网络阶段后不读取第二段缓存。
6. 历史修订只在后续网络查询重新覆盖该时间戳时发现，不保证主动同步交易所删除或回填。

当前缓存没有旧 Parquet/proof-log 兼容读、通用多缺口解析器、中位数估算、自适应补拉、TQ 外层缓存或多进程写入路径。容量限制针对逻辑行数，不承诺精确磁盘字节上限。

## 模块规范

| 主题 | 现行规范 |
| --- | --- |
| TOML、鉴权、白名单、健康与部署 | [配置与生命周期](configuration.md) |
| CCXT 能力、交易、重试及并发 | [CCXT 客户端](ccxt_client.md) |
| 三类 OHLCV 查询、分页与返回 | [CCXT OHLCV](ccxt_ohlcv.md) |
| 缓存身份、表结构与覆盖证明 | [缓存存储模型](ohlcv_cache_storage.md) |
| 缓存 API、事务、合并、淘汰 | [缓存操作](ohlcv_cache_operations.md) |
| 单前缀查询与网络续接 | [缓存查询算法](ohlcv_cache_resolution.md) |
| TQ 路由、订阅和实时状态 | [TQ 行情](tq_data.md) |
| TQ DataFrame 清洗、错误和验证 | [TQ 数据处理](tq_processing.md) |
| CTP 原生交易、回调和状态 | [CTP 交易](ctp_trading.md) |
| 独立完整 VeighNa 联调入口 | [CTP 联调脚本](ctp_assessment.md) |
| CFB 转发、配置与自动文档 | [CFB 代理](cfb_proxy.md) |
| Telegram 请求、发送和错误 | [Telegram 文本消息](telegram.md) |
| 币安公共时间转发 | [公共时间](system_time.md) |
| 离线、在线、Bruno 与有状态调试 | [验证约束](verification.md) |

## 文档职责

doc/current_specs 按模块维护当前约定。doc/task_specs 记录正式任务的 meta、context 和 spec，不覆盖后来已经落地的现行规则。doc/archive 保留旧迁移过程、研究样本与已否决方案，不能据此推断当前支持能力。

根 README 提供启动入口和必要导航。文档之间以链接引用完整规则，不保留另一份现行规范副本。业务实现改变时同步受影响的 current spec；纯文档迁移不会授权改变程序行为。
