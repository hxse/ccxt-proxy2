# 验证入口与模块回归约束

## 原则

- 默认测试全部离线，不连接真实交易所、TQ、CTP、CFB 或 Telegram 服务。
- Provider 测试使用单页 raw API response mock，验证 Client 自分页。
- Cache 测试使用独立临时 DuckDB file，不共享 state。
- Cache 正确性断言基于实际 timestamp/order/overlap，不使用 timeframe adjacency。
- Binance/Kraken Futures network mock 在 `m/h/d/w` 上必须包含连续成功页与异常 gap 失败页；只有 Provider 实际支持的 `1M` 才可跳过固定毫秒邻接断言。
- Online/debug tests 必须显式启用，不得进入默认 CI。

查询完整性、片段连续性、尾根资格和 JSON 合法性分别验证，定义见[行情数据契约](market_data_contract.md)。现行约束只对当前实现负责；公开 API 或查询算法被正式任务替换时，其旧断言、fixture、Bruno 与 OpenAPI 必须在同一代码阶段更新，不将必要失败推迟到集成阶段。

## CCXT 客户端与网络验证

### 三类 Route

- `SinceLimit`：数据足够、少于 limit、恰好 page boundary、多页。
- `SinceLatest`：固定 snapshot、分页期间出现新 row 不追加。
- `LatestLimit`：固定一次 S，推导起点复用快照查询，最终校验数量/间隔；非法起点明确拒绝。

### 分页

- 第二页从上一页 tail 含首请求。
- Overlap same timestamp 合并只保留新 row。
- Binance/Kraken Futures 固定周期 page 出现非连续 timestamp 时触发 `NETWORK_INCOMPLETE`。
- 连续性校验不搜索 gap、不扩大时间窗口、不返回 partial rows。
- Page head 不含预期 anchor 时报错。
- 数量查询首窗 Empty/后续短页 anchor-only 可正常结束；快照查询未到 S 则失败。非空短页有合法进展继续，不能跨空窗搜索。满页 no-progress 必须返回 `NETWORK_INCOMPLETE`。
- Network 中途失败不返回 partial result。
- Read-only retry 按次数生效；create/cancel/close/leverage 不自动 retry。
- Authentication/BadRequest 等 Provider rejection 不重试；非网络型 write rejection 不得误标为 `OPERATION_STATUS_UNKNOWN`。
- 周线以上只允许有限单页 SinceLimit/LatestLimit，无项目缓存；SinceLatest 不支持。月不按三十天换算，Provider 未声明周期时前置拒绝。

### 尾根证据

- 所有普通网络 OHLCV 最终尾根返回 false；不再发后继确认请求。
- 完整分页只排除整体末根的新增持久化，不逐页排尾。
- 纯可信缓存返回 true，不重复去尾或写回；未知网络尾根不降级库内同时间戳可信行。
- 最新快照只取一次，所有来源裁去 S 之后数据；同 S 有效修订可更新响应。
- 含首重叠和溢出检测仍保留，不能作为删掉后继请求的连带删除项。
- 真实 Kraken SDK 配离线 candles 验证有限时间窗口短页正常续接与空窗失败；不联网寻找上市日期。

## 缓存读取验证

- Exact since 命中。
- Bracketed since 命中，predecessor 不返回。
- Leading gap：`covered_from <= since < first_time` 命中。
- `since < covered_from` 和 `since > last_time` miss。
- 多 candidate 选可复用 rows 最多者；同数时按 freshness/tie-breaker。
- 只读一个 segment，不自动拼第二段。
- `max_rows` 硬限制 read result；`SinceLatest` 可用 100,001 检出超限。
- Candidate 和 rows 在同一 snapshot。

## covered_from 验证

- 品种上市前 since/上市时 first row 产生 leading gap proof。
- 后续更晚但仍早于 first row 的 since 复用同一 segment。
- 更早 verified request 将 coverage 从 12:00 扩到 11:00。
- 固定周期 `LatestLimit` 从推导 since 取得的合法覆盖沿统一入口保存；没有 since 证明的最新窗口只能从实际首行开始。
- 不支持 since 权威语义的 Provider 不能创建 leading proof。
- Eviction 裁掉 segment 前缀后重置 `covered_from=new first_time`。

## 写入与合并验证

- 无 overlap 创建新 segment。
- 存在一个 exact timestamp overlap 即可合并。
- 只有 min/max 范围交叉、但无相同 timestamp 时不合并。
- Incoming 同时 overlap 多 segment 时合并成一个。
- Incoming valid same-time row 原子覆盖。
- Incoming missing timestamp 不删除旧 row。
- Incoming batch 含 NULL/NaN/Infinity/invalid row 时整批不写，旧值不变。
- Metadata 与 rows 同 transaction 提交，任一失败无半成品。
- 合并后 first/last/count/covered_from 与实际 rows 一致。

## 缓存资格与响应验证

- Metadata `true` → cache 写入全部 user rows。
- Metadata `false` → cache 写 `rows[:-1]`，response 仍返回全部 rows。
- `limit=10` 在数据足够时 true/false 都返回 10 根。
- 空 result/null metadata 和单 row false 都不写空 segment。
- 完整 cache hit 的 response tail metadata 为 `true`。
- 三个 Route 始终返回完整目标 rows；completion metadata 不改变用户 row count。
- Client partial hit 必须用 network overlap revision 覆盖旧 row；若 network tail 未确认，只更新已确认前缀。
- Client leading-gap proof 可服务同一区间内更晚的 since；`SinceLatest` overlap 失败必须丢弃 prefix 后完整重拉。

## 容量与淘汰验证

- Startup config 满足 `100_000 < per_series <= total`。
- Series count 按 series 内 distinct time；total 按 live `(series_key,time)`。
- Per-series 超限后淘汰到 90% watermark。
- Global 超限后跨 series 按 `(time,segment_id)` 淘汰到 90%。
- Eviction 只形成 segment prefix delete，不制造中间缺口。
- Incoming 过旧时可在同 transaction 被淘汰。
- 空 segment metadata 删除，剩余 segment metadata 重算。
- Eviction 失败整个 transaction rollback，Route 映射 507，服务继续。
- `enable_cache=false` 不读、不写、不淘汰。

## 并发验证

- 多 reader 看到 commit 前或后的一致 snapshot，不看中间状态。
- 多 writer 通过同 database-path lock 串行，不是每个 object 各一把锁。
- 不同 Python threads 不共享同一 DuckDB connection。
- Cache close 阻止新 reader，并等待 active reader 完成后才关闭 connections。
- Network latency 不持 DuckDB lock。
- 并发 CCXT calls 经 per-client lock 串行底层 attempt，pagination 页间不持锁。
- CCXT close 与底层 attempt 使用同一 lock；close 后旧 Client 引用返回 503 SERVICE_NOT_READY，并带 service 身份，不得调用 Provider。

## 路由验证

- 三个 request schema 不能产生模糊组合。
- `limit <= 100_000`；`SinceLatest` 第 100,001 根失败且无 partial response。
- Provider capability error 映射稳定。
- Binance inverse 在所有带 symbol 的 public method 前置拒绝；其他未知 symbol 映射 `INVALID_PROVIDER_REQUEST`。
- `mark/index/premiumIndex` 传给 Provider，并使用互不混淆的 cache series；未知 variant 在 Client boundary 拒绝。
- Cache read failure fallback network；普通 write failure 不丢 network response；capacity failure 返回 507。
- Route 只调 `CcxtClient`，没有裸 CCXT/SQL/Provider branch。

### 生命周期与错误映射

- 使用可控事件分别阻塞/失败 TQ、CFB、Binance、Kraken 初始化，证明 HTTP 和其他身份继续运行；全体失败仍可访问鉴权/文档/健康。未就绪身份统一 503，未完成 CCXT 实例不可发布，TQ 工作线程退出后不能返回旧状态。
- readyz 返回应用就绪及逐身份状态，不将 SDK 失败当成容器启动失败；覆盖初始化取消、晚到结果不能复活、重复取消、关闭先等待任务再释放缓存、重新进入生命周期。
- `ExchangeManager` reinitialize/shutdown 只关闭旧 Client；应用 CacheResource 在消费者退出后释放 DuckDB connections；关闭幂等，并拒绝关闭后的访问。
- 重复 whitelist identity 在配置阶段拒绝。
- CCXT request/order/auth/funds/operation error 映射为稳定、脱敏的 HTTP code；Provider 原文不进入 response。
- `fetch_market_info.leverage` 没有可靠 position 证据时为 `null`，position fetch 失败不得伪装成 leverage 1。
- Binance normal/conditional order list 合并时按 ID 去重；Kraken Futures 与 Binance Spot 不进入该分支。
- Exchange factory 固定 Binance Futures `linear` market filter、sandbox demo mode，以及 Kraken Futures/Spot 对应的不同 CCXT class。

### 仓库与 Bruno 约束

- Bruno GET URL query 与 `params:query` 完全一致，request-specific 值不得回流 environment。
- OHLCV Request model、Client signature、OpenAPI 和 Bruno 均不得重新暴露服务端删尾参数。
- 每个 Bruno method/path 必须对应现有 FastAPI route；Just 引用的 `.bru` 路径必须存在。
- Mutating Bruno request 必须带 `[STATEFUL]`；`bru-readonly-basic` 只能引用 GET request。
- `Test/online` 只使用 live identity，并禁止 create/cancel/close/set/send 等 mutating call；`test-online` 不得包含 Telegram 或 sandbox identity。
- Cache package 不 import CCXT/TQSDK/FastAPI，公开 API 仅允许缓存操作规范列出的高级读写与生命周期入口，不暴露底层连接或 callback。
- 生产模块保持每文件不超过 400 行；已删除的平行 CCXT module 不得重新出现。

## 旧接口退出约束

- 新系统不读旧 Parquet/proof log。
- 不存在新旧 public cache entry 并行。
- 当前公开类型、调用方和测试不重新引入已删除的兼容入口。

## 只读在线检查

`just test-ccxt-online` 只使用 whitelist 中已启用的 Binance/Kraken Futures live identity，覆盖三种 OHLCV 模式、Binance `mark/index/premiumIndex`、网络尾根与可信缓存命中。只验证无需账户权限的公共行情，不查询账户/订单，不初始化或访问 sandbox。

`just test-online` 自动按数据源启动独立 Uvicorn HTTP 测试服务，使用临时端口、临时配置及临时 DuckDB、临时 HTTP 登录用户，启动前关闭生产后台计划。请求通过正式鉴权和路由；测试结束关闭进程并删除临时私有配置。不修改真实配置，不争用生产库，不触发清理。在最多 60 秒内等待 HTTP 就绪及该隔离服务的 services 全部 ready；已出现 failed 则明确失败，不把应用 ready 误当 SDK ready。业务等待最多 55 秒；一次服务端错误后停止该服务的后续请求，不循环寻找成功样本。TQ 覆盖主连/加权、日历、真实映射节点与可取得的过渡；旧窗口不能证明过渡时明确 skip，该项不算通过。Online test 不进入默认 CI。

## 验证入口边界

Podman 部署的配置、传输和生命周期使用离线命令替身与临时目录验证，纳入 `just test`；覆盖上传不启停、准备版本复用、停止不等待操作队列、在途/等待启动被停止代次取消。实际构建与无网络应用烟测通过目标机器的 build 显式执行；远端完整验证使用 `just deploy --target=remote --upload --build --start`，上传源码而非本机镜像。离线覆盖 HTTP/HTTPS 代理真实 SDK 参数、源码归档排除配置、构建失败保留旧版本及父子镜像定向清理；显式 build/start/stop/upload 不属于离线测试或 live 只读在线验证。完整边界见 [容器部署](container_deployment.md)。

TQ 代理离线验证截获真实 SDK 认证及传输入口，覆盖同步/异步 HTTP、WebSocket 上下文继承、元数据下载、直连忽略环境代理、异常恢复及其他线程不受影响。上传包验证 Binance/Kraken/TQ 三个开关开启且本地原件未变；缺地址在发送前拒绝，local 与 --keep-remote-config 不打补丁。

Just 参数转发使用隔离 uv 替身验证含空格/引号的 argv 与退出语义；test-file 不追加全量 Test 目录，test-online 保持显式启用和 live 只读范围。serve 命名参数在加载账户前校验，sync 与启动分开，CTP GUI 依赖隔离继续通过既有脚本入口验证；旧 image-build、container-start、serve-ctp、cleanup 命令必须明确退出。

- 裸 `pytest` 和 `just test` 都只运行 `Test/` 中的 offline tests，并忽略 `Test/online`。
- Offline pytest 在 collection 前将 `CCXT_PROXY_CONFIG_PATH` 指向 `Test/fixtures/config.toml`，不得读取真实 `config.toml` 或 Bruno 用户密码；Bruno credential 只由 `scripts/run_bruno.py` 在 `just bru-*` 内按需读取。
- `just test-online` 是只读 live online 聚合入口，仅执行 CCXT 与 TQ 查询；按 Provider 可使用 `just test-ccxt-online`、`just test-tq-online`。Sandbox 只属于 `just debug*` 调试入口。
- Telegram send 不属于 online test；有状态测试通过 `just debug-telegram-stateful` 显式执行，单次手动请求使用 Telegram 规范列出的 debug/Bruno 入口。
- `debug/route_tests` 会撤单/下单或修改 sandbox settings，标记为 `stateful`，不属于默认或普通 online suite。即使显式传给 pytest，也必须先设置 `CCXT_STATEFUL_DEBUG=1` 才会创建应用 Client；使用 `just debug-route-test` 或 `just debug-route-tests` 等明确入口运行。
- Stateful order test 只取消本测试创建的订单，不调用全账户 `cancel_all_orders`；closed-order history 只作 route smoke，不依赖短时间最终一致性。

模块行为分别以相应 current spec 为准；已结束的迁移步骤仅保留在历史归档。

## 其他模块与文档约束

TQ 的数据验证见 [TQ 数据处理](tq_processing.md)，CTP 的假前置与原生退出检查见 [CTP 交易](ctp_trading.md)，Telegram 的 mock 与手动发送边界见 [Telegram 规范](telegram.md)。CFB 使用模拟 HTTP 验证参数、状态码与正文透传、鉴权和白名单、超时不重试，并核对自动文档快照一致性；不以代理离线测试代表上游终端交易已经验收。

本地定义的公开 HTTP operation 提供摘要、行为或副作用说明、tag、参数与成功响应类型；CFB 的业务文档直接同步上游，保持参数与返回定义一致，只补充代理自身鉴权和错误。

非纯文档实现任务按其 spec 约定选择现有 just 验证入口；测试前核对入口是否访问外部服务或产生副作用。纯文档任务只核对文字、示例、链接、行数和差异，不因规范列出测试命令而执行它们。

文档中的待实现设计不属于当前能力。正式任务引用和示例应在对应 revision 自洽，手写文档遵守 AGENTS.md 的 400 行上限，meta 遵守 60 行上限；不能把旧检查脚本的宽松或空扫描结果当作例外许可。

## 共享缓存基础

离线覆盖 schema 1→2 迁移保留与失败回滚、类型/单位隔离、HUGEINT 跨源年龄排序、普通写入不删除辅助片段。向前读取、连接读取和概况不得跨片段，未知尾根不能独自连接；所有选择与行读取来自同一快照。TQ id、OI、symbol/秒制 duration 与身份一致，非法可持久化批次整批不写。应用资源并发首用只有一份实例，Provider 关闭不关闭共享库，所有测试使用隔离数据库。

元数据验证覆盖 schema 2→3 保留、日期交集连接、完整范围 miss、数据/上下文/核验事实原子提交；新旧转换等价性限于同一已生效区间。运行时和 AST 双重证明旧 SDK 元数据入口退出；预公告、真实参考日、在线时间失败、首次和再次刷新、下载合并与取消都用离线 fixture。详细要求见 [TQ 元数据](tq_metadata.md)。

TQ 映射回归还覆盖仅当前查询与历史查询统一源、每请求下载、摘要未变且完整命中不重写、历史修订不能被另一片段的新摘要掩盖；query_symbol_info 运行时硬禁，第一方 Quote/GraphQL 旁路由 AST 拒绝。主连/加权休市兜底使用历史源的实际合约，不能沿用 SDK 当前标的。

schema 5 还须验证旧 schema 3/4 缺少 roll_date 的真实结构：空及有数据的日表均能升级，原行、片段和覆盖保持；旧关联按 miss 重新核验，不能猜测节点；已有关联不降级。覆盖结构/版本一起回滚，以及未重新获取前清理仍保留必要上下文。

维护验证只用临时库；覆盖跨片段最新 K、0 清零、覆盖下界重置、独立日期/映射/过渡、部分失败、实际概况、维护不重入、HTTP 取消后门禁保持和关闭身份边界。清理 POST 不进入 live 只读在线聚合。

后台验证覆盖完整注释示例加载、严格类型/依赖与清单替换、默认只从 5m 请求映射、固定一万窗口、实际纳秒范围、短历史/空结果、独立项失败继续及清理显式规则。HTTP 替身验证令牌期限、401 一次重登、清理超时不重发、日志脱敏、有限就绪等待、单调调度跳过错过时点和退出顺序。所有测试预先选择关闭调度的 fixture；不使用生产 28 品种计划或生产数据库。
