# 缓存概况与保留清理

## 任务边界

遵循[共同契约](<../20260926A market data caching and maintenance/spec_01_contract.md>)、[缓存基础](<../20260926A.001 shared cache foundation/spec.md>)、[日期数据](<../20260926A.004 tq calendar and mapping/spec.md>)和[过渡窗口](<../20260926A.005 tq rollover transition windows/spec.md>)。

新增 GET /cache/summary 与 POST /cache/prune，以及同一缓存模块的高级列举与维护 API。路由不能访问连接或 SQL，脚本不能直接打开数据库。清理不触发 Provider 初始化，不受后台品种清单限制。

不实现多层配置覆盖、自动备份、分布式锁、物理压缩路由或固定字节容量上限。停止线是所有缓存类型统计准确、删除与证明一致、并发安全、部分未执行明确报告。

## 任务规范

### 概况

cache.list_series_summaries(filters) 在一致读取快照中取得完整身份、总行数、片段数量和最新实际片段。count 仅指最新连续片段，total_count 指该序列跨全部片段的 distinct 业务时间数；start 是实际首根，不是 covered_from。

按实际时间选择最新，不按 segment_id/updated_at。日历和映射的日期片段也按日期排序；过渡的一个身份只有一个完整窗口，count 为实际 K。

不存在的精确序列不创建数据库记录；返回 items=[]。已有序列的空结果统一 start=null/end=null/count=0；已被清理删除的空片段不保留为假历史。

不返回任意 SQL、内部 segment_id、凭据、数据库绝对路径或隐式“生效 K”。不调用 SDK、状态或公共时间。Provider 已禁用也可查看已存在的本地数据；应用未就绪/缓存关闭仍拒绝。

例如总量 12000、最新连续量 6000、本次清理 K=30000 是三个不同事实。概况报告前两项；本次清理响应回显 K。人工先清到 10000、下轮后台按 30000 执行均合法，不建立“上次 K 就是长期策略”的状态，也不从后台文件推测概况中的 K。

### 普通行情保留

本次请求显式指定数据源类别与环境 K。ccxt 包含各实际 CCXT provider，tq 属于 live。每条完整身份跨全部片段，按真实时间保留最新 K 个 distinct time；不同 provider/mode/market/symbol/timeframe/variant 不混算。

默认脚本传 live=30000、sandbox=0，HTTP 路由不自行采用默认 K。0 不是禁用写入，两轮之间仍可新增；清理时删除匹配普通行情及其空片段。

每条序列在同一写事务内完成：确定最旧溢出行 → 删除及关联 TQ 字段 → 删空片段 → 重算实际首尾/count → 被截前缀 covered_from 重置新首根 → 提交。失败回滚该序列，不留下半个证明。

例如旧片段 28000、最新片段 6000，保留三万后为 24000＋6000，最新连续 count 仍为 6000。不能为了凑连续三万删除最新短片段，也不能把它们合并。

原单序列两百万/全库两千万和 90% 写入淘汰独立保留；小时 K 不替代它，十万响应预算也不降低。清理不再次删可信末根，不影响已成功组织的网络响应数量。

### 辅助数据保留

仅 providers 包含 tq 且 auxiliary 非空时执行。服务调用公共时间函数一次取得 serverTime，固定 Asia/Shanghai 日期 D；按日历年回退 keep_years 得 L，目标年月不存在对应日时取该月末日，不用固定 365×年数。

该时间属于本次清理请求，不复用采集脚本的整轮时间；本次各辅助身份共用固定 L，不逐品种重新取时。仅按根数清理不需要公共取时，调用方不能通过 now/cutoff_date 代替服务端可信边界。

| 类型 | 保留规则 |
| --- | --- |
| 日历 | 删除 date<L；未来已公布日期保留，同步片段元数据 |
| 日映射 | 保留最近年限日序列及解释 L 左边界所需的真实节点和不同前驱 |
| 过渡 | 仅当实际缓存 K 根全部早于 L 时整窗删除；跨 L 整窗保留 |

映射上下文不能退化为“把剩余首日当换月日”；只留必要上下文，不保留无限早的整段历史。辅助缓存不依赖普通 OHLCV 存在，不随其清零级联删除。

过渡用实际时间戳和 K 判断，不用周期×数量，也不按最后用户请求 N 裁短。保留窗口携带完整新旧身份，不因映射清理失去来源。

源表 H/有效范围是官方事实，不改写为清理后的最后日期；清理不能推进源核验事实。读取是否完整仍看剩余实际片段。

公共时间失败仅跳过按年份部分，普通按根数部分仍执行；响应必须标明 partial 和准确原因，不宣称全轮成功。客户端不传可信 now/cutoff_date，防止任意日期被当在线事实。

### 并发、生命周期与物理空间

维护在数据库所属服务进程执行。业务维护互斥门禁不重入，冲突立即 409，不堆积；真正读写安全仍由缓存共享写锁与事务保证。公共取时在数据库写锁外。

先固定待处理身份清单，逐序列/完整辅助身份短事务处理，不长时间持整库写锁。并发新增序列允许下轮清理；每个已处理序列只保证其事务提交时≤K，之后新写入可再次增长。

关闭时停止新维护，等待在途事务；在身份边界终止尚未开始部分并明确报告，不中断 SQL 留半个删除。单序列失败记录并继续可独立处理部分；结构或数据库级故障则终止本轮并返回明确失败。

新增高级入口 cache.prune(policy, trusted_cutoff_date) 接受已校验保留规则与服务内可信边界；不接受网络 callback。业务层只负责取时和门禁，缓存内部遍历、删除和更新元数据。

保留 DuckDB 默认 WAL 和自动 checkpoint，不按序列强制 checkpoint、不每小时复制重建数据库。删除空间可后续复用，文件不必立即缩小。备份需一致快照；最简单为正常停服关闭后复制，本任务不实现备份脚本。

### 容量规划

默认 28 个品种×4 个周期×主连/加权两类=224 条序列，清理后全满为 672 万根。时间和 OHLCV 裸数值约 322.56 MB；加 segment_id、TQ id 和两列持仓量约 537.60 MB，按十进制单位估算，不含索引或压缩效果。

数据库本体可先按 0.5～1.5 GB 作工程预算，尚非实测或硬上限。运行空闲余量另作规划，不能把预留的几 GB 宣称为 WAL/临时文件常驻占用。压缩、索引、修订、空闲块复用和实际行数都会影响文件大小。

日历全局一份，映射每品种按日一份；默认只有 5m 过渡，每窗十根，不另算一整套行情。筛选用旧合约一万根不入库。日线/周线通常未达三万，用户额外品种、CCXT 与更长过渡增加容量；上限限制每条序列，不限制序列种类数。备份副本另计。

## 公开接口与用户写法

两个入口均使用现有 Bearer 鉴权，无额外匿名本机入口，不新增权限模型。部署仍是单 Uvicorn 进程。

GET /cache/summary 可选过滤字段：provider（binance/kraken/tq）、mode（live/sandbox）、market、symbol、timeframe、variant、kind（ohlcv/calendar/main_mapping/transition）。字符串非空、精确匹配，不接受路径、SQL 或表达式；TQ 周期身份例如 300s。

```text
GET /cache/summary?provider=tq&symbol=KQ.m@SHFE.rb&timeframe=300s&kind=ohlcv
```

```json
{"items":[{"kind":"ohlcv","identity":{"provider":"tq","mode":"live","market":"future","symbol":"KQ.m@SHFE.rb","timeframe":"300s","variant":"default"},"time_unit":"ns","start":1790211600000000000,"end":1790211900000000000,"count":2,"total_count":30000,"segment_count":2}]}
```

日期类 time_unit=date，start/end 为 YYYY-MM-DD；CCXT 为 ms 整数，TQ 行情/过渡为 ns 整数。identity 为对应类型的确定字段：日历为 provider/mode，日映射另含 symbol，过渡另含 roll_date/new_symbol/old_symbol/timeframe/variant；不返回未声明动态字段。

POST /cache/prune 只接受 JSON body，不接受 query：

```json
{"providers":["tq","ccxt"],"modes":{"live":30000,"sandbox":0},"auxiliary":{"keep_years":10}}
```

providers 非空、不重复，只允许 tq/ccxt；modes 必须同时显式给 live/sandbox，均为非负整数，bool 不当整数；auxiliary 可省略或 null，只清普通行情。非空 auxiliary 要求 tq 在 providers 中且 keep_years 为正整数。未知字段/类型非法为 422，校验在取时和数据库操作前。

完成响应固定包含 status、rules、ohlcv、auxiliary、errors：

```json
{"status":"completed","rules":{"providers":["tq","ccxt"],"modes":{"live":30000,"sandbox":0},"auxiliary":{"keep_years":10}},"ohlcv":{"series_processed":224,"deleted_rows":100},"auxiliary":{"status":"completed","cutoff_date":"2016-09-26","deleted_rows":12},"errors":[]}
```

部分失败或取时失败时 HTTP 200、status=partial；auxiliary.status 为 completed/skipped/not_requested，跳过时 cutoff_date=null，errors 包含 scope、可得 identity、稳定 code。脚本必须检查 status/errors，不能仅凭 HTTP 200 报全成功。

门禁冲突为 409 CACHE_MAINTENANCE_BUSY；库不可用为 503 CACHE_NOT_READY；无法继续的维护故障为 500 CACHE_MAINTENANCE_FAILED，错误中保留已完成部分摘要。所有失败都不伪称已回滚整轮已提交序列。

rules 只是本次请求回显，不成为服务端下一次默认规则，也不写入概况作为长期生效配置。人工请求可传其他 K，后台仍用自身计划构造下次请求。

## 测试、验证与阶段过渡

实现时通过 just test、just lint、just check；仅临时数据库离线验证删除，文档阶段不运行。覆盖多片段跨片段截断、K=0、无溢出、身份隔离、前缀覆盖重置、空片段移除和源事实不伪造。

覆盖 CCXT 毫秒/TQ 纳秒并存、最新片段统计与 total_count 区别、日期和窗口概要；概况不取时、不启动 SDK、不读取后台配置，查询和行统计处于一致快照。

辅助清理覆盖闰日年回退、未来日历、左节点/前驱、跨 L 的完整过渡、更长 K、无普通行情的独立映射/过渡。取时失败准确 partial，按根数仍执行。

注入单事务失败验证回滚；并发写入、读出、整窗替换、两次维护与关闭验证门禁和一致性。验证普通 HTTP 超时不被误称为服务端删除已取消。

同步 OpenAPI、带 STATEFUL 标记的维护 Bruno 示例、只读概况示例和 current spec。只读在线聚合不得引用 prune 或真实运行库；本阶段不启动后台，也不实施物理 compact。默认源码边界检查继续禁止路由 SQL 和外进程直写。
