# 缓存概况与保留清理

## 入口与范围

GET /cache/summary 只读本地数据，POST /cache/prune 会删除本地缓存。均使用既有 Bearer 鉴权；应用必须就绪，但不要求对应 Provider 启用，也不为了维护初始化 SDK。脚本和路由不得直接打开另一个 DuckDB 写进程。

## 概况

```text
GET /cache/summary?provider=tq&symbol=KQ.m@SHFE.rb&timeframe=300s&kind=ohlcv
```

可选精确过滤 provider（binance/kraken/tq）、mode（live/sandbox）、market、symbol、timeframe、variant、kind（ohlcv/calendar/main_mapping/transition）。未知/空参数拒绝；TQ 周期身份为秒数字加 s。

items 中每项包含 kind、identity、time_unit、start、end、count、total_count、segment_count。count 是最新实际片段的数量，total_count 是全序列的 distinct 时间数；start 是实际首行，不是 covered_from。最新按实际时间排序，不按 ID。片段真实交集唯一化保证元数据计数不会重复计同一业务时间。

```json
{"items":[{"kind":"ohlcv","identity":{"provider":"tq","mode":"live","market":"future","symbol":"KQ.m@SHFE.rb","timeframe":"300s","variant":"default"},"time_unit":"ns","start":1790211600000000000,"end":1790211900000000000,"count":2,"total_count":30000,"segment_count":2}]}
```

不存在的身份返回 items=[]。日期类端点为 ISO date；CCXT 为毫秒、TQ 行情与过渡为纳秒。日期身份和过渡身份见各主题规范；不返回 segment_id、数据库路径、凭据或隐式保留 K。概况不取在线时间、不访问 SDK、不读取后台计划。

## 显式清理规则

```text
POST /cache/prune
```

```json
{"providers":["tq","ccxt"],"modes":{"live":30000,"sandbox":0},"auxiliary":{"keep_years":10}}
```

providers 必须非空、不重复且属于 tq/ccxt；modes 的 live/sandbox 均须显式给非负整数，bool 不当整数。auxiliary 可省略或 null，表示只清普通行情；非空需 tq 在 providers 中且 keep_years 为正整数。未知字段/查询参数在操作前返回 422。

路由不读取后台默认规则，不记忆上次 K。K=0 表示本次清零，随后请求仍可正常写缓存；两轮之间允许超出 K。十万响应上限与原两百万/两千万写入容量保护不变。

每条完整 OHLCV 身份跨全部片段只保留最新 K 个时间戳。旧片段 28000、最新片段 6000，K=30000 后为 24000＋6000，仍是两个片段。原子删除行及关联字段、删空片段、更新首尾/count；被截前缀的 covered_from 重置为新实际起点。不重新去掉可信缓存末根。

## 辅助保留

只有本次明确请求辅助清理才调用公共时间一次；固定 Asia/Shanghai 日期，按日历年回退 keep_years，闰日按目标月末日处理。各辅助身份共用同一个截点 L，不接受客户端 now/cutoff_date。

- 日历删除 date<L，已公布未来日期保留。
- 日映射删除旧日行，保留解释剩余左边界所需的真实节点/不同前驱，不能伪造新首日为换月日。
- 过渡只在实际 K 根全部早于 L 时整窗删除；跨 L 整窗保留，不按周期×数量估算。

辅助数据不随普通行情清零级联删除。源 H/有效年份/核验事实不改成清理后的边界，不因清理推进 source facts。公共时间失败只跳过辅助部分，普通根数清理继续；结果标 partial 和准确时间错误。

## 并发和失败

一个进程的维护门禁不重入，第二请求立即 409 CACHE_MAINTENANCE_BUSY。客户端取消或超时不等于数据库操作取消，正在执行的维护结束前门禁不会提前释放。

先固定身份清单，每个身份用共享写锁和独立短事务。单身份失败回滚该身份，记录并继续其他身份；结构/连接级故障停止并返回 500 CACHE_MAINTENANCE_FAILED，detail.summary 保留已提交摘要。关闭先禁止新事务，允许当前事务完成，在身份边界结束剩余清理。

```json
{"status":"completed","rules":{"providers":["tq","ccxt"],"modes":{"live":30000,"sandbox":0},"auxiliary":{"keep_years":10}},"ohlcv":{"series_processed":224,"deleted_rows":100},"auxiliary":{"status":"completed","cutoff_date":"2016-09-26","deleted_rows":12},"errors":[]}
```

HTTP 200 仍可能 status=partial；成功必须同时看 status/errors。auxiliary.status 为 completed/skipped/not_requested，已执行数量如实保留。应用未就绪或缓存不可用为 503 CACHE_NOT_READY。新写入可能在清理事务之后再次增加数量，不能宣称任意时刻都≤K。

## 模块接口与空间

list_series_summaries(filters) 在一个一致 SQL 快照读取概况；prune(policy,trusted_cutoff_date) 只处理本地 IO。取时、鉴权和维护门禁属于业务层，SQL、删除和证明维护属于缓存内部。

不执行小时级物理压缩、整库复制或强制逐序列 checkpoint。DuckDB 正常 WAL/checkpoint 和空闲块复用保持；删除不承诺立刻缩小文件。默认 224 条 TQ 序列各三万根为 672 万根，含时间、OHLCV、segment_id、SDK id、OI 的裸数值约 537.6 MB；数据库本体可先按 0.5～1.5 GB 预算，实际压缩/索引不同，运行余量和备份另计。

离线在临时库验证多片段裁剪、零值、单位/身份隔离、辅助独立保留、上下文、整窗、事务回滚、部分失败、关闭和取消门禁。清理不加入只读在线或 Bruno 聚合；备份最简单是正常停服关闭后复制，当前不提供分布式备份或自动重建。
