# 共享缓存基础与生命周期

## 任务边界

共同约束见[主任务](<../20260926A market data caching and maintenance/spec.md>)及其 spec_01_contract.md。本任务调整资源所有权、普通行情存储和高级读取，不改变 CCXT 查询算法，不启用 TQ 磁盘业务或维护 HTTP 路由。

保留 read_best_prefix、write_segment、close 的现有调用语义和 CCXT OhlcvResult；不保留另一个独立缓存实现。新增 TQ 类型承载与读取入口，不把网络、SDK、FastAPI、callback 或 Provider 对象放入 cache_tool。

迁移范围包括 ExchangeManager 的 _cache 创建/关闭职责、ServiceRuntime/lifespan、直接构造 Manager 的测试及调试调用方。停止线是现有 CCXT 行为不变、共享库可被独立消费者使用、升级保留数据、高级读取不能跨无证据片段。

## 任务规范

### 应用资源所有权

由应用生命周期创建并持有唯一缓存资源，生产消费者注入该资源。仅 TQ 启用时仍可取得它，单 Provider 关闭不关闭库；空白名单不初始化行情 SDK，维护需要库时也不隐式启用 Provider。

资源可按首次实际需求延迟创建，但创建和关闭门禁属于应用，不能恢复各 Manager 自行持有不同缓存的路径。重复初始化和关闭须幂等；启动失败释放已创建资源。先停止新任务、等待在途业务，再关闭缓存连接。

现有 thread-local connection、同路径写锁、事务和 reader lifecycle scope 保留。所有新读取必须在同一 SQL 或同一读取事务内完成片段选择、元数据和行读取。不得返回供调用方继续执行 SQL 的连接、游标或片段编号。

### 数据类型与身份

CCXT OhlcvSeries 的六维编码与毫秒时间保持，已有 series_key 不改名。TQ 新增 TqOhlcvSeries，provider=tq、mode=live、market=future、完整 symbol、timeframe 为 duration_seconds 的十进制数加 s，例如 300s；variant 将空值规范为 default，F/FORWARD 为 F，B/BACK 为 B。

TqOhlcvBatch 保存稳定 records 及末根资格；字段至少包括 id、datetime、open/high/low/close/volume、open_oi、close_oi。symbol 与 duration 从确定的序列身份恢复，不能与传入 records 冲突。SDK 其他非持久字段只随本次网络结果返回，不建立任意 JSON/blob 列。

普通缓存核心价格/volume 必须有限且满足价格关系，volume 非负。TQ 合法 nullable 附加字段可存 NULL；原生 id 必须为非负整数。批次不满足持久化资格时整批不写，不删除中间坏行后存其余行。响应合法性由业务层按 TQ 原契约处理，不因缓存资格不足擅自改成查询失败。

批次中的尾根标记由业务层提供；现有筛选器保持唯一。此阶段仍允许旧 CCXT 调用方提交已经取得后继证据的 true；取消其专门取数在 CCXT 阶段完成。

### schema 与单位

从 schema version 1 升级为 2，升级在缓存模块的一个事务中完成：

- cache_segments 增加 data_kind 和 time_unit，现有行补为 ohlcv/ms。
- ohlcv_rows 增加可空 sdk_id BIGINT、open_oi DOUBLE、close_oi DOUBLE；CCXT 行保持 NULL，TQ 行原子写入对应值。
- TQ 普通片段为 ohlcv/ns；time、first_time、last_time、covered_from 都采用该片段原生整数单位。
- 所有现有行、segment_id、covered_from、sequence 和索引关系保留；不得用删库重建作为升级。
- 辅助数据后续使用独立类型化表与升级步骤，继续共享片段和生命周期；本阶段不提前建立空业务框架。

同一 series_key 必须对应唯一 kind/unit；发现冲突报内部资格错误，不能合并。跨序列容量淘汰按精确时间排序：毫秒先转 HUGEINT 再乘一百万，纳秒转 HUGEINT 原值比较；不能先在 BIGINT 中溢出或使用 float。排序相同时沿用稳定 segment_id 次序。

普通行情读写、计数、淘汰与元数据刷新显式限定 data_kind=ohlcv。原来“没有 ohlcv_rows 就删除 cache_segments”的全库逻辑必须改成类型内操作，为后续辅助表保留安全边界；不能等新增日历后才发现普通写入删除了其他类型片段。

旧版对新 schema 明确拒绝启动；不提供降级写入。升级失败回滚，版本号与表结构不能半更新，错误不自动删除旧文件。普通 OHLCV 的单序列/全库容量继续为既有配置与 90% 水位行为，TQ 纳入普通行情计数。

### 高级读取

read_contiguous_before 在给定包含式 end_time 之前选取含最新实际行的一个片段，只在该片段取末 N 根并升序返回。它仅描述已缓存历史，不证明缓存已追上 end_time；跨越空白范围不补行。

read_latest_summary 由同一快照返回最新实际片段的 start/end/count，及该序列 total_count/segment_count。最新由实际时间选，不按 segment_id 或 updated_at；start 是 first_time。空结果 null/null/0，统计不含未知网络尾根。

read_connected_history 接收已验证的完整网络批次，内部沿用实际时间戳交集确定可复用片段，返回可与本批次连接的缓存历史，最多 N 根。只使用符合原持久化/片段资格的交集；未知尾根不能独自制造连接。多个候选按可向前复用行数、更新时间、ID 稳定选一个，不跨独立片段拼接。

读取裁到本批次时间上界，不因另一请求写入更新行情而越界。没有连接则返回空历史；写入后被并发清理导致失去交集也按无连接处理。调用方只做结果去重组织，不重新证明片段连接。

普通 write_segment 与 write_tq_segment 共用筛选、重叠合并、修订和容量逻辑。TQ 附加值跟随整行修订、片段吸收及淘汰原子移动/删除，不能留下独立失配记录。

### 错误

保留普通读失败按 miss、普通写失败不丢成功网络响应、容量失败 507 的业务契约；缓存模块本身抛出准确错误供调用方处理。关闭后所有读写拒绝，不能重新创建 connection。

## 公开接口与用户写法

无 HTTP 参数变化。以下冻结供第一方业务使用的高级入口；已有三个方法保留原签名，新增方法不暴露 SQL 或可写片段句柄：

```python
cache.write_tq_segment(series, batch)
history = cache.read_contiguous_before(series.key, end_time, max_rows)
connected = cache.read_connected_history(series.key, batch, max_rows)
summary = cache.read_latest_summary(series.key)
```

新增结果类型位于 cache_tool 公开类型模块：CachedHistory.rows 保持其数据类型；SeriesSummary 包含 start、end、count、total_count、segment_count、time_unit，不包含 segment_id。端点为原生单位整数；max_rows 必须为正整数且不超过十万，内部既有 CCXT 100001 根溢出检测入口不受该新限制误伤。

例如两个片段分别存 24000 根旧行情和 6000 根最新行情，summary.count=6000、total_count=30000；向前读取最多取得所选最新片段的 6000 根，不能凑成三万根。

给定真实批次连接了两个旧片段时，普通写入口按原规则合并；调用方不能自行传 segment_id 或执行 connect_segments。查询读出末根不重复删尾。

## 测试、验证与阶段过渡

文档阶段不运行验证。实现阶段通过 just test、just lint、just check；新增离线临时 DuckDB 用例验证 schema 1→2 数据/证明保留、失败回滚、未知版本拒绝、整数单位及跨源时间排序。

保留原完整命中、含首连接、历史修订、桥接、容量、读写并发和关闭测试；新增向前读取、连接读取、最新片段统计、空库、相同端点候选与读写同快照用例。不得以实现私有调用次数代替业务结果。

覆盖只启用 TQ 的资源获取、仅 CCXT、混合消费者、关闭一个 Provider 后其他消费者可用、启动失败、并发首次创建和关闭等待。直接构造/重建 ExchangeManager 的测试和脚本同步迁移，避免生产与测试出现两套所有权。

本阶段即更新 Test/test_repository_contract.py 中仅允许三个公开缓存方法的旧约束，改为核对正式高级 API 与无网络/无 callback 边界；不能删除边界测试以放开底层连接。

同步 architecture、configuration、ohlcv_cache_storage、ohlcv_cache_operations 和 verification；此时 CCXT 查询和 TQ HTTP 行为仍是旧契约，不能提前写成后续能力。新辅助类型由其阶段增加，不以未完成辅助业务导致本阶段必测项跳过。
