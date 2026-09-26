# TQ 日历与主连映射迁移

## 任务边界

共同规则见[主任务契约](<../20260926A market data caching and maintenance/spec_01_contract.md>)，普通行情获取边界见[TQ 前置任务](<../20260926A.003 tq ohlcv cache integration/spec.md>)。本任务同时替换日历和历史映射源、接入其缓存并返回节点；不实现过渡价格。

获取、刷新、转换、源事实与硬禁细节见 [spec_01_source.md](spec_01_source.md)。两份 spec 共同构成完整规范。

原 SDK get_trading_calendar/query_his_cont_quotes 生产调用退出，自有实现不反调旧入口。不修改 .venv 或全局 monkeypatch，不替换普通行情、Tick 和状态 SDK；SDK 当前标的查询退出，items 统一由历史源派生。旧 n、映射多 symbol 和宽表响应退出；items 当前标的职责保留。

停止线：两个元数据路由在新链上可用、时间可信且刷新条件准确、有效日期覆盖与节点上下文可复用、旧入口无生产旁路。没有过渡参数实现前继续拒绝它们，不能返回假空实现。

## 任务规范

### 分层与服务边界

业务查询取得一次公共在线时间，固定源快照和边界；转换层只接受明确日期与源数据；缓存层只接收类型化有效结果。元数据 HTTP 下载和 Pandas 转换在 SDK 行情工作线程及数据库写锁之外执行。

当前标的只由每次重新取得的官方历史事件源在生效日 D 的节点派生；query_symbol_info 硬禁，不保留 get_quote 回退，保持 TQ 服务门禁与行情权限。公共时间调用 public_time.py 的函数，不经本机 HTTP 回调自身；显式历史请求也执行在线检查。

历史请求还通过无落盘 SDK 获取入口读取同一主连最新一根 5m K 线，仅取其实际行情时间作为核验日期证据；作为本次稳定边界；不读取 SDK 当前 U。它不是第二次历史采集，不进入普通缓存，不用价格猜 underlying，不查询上市日期。缺少可靠参考时直接报错，不分页搜索其他参考日期。

### 日历

请求闭区间 [A,B]，范围内每个自然日必须恰有一条 date/trading，包括非交易日。日期合法、唯一、升序且在范围内，行数必须为 (B−A).days＋1。用标准 date/timedelta 处理跨月、跨年和闰日，不引入行情 interval。

自动检查通过且缓存片段完整覆盖时可复用；否则自有入口下载并转换目标范围。需要补接时通过缓存高级入口获取所需边界上下文，保留实际日期重叠。不能仅因两段日期相邻自动合并。

写前验证与返回前验证都检查完整范围，缺日期不能填 trading=false。日历不去尾，不判断合约是否开市，不为普通 OHLCV 完成性提供证据。

### 日映射与节点

日映射以完整主连 symbol、trading_date 保存 underlying。转换保留范围前事件、非交易日事件，在本次已生效上界 D 内按官方算法展开，再验证交易日覆盖并派生节点。已下载表中明确的未上市前连续空白前缀简单裁掉、不持久化；处理不了就报错，不加上市前恢复流程。有效历史中间缺失不能统一过滤。

同一可信连续序列 d1=A、d2=A、d3=B、d4=B 中，真实节点是 d3，old_symbol=A。用户从 d4 开始也返回 d3；不跨片段缺口比较新旧合约。A→B→C→D→E 中查询 C..E 时取得 B 作为 C 的前驱，但响应主体只有 C、D、E。

左边界真实节点与旧合约上下文同日表一起返回给业务并交缓存保存。上下文是日映射的配套事实，不能由路由维护另一份独立事件表。可靠缓存不足时从已下载事件表定位，不再次按 n 试探。

前驱缺失但真实节点日期已知时，old_symbol=null；不得虚构前驱。需要解释有效映射但真实节点日期也无法确认时，返回 502 TQ_MAPPING_CONTEXT_UNAVAILABLE，不生成假节点。合法未上市/全非交易范围且无有效映射则 history=[]；主体源失败仍报错。

有当前核验目标 D 但用户请求旧历史时，只保留本次核验所需 D 的日映射及其源事实，不能自动填充旧范围到 D 的整个缺口。D 与历史若无实际交集，继续为独立片段；该核验行不加入用户历史响应。

### 日期与时间

HTTP 历史范围为包含式 start_time/end_time，整数 Unix 纳秒。服务先按提取的 TQ 交易日归属函数解释 K 线时刻，再由有效日历筛选对应日期；不能将夜盘时刻简单取自然日。

当前核验 D 由本次稳定参考行情的实际时间按 TQ 交易日归属与有效日历解释，定义详见源专题。SDK 18 点/周末换算可用于解释真实 K 线时间，不能直接把在线“现在”换算得到的下周交易日设成 D。休市期间 D 可以仍是较早的最后行情交易日，不以时钟前进要求补出尚未发生的映射。

D 同时是本次历史映射最晚可信日期。请求解释后的有效交易日超出 D 时返回 422 TQ_MAPPING_RANGE_UNAVAILABLE，不静默裁掉未来部分后返回成功；未来事件不生成日行、覆盖证明、history 节点或过渡触发。单纯非交易日按已有日历语义处理，不能把日历能公布未来日期解释为主连历史也已生效。

源表可以含 D 之后的真实预公告，解析时允许保留原始事实，但本轮不新增未来事件查询接口或持久化表。将来实际行情进入该交易日后，再按正常日期边界和本次源快照纳入历史。

跨长假或超出日历可信覆盖时不能仅按星期补造日期。日期转换核心不读本机当前时间；服务端时钟失败明确传播，客户端任意 now/as_of 参数一律不接受。

### 类型化缓存与 schema

缓存基础 schema 2 升级为 3，升级事务保留普通行情。新增类型化表：

| 表 | 业务内容 |
| --- | --- |
| calendar_rows | segment_id、date、trading；业务身份为全局 TQ 日历 |
| mapping_rows | segment_id、symbol、trading_date、underlying_symbol、真实 roll_date；逐行关联同片段的确切节点，防止历史修订后引用旧节点 |
| mapping_context | 随映射覆盖保存的真实节点与前驱，关联相应日序列；不是另一个可独立修改的事件源 |
| metadata_source_facts | 源种类、摘要、官方边界及本次可信核验事实；不存完整任意 DataFrame |

日历/映射片段 kind 分别为 calendar/main_mapping，time_unit=date，片段内部边界使用标准 date.toordinal 整数；业务列保持 DATE。身份独立于普通行情和周期，不为四个周期重复保存日表。

提交 calendar/mapping 的数据、上下文和相关源事实在同一写事务完成；失败不推进“已核验”事实。映射结果明确携带 D，业务层提交前与缓存入口都拒绝超出该上界的日行/已生效节点，已有缓存读取也受本次 D 限制。高级入口为 read_calendar_range、submit_calendar、read_mapping_range、read_matching_mapping、submit_mapping、read_metadata_facts；通过类型化输入输出使用，禁止任意事实键写入。

range 读取返回明确的完整命中或 miss，不将部分行伪装完整。日期修订、边界扩展和片段合并复用原机制；只有有效批次的实际日期交集才连接。源缺少旧记录不自动删除已有历史。

复用内部片段原语时按确定的数据种类绑定对应行表；普通 OHLCV 刷新/容量统计不得作用于 calendar/main_mapping。日历或映射也不能用 ohlcv_rows 是否存在来判断片段为空，数据与所属片段的类型必须一致。

## 公开接口与用户写法

日历保留原路径与日期参数，增加 enable_cache=true：

```text
GET /tq/fetch_trading_calendar?start_date=2026-09-01&end_date=2026-09-05&enable_cache=true
```

```json
[{"date":"2026-09-01","trading":true},{"date":"2026-09-02","trading":true},{"date":"2026-09-03","trading":true},{"date":"2026-09-04","trading":true},{"date":"2026-09-05","trading":false}]
```

映射保留 /tq/fetch_underlying_symbol，参数冻结为：

| 参数 | 规则 |
| --- | --- |
| symbol | 必填单个完整 CONT 代码，重复同值也拒绝 |
| start_time、end_time | 均省略则仅当前 items；否则两者必填，正整数纳秒且 start≤end |
| enable_cache | 默认 true；false 不读写项目缓存和源事实 |

```text
GET /tq/fetch_underlying_symbol?symbol=KQ.m@SHFE.rb
GET /tq/fetch_underlying_symbol?symbol=KQ.m@SHFE.rb&start_time=1790125200000000000&end_time=1790211600000000000
```

历史形状示例，不代表该日期的真实合约：

```json
{"items":[{"symbol":"KQ.m@SHFE.rb","underlying_symbol":"SHFE.rb2701"}],"history":[{"date":"2026-09-24","symbol":"KQ.m@SHFE.rb","underlying_symbol":"SHFE.rb2701","old_symbol":"SHFE.rb2610"}]}
```

无历史范围时模型 history=[]，HTTP 继续省略未设置的 history；items 保留主连 metadata 字段，同样取在线时间、参考行情和本次历史源。历史节点按日期升序；允许解释左边界的真实节点早于 start_time 所属交易日。

旧 n、refresh_source、now 等未声明参数返回 422；多 symbol 返回 400 TQ_MULTIPLE_SYMBOLS_NOT_SUPPORTED；范围一端缺失或倒置为 400 TQ_INVALID_DATE_RANGE。非 CONT 为 TQ_NOT_CONT_SYMBOL，源在 D 无有效合约为 TQ_MAPPING_INCOMPLETE，日历超范围沿用原稳定错误。

映射每次在线获取官方事件源；摘要与完整目标内容一致才复用缓存，否则经既有缓存接口提交修订。false 不访问旧事实或缓存。用户可独立请求日历或映射，后台采集是否启用、是否先请求主连均不影响此接口。

历史范围受本次实际生效日 D 限制；缺少有效行情时间为 502 TQ_MAPPING_REFERENCE_UNAVAILABLE，查询有效交易日超过 D 为 422 TQ_MAPPING_RANGE_UNAVAILABLE。未来预公告不改变 items；TQ_MAPPING_NOT_CURRENT 两源相等检查退出。

## 测试、验证与阶段过渡

实现时通过 just test、just lint、just check；文档阶段不运行。新源等价性、禁用反向用例和架构检查见源专题，均进入离线入口。测试不访问真实源文件。

日历覆盖跨年、闰日、逐日缺失、重复、越界、实际日期重叠与相邻但未连接；映射覆盖非交易日事件、前置事件、未上市前缀、内部缺失、真实节点、无前驱、未知节点和左边界截取。

以固定样本验证休市时参考行情停在上个交易日、源表已经公布下一交易日新合约：当前合约取旧交易日的有效节点，不能提前启用未来预公告。覆盖夜盘实际进入次日、参考缺失/非法、未来范围拒绝、直接提交超 D 行被拒绝，以及真正生效后正常接入。上市前只验证简单裁剪或准确失败，不要求额外恢复。

验证 source facts 与行事务一致、只核验 D 不填充中间缺口、并发刷新不混快照、单位准确、缓存关闭无读写、公共取时失败不能因命中绕过。无历史 items 查询统一进入历史源链，完整命中也不跳过网络取源；历史修订、旧片段和休市兜底迁移按源专题验证。

源获取替换、TqManager 执行边界、SDK 工厂禁用、请求/响应模型、fixture 和旧调用移除同阶段完成；普通行情、Tick、状态及其 SDK 消息观察必须继续可用。当前阶段更新 online 调用；联网验证留到最终集成版本，通过独立 live 只读入口执行。

同步 tq_data、tq_processing、system_time、缓存存储/API、configuration、verification 和 Bruno；复杂源与节点规范可分别新增 current spec 主题，现有入口提供导航，不复制规则。旧 n 写法不保留兼容包装。
