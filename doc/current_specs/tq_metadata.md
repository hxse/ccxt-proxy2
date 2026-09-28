# TQ 日历与历史主连元数据

## 唯一链路与禁用原因

日历与映射走第一方官方源下载、显式日期转换及类型化缓存。当前 items、历史 history 和 OHLCV 休市兜底中的合约解析统一以 continuous_table.json 为映射依据；SDK 只提供同一主连最新单根 5m K 线的时间。

SDK 当前合约资料可能标注正确交易日，却仍返回旧 underlying；主连行情和历史事件源已经切换。get_quote 的 underlying_symbol 同样由这份合约资料填入，不能作为独立核验。以它否决历史源会造成有效历史查询失败，因此不再读取该来源，也不再执行 TQ_MAPPING_NOT_CURRENT 两源相等检查。

生产适配实例硬禁 get_trading_calendar、query_his_cont_quotes、query_symbol_info，调用立即抛 TqLegacyMetadataCallForbidden，映射为 500 TQ_LEGACY_METADATA_CALL_FORBIDDEN。没有开关或旧回退。第一方 AST 检查同时禁止 get_quote/get_quote_list/query_graphql、旧包装及反射/别名旁路；SDK 内部为普通行情加载合约基础信息仍允许运行，其附带标的字段不作为业务映射依据。

参考 TqSdk 3.10.2 calendar.py/datetime.py 的转换，保留 Apache-2.0 许可和作者说明。官方源为 shinny_chinese_holiday.json 和 continuous_table.json，集中支持 TQ_CHINESE_HOLIDAY_URL、TQ_CONT_TABLE_URL。认证头从 SDK 所属线程取得稳定副本，不泄露。

日历与主连源下载遵守 tq.enable_proxy，与 SDK 共用启动时选定的 proxy.effective_http 地址；关闭时不继承环境代理。下载刷新、在途合并及关闭规则保持不变，公共时间仍使用独立入口。

每次下载总时限十秒、无重试，仅共享同源在途下载。网络和数据库操作在 SDK 线程及 FileLock 外执行；应用关闭 HTTP 客户端和在途业务。重新下载保证本次访问官方源，不承诺官方自身没有发布延迟。

## 请求时间、刷新与生效边界

每个日历/映射请求先固定一次公共在线 serverTime，保持五秒、无重试、无本机回退。不接受 now、refresh_source 或小时 TTL。映射不带历史范围时也走同一条源链。

日历以可信中国自然日 C 与实际最大节假日 H 比较。C≥H、无 H、缺覆盖或超出已有有效年份时下载一次，否则复用完整缓存。H 从全部源日期取最大值，不改成年末；有效范围仍是首末源日期所在年份的全年。H 未延长不循环；下次请求继续按条件判断。

映射每次重新获取官方历史事件源，校验 symbol 后只取一根最新主连 5m 参考行情，不进入普通行情缓存、不查当前标的、不从价格猜合约。参考为空、不是单根或数据非法均明确失败；网络/权限错误保持相应契约。

reference_time 按 TQ 十八点/周末归属规则与有效日历解释为生效交易日 D。公共时间只检查日期合理性，不直接把周末时钟推到下周当成 D。未来预公告可以保留在本次源快照，但不进入 items、history、日映射缓存；请求有效交易日超过 D 返回 422 TQ_MAPPING_RANGE_UNAVAILABLE，不静默裁短。

当前合约 U 从该源在 D 的有效节点取得。原始末次换月日期不是逐日覆盖终点，不用于判断是否刷新。长期没有换月也每次重新取源。日历和事件源在一次操作内保持稳定快照，各最多下载一次。

## 映射缓存复用

先按本次源和 D 转换有效日表、真实节点及前驱，再通过 read_matching_mapping 检查缓存。事件/日历摘要相同，D 和目标范围完整且具体日记录、节点及前驱一致时复用；否则用 submit_mapping 原子提交本次结果。完整映射命中不重写日表和节点；确有日历刷新或修订时仍按日历规则提交。

核对由缓存高级入口在同一个读取事务中完成，业务不操作 SQL、segment_id 或底层连接。全局摘要记录最近提交，不能证明其他旧片段已更新；修订区间 A 后再读旧区间 B，B 的内容仍须符合本次源，不能借新摘要静默返回旧映射。

源失败或转换失败明确报错，不能因缓存完整就返回旧结果。下载成功但缓存写失败可返回本次官方结果，不宣称新的事实已持久化。日期交集、片段连接与修订继续使用原机制。

## 日历与主连接口

GET /tq/fetch_trading_calendar 保留 start_date/end_date 自然日闭区间和 enable_cache=true。逐日返回 date/trading，包含 false；用标准 date/timedelta 验证跨月、跨年、闰日的完整集合。缺行不能填 false，超有效年份为 TQ_CALENDAR_RANGE_UNAVAILABLE。日历不证明开市或 K 线完成。

```text
GET /tq/fetch_trading_calendar?start_date=2026-09-01&end_date=2026-09-30&enable_cache=true
GET /tq/fetch_underlying_symbol?symbol=KQ.m@SHFE.rb
GET /tq/fetch_underlying_symbol?symbol=KQ.m@SHFE.rb&start_time=1790125200000000000&end_time=1790211600000000000
```

映射只接受单 symbol，重复同值也拒绝。范围必须成对、升序、正整数 Unix 纳秒，夜盘按交易日解释。enable_cache 默认 true；false 不读写项目数据或源事实。

省略范围只设置 items，HTTP 省略未设置的 history（模型默认空列表）。items 为本次历史源在 D 的合约，ins_class=CONT、exchange_id=KQ、product_id 空字符串描述主连自身；不再依赖 SDK 当前资料。

带范围时同时返回 items 与历史节点，结构示例：

```json
{"items":[{"symbol":"KQ.m@SHFE.rb","underlying_symbol":"SHFE.rb2701","ins_class":"CONT","exchange_id":"KQ","product_id":""}],"history":[{"date":"2026-09-24","symbol":"KQ.m@SHFE.rb","underlying_symbol":"SHFE.rb2701","old_symbol":"SHFE.rb2610"}]}
```

示例只说明结构。日库保存逐有效交易日映射，history 只返回真实换月节点；查询从合约中段开始仍返回真实节点，可早于请求起点。源保留范围前节点与非交易日事件，按官方规则向后沿用再筛选交易日；相同合约不生成新节点。

前驱未知但节点日期已知时 old_symbol=null，无法确定真实节点为 TQ_MAPPING_CONTEXT_UNAVAILABLE。未上市前明确连续空白简单裁掉，合法全非交易范围可返回空 history；内部坏值/缺失报错。旧 n、多个 symbol、now、refresh_source 均退出。

可选 transition_timeframe/transition_bars 在真实节点附带旧合约价格，详见[换月过渡](tq_transition.md)；不传周期时省略 transition。

## 日期存储与事实

schema 2→3 原子增加 calendar_rows、mapping_rows、mapping_context、metadata_source_facts，保留普通行情。日期片段 data_kind 为 calendar/main_mapping、time_unit=date，片段边界为 date.toordinal，业务列使用 DATE。

逐日映射保存真实 roll_date，并关联同片段的确切合约节点。官方修订移动/撤销节点时更新该关联；部分新旧记录矛盾则完整读取 miss，不用剩余首日伪造节点。上下文随日表原子保存，不构成另一份事件数据库。

schema 5 为缺少 roll_date 关联的旧 schema 3/4 补齐结构；旧日行和上下文保留，未证明关联的日行按完整读取 miss，正常源查询成功后原子补齐。清理保留未重新核验行所需的旧上下文，不将结构升级当成新的源事实；详见[缓存存储](ohlcv_cache_storage.md)。

身份为 provider=tq/mode=live；日映射另含完整 symbol，与 OHLCV 周期无关。只有实际日期交集可连接片段，日期相邻不自动连接。源未出现旧记录不等于删除。

高级入口为 read_calendar_range、submit_calendar、read_mapping_range、read_matching_mapping、submit_mapping、read_metadata_facts。返回完整命中或 miss；读事务覆盖选择、数据、节点与事实，写事务覆盖数据、上下文与事实，失败回滚。

MappingFacts 保留两源摘要、serverTime、D/U/reference_time；U 来自历史源，verified_date 是经日期边界检查的 D，不代表双源一致。历史与 D 有缺口时只保存历史及独立 D 行，不填充间隙；提交和读取都受 D 限制。日历/日映射不使用普通 OHLCV 去尾规则。

普通行情计数与淘汰只处理 ohlcv，不误删日期片段。映射 SDK 参考也不写普通行情表。

## 休市兜底与验证

TqManager 的 OHLCV 异步编排仅在既有可兜底网络失败时查询同一个映射入口，再读实际合约交易状态。SDK 和缓存操作在线程执行，下载不阻塞事件循环。实际合约直接查自身；主连/加权不能回退旧 SDK 标的，映射失败为 TQ_TRADING_STATUS_UNAVAILABLE，权限错误保留 403。

只有真实 NOTRADING 且 is_open=false 才能读原序列连续缓存。不能凭历史映射、日历或时段判休市；正常行情成功不额外查映射或状态。

源网络/HTTP 错误为 502 TQ_METADATA_SOURCE_UNAVAILABLE，总超时为 504 TQ_METADATA_SOURCE_TIMEOUT，非法源为 502 TQ_METADATA_INVALID_SOURCE。参考不可用、无有效映射和节点不明分别为 TQ_MAPPING_REFERENCE_UNAVAILABLE、TQ_MAPPING_INCOMPLETE、TQ_MAPPING_CONTEXT_UNAVAILABLE。公共时间失败原样传播。

离线验证每请求取源、当前/历史统一、未来边界、完整命中不重写、跨片段历史修订、缓存关闭、失败不污染及休市兜底。运行时与 AST 双重约束旧入口；唯一离线参考 Test/helpers/tq_metadata_reference.py 只运行 fixture 下的旧日期转换，不放行当前映射查询。在线入口独立，最终手动 live 只读验证。
