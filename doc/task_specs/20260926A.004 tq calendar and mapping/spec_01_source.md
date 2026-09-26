# 元数据源、刷新证据与 SDK 硬限制

## 提取范围和来源

以 TqSdk 3.10.2 的 calendar.py 及必要日期转换为等价基准，保留作者、Apache-2.0 许可和修改说明。不复制整个 SDK，不改安装目录或全局 monkeypatch。

唯一模块职责：tq_metadata_source.py 下载官方文件，tq_metadata_conversion.py 转换显式日期和有效节点，tq_metadata_query.py 编排业务与缓存；tq_metadata_sdk.py 仅取得参考 K 线时间，不读取当前合约关联。

源为 https://files.shinnytech.com/shinny_chinese_holiday.json 和 https://files.shinnytech.com/continuous_table.json；集中支持 TQ_CHINESE_HOLIDAY_URL、TQ_CONT_TABLE_URL 覆盖。请求头由 SDK 所属线程提供稳定副本，不泄露凭据或敏感 URL。

每次下载总时限十秒、无重试，逐层验证 HTTP、JSON 和结构。只共享同源在途下载，完成的旧结果不能作为下一请求的新下载。应用拥有异步 HTTP 客户端，关闭时收尾；网络不占行情 SDK 工作线程或数据库锁。

## 唯一映射依据及原因

官方历史事件源是当前 items、历史 history 和休市兜底合约解析的唯一映射依据。SDK 当前合约资料可能已标注正确 trading_day，但 underlying 关联仍落后于主连真实价格和官方历史事件；get_quote 的 underlying_symbol 同样由这份合约资料填入，不是独立核验来源。

因此退出 query_symbol_info 以及 get_quote/query_graphql 当前标的回退，删除 TQ_MAPPING_NOT_CURRENT 两源相等检查。接受官方历史源自身的发布时效，不把重新下载解释为上游绝不迟报；不增加价格匹配、上市日期搜索或另一套当前合约算法。

## 源结构与等价转换

节假日源为合法自然日期集合，相同日期去重，非法项失败。实际最小/最大日期从全部解析值取得；H 为最大节假日，有效范围仍为首末源日期所在年份的全年，不把 H 改成年末。

日历按标准 date/timedelta 枚举完整自然日；工作日且不在节假日集合时 trading=true。越出有效年份失败，不按星期无限延长，不用行情 interval，不以日历证明合约开市。

映射源为 symbol 对应的日期/合约事件序列。同日相同事件去重，冲突或非法内部值失败。保留范围前节点及非交易日事件，排序、向后沿用并筛选交易日。非交易日期事件在首个有效交易日生效，连续公布的事件以当日最终值为准。

原始事件日期并非文件更新日期，也不是逐日覆盖终点。不得用最后换月日期判过期。D 之后的预公告允许存在于源快照，但不进入 items、history、日映射缓存或过渡触发。

未上市前明确连续空白简单裁掉且不持久化，中间空白失败。未知主连为 TQ_NOT_CONT_SYMBOL。当前已生效日没有有效映射为 TQ_MAPPING_INCOMPLETE。边界处理不增设上市前恢复流程。

start/end 是项目内转换范围，不是官方文件服务器接受的参数；事件源不受 OHLCV 一万根窗口限制。历史前驱和真实节点直接从本次完整事件源定位，不按 n 试探。

## 日期与请求上下文

带范围和不带范围的映射查询，都固定一次公共在线 serverTime，保持五秒、无重试、无本机回退。先取得本次事件源并验证 symbol，再从同一主连获取 duration_seconds=300、data_length=1 的稳定原始 SDK 副本，只使用其开盘纳秒时间 reference_time；不写普通行情缓存、不读 Quote 当前标的、不用价格猜合约。

reference_time 按 TQ 十八点/周末归属规则与有效日历解释为生效交易日 D。空序列、非单根或非法参考为 TQ_MAPPING_REFERENCE_UNAVAILABLE；SDK 网络及权限错误保持相应契约。公共时间用于边界合理性检查，不能以时钟前进自动推进 D；不以旧缓存事实替代本次参考。

只有 D 及以前的有效事件可转换。当前 U 直接从本次历史源在 D 的节点取得，不是第二份 SDK 输入。真实行情仍停在旧交易日时，未来预公告保持未生效；真实夜盘进入下一交易日后再启用对应节点。请求有效交易日超过 D 返回 TQ_MAPPING_RANGE_UNAVAILABLE，不静默裁短。

MetadataContext 只含 serverTime、源快照和 reference_time，不接受用户提供 now/as_of。MappingSourceResult 保留日 records、真实节点和前驱、D 的 verification_node、源事实；MappingFacts 的 underlying_symbol 表示源在 D 的合约，verified_date 表示经过本次日期边界校验的 D，不表示两个独立来源已经一致。

源事实保留事件/日历摘要、serverTime、D、U、reference_time，与行及上下文同事务提交。当前日和用户旧历史间有缺口时只提交两者，不填充整个间隙；保持独立片段。

## 每请求获取与缓存复用

日历独立路由继续按 C/H/覆盖判断：缓存开启且 C<H、已知有效范围与完整日期覆盖满足请求时复用，否则下载一次。H 未延长不循环；下一请求仍按条件判断。enable_cache=false 禁止旧事实和数据读写。

映射路径统一为：

```text
固定可信时间，重新取得本次官方事件源
→ 获取主连最新单根参考时间，结合日历确定 D
→ 按本次源转换目标日表、真实节点和 D 上的当前合约
→ 范围越界或源非法：明确失败，不提交事实
→ 缓存高级入口核对摘要、当前日、目标范围和真实节点
→ 完整相同：复用缓存，不重写映射日表与节点
→ 未命中或有修订：原子提交本次有效结果
→ 当前 items 与可选 history 均使用同一份来源
```

所需日历源在同一操作内最多获取一次，供映射转换及后续过渡共用。日历缺覆盖或确有修订时按原日历提交规则更新；不在同一响应中混用新旧日历。

缓存新增高级入口 read_matching_mapping(MappingSourceResult)，在一个读取事务中复用 read_mapping 的覆盖和节点检查，同时比较事件/日历摘要、目标日记录、节点前驱及 D 节点。返回完整 CachedMapping 或 miss，不向业务暴露 SQL、segment 或部分结果。

摘要属于该序列最近一次成功提交，不能证明所有旧片段均已按新源更新。因此即使摘要相同，仍须核对本次目标内容；先更新区间 A 后，旧区间 B 必须能按正常请求修复，不能仅凭 A 写入的新摘要信任 B。实际日期交集、片段选择与合并继续由缓存模块负责。

不缓存第二份可任意修改的事件数据库，不按小时 TTL 或末换月日期跳过网络。下载失败不能用完整缓存冒充成功；成功但写库失败按既有错误契约返回本次官方结果，不宣称已持久化。

## 当前查询与休市兜底

省略范围仍返回 items，序列化继续省略未设置的 history；items 合约来自源在 D 的节点。ins_class=CONT、exchange_id=KQ、product_id 空字符串描述主连自身，保持原字段形状。带范围时返回同一当前 items 和历史真实换月节点。

TqManager 的 OHLCV 编排使用异步入口；SDK 工作和缓存操作在线程中执行。仅既有可兜底网络失败分支进入同一个异步映射查询，不创建第二个下载器或阻塞主事件循环。实际合约仍直接查自身状态；主连/加权解析失败为 TQ_TRADING_STATUS_UNAVAILABLE，权限错误保留 403。

必须取得可靠实际合约后读取独立交易状态快照；只有 NOTRADING 且 is_open=false 才返回原序列连续缓存。不能用日历或历史映射推断休市；映射失败不得退回旧当前标的。

## 错误与硬限制

公共时间错误沿用 PUBLIC_TIME_*；下载失败为 502 TQ_METADATA_SOURCE_UNAVAILABLE，总超时为 504 TQ_METADATA_SOURCE_TIMEOUT，非法源为 502 TQ_METADATA_INVALID_SOURCE。日历越界为 422 TQ_CALENDAR_RANGE_UNAVAILABLE，历史超 D 为 422 TQ_MAPPING_RANGE_UNAVAILABLE；无有效参考、映射缺失和节点不明沿用 TQ_MAPPING_REFERENCE_UNAVAILABLE、TQ_MAPPING_INCOMPLETE、TQ_MAPPING_CONTEXT_UNAVAILABLE。

生产唯一 TqApi 适配实例覆盖 get_trading_calendar、query_his_cont_quotes、query_symbol_info，调用立即抛 TqLegacyMetadataCallForbidden；映射为 500 TQ_LEGACY_METADATA_CALL_FORBIDDEN。没有开关，也不保留 fetch_current_underlying、resolve_underlying、get_underlying 旧入口。

第一方 src/scripts/script/debug 的 AST 检查禁止旧方法、get_quote/get_quote_list/query_graphql 及其引用、getattr、别名、super/基类调用和原始 TqApi 构造。禁止 tqsdk.calendar 私有入口。此规则约束项目业务；SDK 内部为普通行情加载合约基础信息仍可运行，该附带字段不得成为项目映射依据。

唯一离线参考 Test/helpers/tq_metadata_reference.py 只放行旧日历/历史转换，固定 fixture 代替下载并恢复全局状态。不能因此放行当前合约查询，也不能联网或被生产导入。

## 验证

通过离线 just test、just lint、just check。保留日历自然日齐全、真实日期交集、历史节点/前驱、未上市前缀、未来边界和事务失败测试；转换与官方离线参考在相同有效范围等价。

新增覆盖：无范围也取在线时间和参考并下载；同源完整命中不重写；不换月也每次下载；官方修订且当前 U 未变仍更新历史；新摘要不能掩盖另一旧片段；源失败不落盘或回退；缓存关闭无读写；主连和加权休市兜底走同一来源，元数据失败不猜状态；当前入口硬禁且参考获取完全不调用它。

在线仅在最终集成版本经独立 just test-tq-online 手动运行一次，使用 live 只读请求和隔离缓存，比较当前 items、历史范围与参考交易日；包括沥青，不能用旧 SDK 映射作成功前置条件。SDK 数据或网络不满足条件时如实失败，不反复请求找成功样本。
