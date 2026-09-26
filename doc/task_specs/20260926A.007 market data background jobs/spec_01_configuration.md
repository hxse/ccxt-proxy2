# 后台计划与 HTTP 客户端配置

本文冻结用户配置的完整写法。所有字段只控制后台脚本的请求计划；HTTP 路由的默认值、能力与用户请求范围由其自身规范定义。

## 公开计划文件

文件固定为项目根目录 market_data.toml，可纳入版本控制。每个参数都保留以下 # 注释；品种字典是默认用户示例，不是服务端白名单。

```toml
# 文件：项目根目录 market_data.toml；只控制后台请求计划。
# 调度器依次执行采集和清理，两类脚本分开维护。
[pipeline]
# 整轮任务间隔，单位秒；不表示官方元数据每小时强制刷新。
interval_seconds = 3600

# TQ 后台采集计划；这些值不会成为用户路由的默认参数。
[tq_collection]
# 是否自动执行 TQ 采集；关闭不影响用户调用路由或自动清理。
enabled = true
# 后台主动请求的行情周期；可从这四种周期中选择。
timeframes = ["5m", "1h", "1d", "1w"]
# 每次 OHLCV 请求的固定数量；默认 10000，不逐级扩大或重试凑数。
data_length = 10000
# 是否请求原始主连行情；不改写或复权主连价格。
save_main = true
# 是否请求对应的加权行情；与主连使用不同缓存序列。
save_weighted = true
# 是否根据指定周期主连响应的实际范围请求映射；须同时启用 save_main。
save_mapping = true
# 后台哪些行情周期触发映射；启用映射时必须非空且属于 timeframes。
mapping_timeframes = ["5m"]
# 是否在映射请求中附带过渡周期；后台启用此项须同时启用 save_mapping。
save_transition = true
# 是否每轮单独请求日历；关闭不禁止映射路由内部使用日历。
save_calendar = true
# 已触发映射的哪些周期附带过渡；启用过渡时必须非空且属于 mapping_timeframes。
transition_timeframes = ["5m"]
# 后台单独请求日历时，按可信在线日期向前取多少个日历年。
calendar_lookback_years = 10

# 后台主动采集的主连清单；左侧是别名，右侧完整代码才是数据身份。
# 加权代码按同交易所、同品种生成；保留大小写，不限制用户请求清单外品种。
[tq_collection.symbols]
rb = "KQ.m@SHFE.rb"  # 螺纹钢主连。
hc = "KQ.m@SHFE.hc"  # 热轧卷板主连。
ma = "KQ.m@CZCE.MA"  # 甲醇主连。
bu = "KQ.m@SHFE.bu"  # 沥青主连。
fu = "KQ.m@SHFE.fu"  # 燃料油主连。
l = "KQ.m@DCE.l"     # 聚乙烯主连。
pp = "KQ.m@DCE.pp"   # 聚丙烯主连。
ta = "KQ.m@CZCE.TA"  # PTA 主连。
v = "KQ.m@DCE.v"     # 聚氯乙烯主连。
m = "KQ.m@DCE.m"     # 豆粕主连。
rm = "KQ.m@CZCE.RM"  # 菜籽粕主连。
a = "KQ.m@DCE.a"     # 黄大豆一号主连。
b = "KQ.m@DCE.b"     # 黄大豆二号主连。
c = "KQ.m@DCE.c"     # 玉米主连。
cs = "KQ.m@DCE.cs"   # 玉米淀粉主连。
y = "KQ.m@DCE.y"     # 豆油主连。
sr = "KQ.m@CZCE.SR"  # 白糖主连。
cf = "KQ.m@CZCE.CF"  # 棉花主连。
ag = "KQ.m@SHFE.ag"  # 白银主连，仅保留一个条目。
au = "KQ.m@SHFE.au"  # 黄金主连。
cu = "KQ.m@SHFE.cu"  # 铜主连。
ru = "KQ.m@SHFE.ru"  # 天然橡胶主连。
jm = "KQ.m@DCE.jm"   # 焦煤主连。
j = "KQ.m@DCE.j"     # 焦炭主连。
al = "KQ.m@SHFE.al"  # 铝主连。
zn = "KQ.m@SHFE.zn"  # 锌主连。
i = "KQ.m@DCE.i"     # 铁矿石主连。
p = "KQ.m@DCE.p"     # 棕榈油主连。

# 后台清理请求计划；脚本将以下保留规则明确传给维护路由。
[retention]
# 是否自动请求清理；关闭不禁止用户自行调用清理路由。
enabled = true
# 本次清理的数据源类别；扫描其全部缓存，不限于上面的采集清单。
providers = ["tq", "ccxt"]

# 每条完整 OHLCV 序列跨所有片段的保留根数，不是用户响应上限。
[retention.modes]
# live 行情每条序列保留最新 30000 根；TQ 行情按 live 处理。
live = 30000
# sandbox 行情每轮清理到 0 根；两轮之间仍可正常写缓存。
sandbox = 0

# 辅助数据独立按年限保留，不套 OHLCV 根数限制。
[retention.auxiliary]
# 日历、逐日映射和过渡默认保留十年；必要节点上下文和整窗规则仍生效。
keep_years = 10
```

28 个完整 symbol 不重复；ag 只有一个。公开文件不放账号、密码、API key、JWT、token 或数据库路径。数据库路径继续来自私有 ohlcv_cache.database_path，不建立第二份配置。

## 私有 HTTP 客户端

在现有 config.toml 中增加以下片段，保留原 SECRET、users 和各服务账号；这不是完整私有配置文件。

```toml
# 文件：现有私有 config.toml；以下只是新增片段，不是完整文件。
[market_data_client]
# 后台脚本实际访问的本项目服务地址；容器内运行时按容器监听端口填写。
base_url = "http://127.0.0.1:5123"
# 引用已有 [users.admin] 的 HTTP 登录账号；示例名须换成本地实际配置的键。
user = "admin"
# 后台单次 HTTP 请求的等待上限，单位秒；不覆盖路由或 SDK 自身超时。
request_timeout_seconds = 60
```

user 引用已有 users 中的 HTTP 账号并读取其 password，通过 /auth/token 登录；不复制密码，不使用 TQ 登录账号替代，不绕过鉴权，不隐式选择第一个用户。示例 admin 需要在该私有文件的 users 中存在。

base_url 不配置服务监听地址。容器内访问容器实际监听端口，例如 8000；request_timeout_seconds 只限制客户端每次等待，不覆盖路由/SDK 的超时。

## 加载与默认值

公开计划路径按项目根目录解析，不随 cwd 改变；私有文件沿用 CCXT_PROXY_CONFIG_PATH 和现有加载器。自动调度与单轮 CLI 均在启动时读取并校验，运行中修改须重启，不热加载。

除 symbols 和 market_data_client.user 外，省略字段采用上述示例默认值。启用按品种采集时必须给非空 symbols；存在实际后台 HTTP 工作时必须给 user。配置中的字典/周期列表整体替换计划，不与示例清单隐式合并。

公开文件缺失、TOML 非法、未知字段、错误类型或非法组合明确报错，不静默启用部分计划。私有 user 仅在有实际 HTTP 工作时要求；显式存在的字段仍须类型合法。日志不包含原始私密值或完整配置异常链。

## 逐项校验

| 参数 | 规则 |
| --- | --- |
| pipeline.interval_seconds | 正整数，默认 3600；用于调度，不是源表 TTL |
| tq_collection.enabled、save_*、retention.enabled | 严格布尔值，默认 true |
| tq_collection.timeframes | 非空、不重复，取自 5m/1h/1d/1w；只限制后台 |
| tq_collection.data_length | 整数 1..10000，默认 10000；不限制用户路由十万响应上限 |
| tq_collection.mapping_timeframes | 合法周期且不重复；默认 ["5m"]，启用映射时非空且属于 timeframes |
| tq_collection.transition_timeframes | 合法周期且不重复；默认 ["5m"]，启用过渡时非空且属于 mapping_timeframes |
| calendar_lookback_years | 正整数，默认 10，只影响主动日历范围 |
| tq_collection.symbols | 别名和完整 KQ.m@ 代码非空，值不重复，交易所/品种大小写保持 |
| retention.providers | 非空、不重复，只允许 tq/ccxt，默认两者 |
| retention.modes.live/sandbox | 非负整数，默认 30000/0；bool 不当整数，0 不回退默认 |
| retention.auxiliary.keep_years | 正整数，默认 10，只影响辅助保留 |
| market_data_client.base_url | 绝对 HTTP(S) 地址，不含用户名/密码/query/fragment |
| market_data_client.user | 已有 users 的非空账号引用，无默认首用户 |
| market_data_client.request_timeout_seconds | 有限正数，默认 60 |

## 后台依赖与路由作用域

对启用的采集计划：save_mapping=true 要求 save_main=true，并要求 mapping_timeframes 非空且为 timeframes 子集；save_transition=true 要求 save_mapping=true，并要求 transition_timeframes 非空且为 mapping_timeframes 子集。这些条件来自脚本用指定周期主连响应构造范围的方式，不加入用户映射路由校验。

默认四个周期都取主连和加权，只有 5m 再取映射及过渡；1h/1d/1w 不触发映射。只关闭 save_transition 时，mapping_timeframes 仍决定正常映射请求，不能让两个周期列表承担同一开关职责。映射逐日保存，不按列表复制多份日表。

关闭整个采集计划后不执行它的运行依赖检查，未知字段与类型仍校验。save_calendar=false 只关闭后台独立日历请求，不禁止映射路由使用必要日历依赖。

retention.enabled=false 只停止自动清理，用户可主动调用清理路由；路由不读取这里的规则作为隐式默认。providers 不含 tq 时脚本发送 auxiliary=null，不能请求清理类别之外的数据。

transition_bars 不在计划中重复配置，后台沿用路由默认 10；用户可通过路由请求其他合法数量。公开计划不加入源强刷、TTL、interval 补拉、数量达标或 CCXT 主动采集字段。

## 真实请求构造

默认 rb/5m 先请求 /tq/fetch_ohlcv，symbol=KQ.m@SHFE.rb、duration_seconds=300、data_length=10000、enable_cache=true。非空响应的首尾 datetime 原样进入映射 start_time/end_time，附 transition_timeframe=5m；再按开关请求 KQ.i@SHFE.rb。

默认 rb/1h、rb/1d、rb/1w 只请求对应主连与加权。它们即使返回更早历史，也不扩大后台映射采集范围；5m 为空/失败不回退这些周期。用户主动请求更早映射仍按路由能力处理。

清理默认提交：

```json
{"providers":["tq","ccxt"],"modes":{"live":30000,"sandbox":0},"auxiliary":{"keep_years":10}}
```

反例：把 save_main=false、save_mapping=true 放在启用的后台计划中，启动前失败；mapping_timeframes=["5m"] 时启用 transition_timeframes=["1h"] 也失败。同一用户直接调用合法范围的映射/1h 过渡仍合法。将 sandbox=0 解释成“不清理”属于错误实现。

更细全局/数据源/品种覆盖不在本轮公开配置中，未知键拒绝。修改这里已冻结的用户写法须同时维护配置实现、示例、正式规范与对应离线测试，不依赖外部材料补全含义。
