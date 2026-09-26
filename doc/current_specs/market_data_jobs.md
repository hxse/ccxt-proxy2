# 后台行情采集与清理

后台通过正式 HTTP 路由登录、查询和触发缓存，和用户请求复用同一套校验、缓存及连续性机制。脚本不调用 SDK、路由函数或数据库，不维护第二份采集状态。采集与清理分开维护，由应用中的一个调度器顺序执行。

## 配置入口与作用域

公开计划固定为项目根目录 [market_data.toml](../../market_data.toml)，与 cwd 无关，可纳入版本控制；所有参数保留中文 # 注释。私有客户端沿用 [config.example.toml](../../config.example.toml) 的 market_data_client 分组和既有配置文件选择机制。

```toml
# 以下放在私有 config.toml；引用已有 HTTP 用户，不是 TQ 用户。
[market_data_client]
# 本地默认监听端口；容器内部通常改为 8000。
base_url = "http://127.0.0.1:5123"
# 使用已有 users.admin.password 登录，不另存密码。
user = "admin"
# 单次 HTTP 等待上限，不修改路由或 SDK 的超时。
request_timeout_seconds = 60
```

配置在启动时读取，修改后重启。公开文件缺失、未知字段、错误类型和非法组合直接失败；业务请求前完成配置校验。除 symbols 和私有 user 外，省略字段采用公开示例默认值；字典和周期列表整体替换，不合并示例清单。存在实际 HTTP 工作才要求 user 引用已有账号；不隐式选择首个账号。

计划参数只决定后台发什么请求，不限制用户请求、不成为路由默认值。启用采集时 save_mapping 要求 save_main，mapping_timeframes 非空且属于 timeframes；save_transition 要求 save_mapping，transition_timeframes 非空且属于 mapping_timeframes。关闭整个采集计划后不检查其运行依赖，但仍检查未知字段、类型、重复值与非法周期。

周期取 5m/1h/1d/1w 且不可重复；data_length 为 1..10000；开启品种采集需要非空、值不重复的完整 KQ.m@ symbol。布尔值严格校验，不把 bool 当数量。间隔及年限必须为正整数；保留数量为非负整数，0 有效。

base_url 必须为绝对 HTTP(S) 地址且不含凭据、query、fragment；timeout 为有限正数。后台地址不决定服务器监听端口。

## 默认一轮采集

28 个主连依配置顺序，每个请求四个周期的主连与对应加权；加权只替换 KQ.m@ 为 KQ.i@，保留交易所与品种大小写。每次直接请求最新 10000 根、enable_cache=true，不试探扩大、不以返回数量达标决定重试、不请求概况指导补拉。

主连成功且非空时，仅 mapping_timeframes 中的周期触发映射。默认只有 5m；直接用响应首尾 datetime 纳秒作为 start_time/end_time。符合 transition_timeframes 且 save_transition 开启时附 transition_timeframe；过渡数量沿用路由默认 10，脚本不配置镜像参数、不处理 N＋1。

1h/1d/1w 默认只采集行情。5m 失败或为空不改用其他周期补映射。关闭过渡仍保留已配置的映射查询。短历史和空行情合法；主连失败不阻止独立加权，映射失败不撤销主连已写入的缓存，其他品种继续。

save_calendar 开启时每轮单独采集一次日历：调用 /system/fetch_time，转北京时间日期 D，以日历年回退十年得到 L，再请求 [L,D]；闰日回退到目标年二月最后一天。在线取时失败只跳过日历并记录失败，普通行情可继续。

脚本的时间仅用于构造查询范围。映射、日历及辅助清理路由仍各取自己的在线时间，在单个请求内部固定使用，不复用整轮旧边界，不回退本机时间。

停机恢复没有特殊算法：一万根窗口与旧缓存相连则由缓存合并，否则保留新片段继续积累。用户请求也可随时触发同一机制。采集计划不包含 CCXT。

## 清理与失败

清理向 POST /cache/prune 显式发送 providers、modes、auxiliary；默认 TQ/CCXT，live=30000、sandbox=0，辅助数据十年。providers 不含 TQ 时 auxiliary=null。详细删除和片段维护规则见[缓存维护](cache_maintenance.md)。

采集后执行清理，采集局部失败仍执行可用清理。HTTP 200 也检查 status/errors；partial 计为失败。409 是维护忙，本轮合法跳过且不重试。关闭自动清理不禁止人工调用；两次清理间缓存可超过计划保留数。

客户端按 expires_in 和单调时间管理内存令牌，临近过期重新登录；收到 401 只允许重登并重发一次。网络错误、超时、504/5xx 均不自动重试，尤其清理 POST 超时不代表服务器未执行，不能重发。

日志记录请求身份、返回数量、错误码及清理摘要，隐藏密码、令牌、认证头及原始异常正文。单轮所有适用项成功或合法跳过退出 0，有失败退出 1；不保存另一份任务历史数据库。

## 调度、启动和关闭

应用仅在存在实际工作时启动调度。TQ 采集要求计划启用且 TQ 在服务白名单；清理独立。默认 interval_seconds=3600，以单调时间安排触发；每轮顺序执行，错过的时点跳过，不积压、不并行。

lifespan 启动不等待自己的 HTTP；注册任务后正常完成 startup。任务在最多 60 秒内等待 /readyz，通过后登录执行。等待失败记录本轮失败，下一正常间隔再尝试，不阻塞服务启动。

退出先停止后台新工作、取消 HTTP 等待并关闭客户端，再关闭元数据、SDK 和共享数据库。路由自身的业务门禁负责等待仍在执行的底层操作；取消客户端不等于服务器或 SDK 已停止。reload 不遗留旧调度器，部署保持单进程。

## 手动单轮入口

```bash
just market-data-collect
just market-data-prune
just market-data-once
```

分别对应 scripts/collect_market_data.py、scripts/prune_market_data.py、scripts/market_data_pipeline.py。全部读取相同配置并尊重 enabled；后两者会删除超出规则的本地缓存，不能加入只读在线测试。

两个后台开关都关闭且没有工作时不创建 HTTP 客户端。用户直接调用路由不受这些开关影响。容器只读挂载公开计划与私有配置；按容器实际监听设置 base_url，例如 http://127.0.0.1:8000。

默认测试在应用导入前选择明确关闭调度的 fixture；在线测试也关闭生产计划并使用隔离库。配置、真实请求参数、一次重登、超时不重发、单调调度和关闭顺序以离线 HTTP 替身验证。
