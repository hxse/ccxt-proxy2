# 后台采集与清理调度

## 任务边界

共同约束见[主任务](<../20260926A market data caching and maintenance/spec.md>)；调用契约使用[TQ 行情](<../20260926A.003 tq ohlcv cache integration/spec.md>)、[映射](<../20260926A.004 tq calendar and mapping/spec.md>)、[过渡](<../20260926A.005 tq rollover transition windows/spec.md>)及[维护路由](<../20260926A.006 cache inspection and retention/spec.md>)。

完整用户配置、默认值、逐参数注释和校验冻结于 [spec_01_configuration.md](spec_01_configuration.md)。本任务交付两个单轮脚本、同一小时流程、应用托管、HTTP 客户端与必要用户/部署说明。

不创建 CCXT 主动采集清单，不调用 SDK、业务路由函数或 DuckDB，不维护第二份数据、连续性证明、采集达标状态或恢复水位。停止线是配置可独立使用、任务按序调用、启停和失败明确，用户路由不受计划限制。

## 任务规范

### 一个业务机制，两种请求来源

用户与后台请求进入同一 HTTP 鉴权、请求校验、白名单、业务获取、缓存和响应流程。用户只处理其指定的序列，后台配置不成为用户白名单或路由默认参数。

后台显式保持 enable_cache=true；不发送 refresh_source，不实现小时 TTL、时段判断或源末日/标的比较。元数据自动检查由路由完成。

### HTTP 客户端

读取既有私有配置选择规则，从 market_data_client.user 指向的 users 项取得密码，POST /auth/token，使用 Bearer JWT。用户名不是 TQ 账号，不复制密码，不隐式选第一个账号。令牌只在进程内存保存，所有日志隐藏密码、token 和认证头。

按 expires_in 与单调计时管理客户端令牌；过期前重新登录。明确 401 鉴权拒绝可登录一次后重发一次；其他网络错误、504/5xx 不自动重试。尤其清理 POST 超时不代表服务端未执行，不能自动重发它。

使用配置的每次 HTTP 等待上限，默认 60 秒。该值不改变服务器/SDK deadline，也不代表客户端超时已经取消底层工作。客户端由单轮/调度所有者正确关闭。

### 单轮采集

按配置顺序逐品种、逐周期执行，不并发灌入 TQ 单工作线程。主连和加权各是一条请求；对应加权仅将完整 KQ.m@ 前缀改为 KQ.i@，交易所和品种大小写保持。

```text
对每个 symbol/timeframe
→ save_main：请求主连，data_length 使用固定配置值
→ 主连成功非空、save_mapping 且周期在 mapping_timeframes 中：从实际首尾 datetime 取得历史范围
   → 请求同一 symbol 的映射
   → save_transition 且周期在 transition_timeframes 中：附带该周期
→ save_weighted：请求对应加权
```

5m/1h/1d/1w 转换为 300/3600/86400/604800 秒，仅用于请求参数。映射 start_time/end_time 直接取主连真实纳秒时间，交易日转换留给路由。

默认 mapping_timeframes=["5m"]、transition_timeframes=["5m"]。1h/1d/1w 只请求主连和加权，不请求映射。满计划且所有 5m 主连响应非空时，一轮为 28 次映射请求；5m 失败/为空不改用其他周期补发映射。

映射本身不分行情周期，mapping_timeframes 只选择哪些响应范围触发请求；transition_timeframes 决定其中哪些请求附带过渡周期。关闭过渡不取消已配置映射触发；其他用户仍可独立请求任意合法历史范围。

每次直接请求配置数量，默认一万；不检查必须达到 9999/10000，不调用概况指导补拉，不按 interval 估算缺口，不发第二个扩大窗口。正常短历史照常成功。

主连失败或为空只跳过派生映射，独立加权和其他品种继续。映射失败不丢主连已缓存结果；过渡沿用路由默认 N=10，不增加脚本镜像参数，也不处理 N＋1 或旧合约筛选。

save_calendar 开启时每轮请求一份日历，不按品种/周期重复：脚本调用 /system/fetch_time，转 Asia/Shanghai 日期 D，按日历年回退 calendar_lookback_years 得 L，请求 [L,D]。闰日回退规则与维护相同。

脚本这次取时仅构造日历范围，失败时记录并跳过该次日历；每个映射/日历业务请求仍由服务端取得并固定自身可信时间。脚本不把先前客户端日期传成可信 now，采集与清理不共享整轮日期。一个请求内的日历依赖、转换、刷新和复核共用已取得时间，不再重复取时。各元数据请求自己的取时失败正常记录，普通行情可继续。

按全部默认步骤成功且不重试计算，一轮公共取时为 28 次映射、1 次日历业务、1 次脚本构造日历范围、1 次辅助清理，共 31 次；关闭/跳过步骤会减少，数量只说明请求计划，不作为采集达标门槛。

### 单轮清理

将 retention.providers、modes 和 auxiliary 转成 /cache/prune 的显式 body。providers 不含 tq 时 auxiliary=null，不发送无效年限组合。路由不读取 market_data.toml 补默认值。

清理请求保持一次，409 表示已有维护，记录本轮 skipped/busy；不排队重试。HTTP 200 仍需检查 completed/partial 和 errors；取时失败导致辅助部分 skipped 时不能记录全轮成功。普通按根数清理由路由决定继续。

后台关闭清理不禁止人工路由；sandbox=0 是明确有效的周期清零，不改变请求时是否写缓存。采集失败后仍执行可用的清理步骤。

### 小时流程与生命周期

只有一个调度器管理间隔，采集和清理模块不各启一个定时器。默认 interval_seconds=3600；首轮在 HTTP 已可用后执行，以单调计时安排后续触发。上一轮未结束则跳过该次触发，不积压、不并行重入。

TQ 采集同时要求计划 enabled、服务白名单启用并就绪；清理独立于 TQ 开关。两者均无实际工作时不启动小时任务。

应用 lifespan 只注册/启动非阻塞托管任务，不能在返回 startup 完成前 await 自己的 HTTP。托管任务在有限启动等待内确认 /readyz 可访问后登录并运行；60 秒仍不可访问则记录本轮失败，后续正常周期再尝试，不无限阻塞服务启动。

退出先禁止新轮次和新 HTTP 工作，取消尚未开始项目，等待/取消客户端在途请求并关闭客户端，然后由服务生命周期等待在途业务并关闭 SDK/数据库。取消 HTTP 等待不等于底层任务已停止，必须与已有操作门禁及退出机制配合。

reload 新进程接管时不能遗留上一进程的调度器。保持单 Uvicorn process 部署，不另开数据库写进程，不以分布式锁掩盖重复服务实例。

### 记录和失败

记录当前请求身份、返回数量、失败原因和维护摘要；不为日志强制请求概况，不另建任务历史数据库。每轮工作集合有限、请求等待有限，停止后不继续生成请求。

一轮所有适用项成功或合法跳过为成功；请求失败/维护 partial 为失败摘要但继续独立项。单轮 CLI 全成功退出 0，有失败退出 1；配置错误在业务请求前退出非零。关闭开关导致无工作只记录 skipped 并退出 0。

停机恢复不设置额外模式：最新一万根能与旧缓存重叠时自然合并，否则保留新片段继续积累。用户随时请求也走同样机制，不由脚本判断是否接上。

## 公开接口与用户写法

计划实现文件与单轮入口：

```text
scripts/collect_market_data.py     对应 just market-data-collect
scripts/prune_market_data.py       对应 just market-data-prune
scripts/market_data_pipeline.py    对应 just market-data-once
```

三个 CLI 都读取同一公开计划及既有私有配置选择规则，执行单轮后退出；同一内部单轮函数供应用调度复用，始终通过 HTTP。手动脚本尊重各自 enabled；用户手动路由不受其限制。

```bash
just market-data-collect
just market-data-prune
just market-data-once
```

清理入口必须在说明中标明会删除本地缓存；不能加入 readonly Bruno/online 聚合。应用默认自动运行无需用户另起脚本，关闭通过已冻结的两个 enabled 字段。

本地默认 base_url 为 http://127.0.0.1:5123。容器内部若监听 8000，私有客户端应使用 http://127.0.0.1:8000；改变后台地址不改变服务监听。README/部署示例说明公开计划文件的挂载和重启生效，不自动推断用户自定义端口。

实现时提供根 market_data.toml 与 config.example.toml 新分组示例，不编辑真实 config.toml。所有新参数保留 # 注释，公开文件可纳入 Git，私有配置与数据继续忽略。

## 测试、验证与阶段过渡

实现时通过 just test、just lint、just check；文档阶段不运行。离线用 HTTP mock/本地 ASGI 证明脚本只访问正式路由：固定窗口、真实首尾传参、保留大小写、合法短响应、失败继续、日历不重复和清理 body 正确。

完整注释配置示例必须由正式加载器读取；验证默认、省略、整体替换清单、未知字段、错误类型、重复合约、依赖、账号引用、关闭计划和环境保留零值。mapping_timeframes 属于 timeframes，transition_timeframes 属于 mapping_timeframes；默认只从 5m 触发映射，1h/1d/1w 不调用映射，5m 空/失败不从其他周期补发。后台关闭或换清单不影响用户路由的合法调用。

验证同一元数据请求只取一次服务端时间并传给内部依赖，不将一轮开始时间复用于后续 HTTP 请求；跨日时各请求使用自己的边界。关闭 save_transition 后仍执行配置的 5m 映射；合法关闭 save_mapping/save_transition 的组合不触发映射或过渡，违反依赖的组合仍在启动时拒绝。

验证 token 过期/401 的一次重登、清理超时不重复发送、日志脱敏、仅 CCXT 时清理独立工作、HTTP 就绪后首轮、无自调用启动死锁、单调调度不重入、取消/reload 和关闭顺序。

默认离线 fixture 必须显式关闭自动调度；在线 fixture 使用隔离缓存并关闭生产计划。不得因新增默认启用脚本让已有测试开始联网或删除数据。

配置模型、样例、脚本、托管生命周期与测试在同阶段交付；不先启用一个依赖尚不存在路由的后台。同步 configuration、architecture、相关行情/维护说明、verification、README 和容器挂载说明；在线执行仍只在最终阶段一次。
