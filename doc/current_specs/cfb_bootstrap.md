# 按模式隔离的 CFB 容器运行与诊断

项目遵循 [项目原则](cfb_project_principles.md)。CFB 执行器只在独立 Podman 容器内运行；主服务可在宿主或容器运行。唯一 HTTP 服务属于 ccxt-proxy2，CFB 通过有界 Unix socket JSON 通信。协议和编排见 [集成边界](cfb_integration.md)。

## 终端与镜像

`src/cfb/terminal.lock.toml` 锁定普通快期2 2.93.405.1998 的官方包及 SHA256，profiles 目录声明上期技术/9999 和 H华安期货/6020。构建分别生成只含目标券商的 simnow/huaan 种子，不修改原生站点和认证参数。Wine 使用 WineHQ 11.0 的 32 位预编译包，`wine-stable:i386` 与 `wine-stable-i386:i386` 均锁定 `11.0.0.0~bookworm-1`，不从源码编译。启动直接运行 q7_release.exe，先验证种子的环境、券商和全部程序文件哈希。

单一执行镜像携带远程桌面组件，由 cfb.vnc_enabled 决定是否启动；关闭后不发布 VNC 端口。始终保留 Xvfb/Openbox、截图和中文字体，1280×800/96 DPI 为默认设置。Xvfb 关闭 GLX，保留二维 X11。先确认显示与窗口管理器，再引导 Wine 会话、启动客户端；持久化前缀不跳过 Wine 会话引导。

Wine 保留官方包及其必要依赖，仅禁用 Mono（mscoree），启用内置 MSHTML。terminal.lock.toml 的 wine_gecko 锁定官方 Gecko 2.47.4/x86 下载地址与 SHA256；构建时校验后提取到 /usr/share/wine/gecko/wine-gecko-2.47.4-x86，运行镜像内置该资源，不在启动时下载。每次 wineboot 后、启动终端前执行 wine regsvr32 /s mshtml.dll，已有 .bridge-initialized 前缀也注册，修复历史 HTML MIME/about 协议缺失；失败返回 HTML_SETUP_FAILED 并阻断启动。正常容器启动和终端重连共用此流程，旧卷无需删除或手工迁移。

优化优先考虑运行内存和构建耗时，不为缩小镜像引入完整 Wine 源码编译，也不强行删除 dpkg 依赖或任意 Wine 服务。Gecko 提取工具、原生桥接和轻量截图工具的编译依赖留在独立构建阶段，运行镜像只携带产物。

诊断和失败证据统一使用 `cfb-capture`，读取 X11 根窗口并通过 libpng 保存 RGB PNG，保留超时和原有容量治理；不安装 scrot。Openbox 仍依赖 Imlib2，因此其共用图像库继续保留。中文字体仍使用完整文泉驿微米黑及原有映射，不裁剪字库。

宿主 data/cfb/sandbox、data/cfb/live 分别挂载为对应容器的 `/data`，共享 logs、artifacts、state；terminal/wine 位于 sessions 下按环境、券商、站点和账户摘要隔离。摘要目录不显示凭证原文，旧 /data/terminal 和 /data/wine 保留但不自动迁移。`.session.lock` 排他锁保护整个实例；停止只处理本容器进程，保留数据卷。已识别连接故障按受控流程定时恢复；进程崩溃、桌面启动失败及未知异常不盲目循环重启。

客户端已有独立探针能力见 [terminal_capabilities.md](cfb_terminal_capabilities.md) 和 [trading_status.md](cfb_trading_status.md)，历史探针数据不等于正式 API 全链路已经在线核验。

## 配置与入口

用户在 ccxt-proxy2 的 config.toml 配置，示例为 config.example.toml。CFB 设置统一位于 cfb 子表：bridge、accounts.sandbox、accounts.live、desktop、vnc、execution、reconnect、logging、artifacts。下文未带前缀的字段均相对于 cfb；不接受原 api 子表或 HTTP base_url。每个 cfb 白名单项必须指定 is_live 严格布尔值，true 对应 live、false 对应 sandbox；同值不得重复。本地默认同时启用两组，remote 覆盖默认只启用 live。守护进程由编排传入固定 --is-live=true|false，每个容器只登录对应账户；cfb.bridge.mode 退出。

Pydantic 对所有分组严格校验字段名、类型和字段基本格式，只对选中组检查凭证配对及环境、券商、站点组合。选中组账号密码均空可启动登录界面，不安排重连；仅填写一项时报错。备用组可为空或待补齐，选中时再做业务完整性校验。sandbox 的空 broker_id/site 仍解析为 9999/电信2；live 必须明确填写 6020 和“一套”或“二套”。错误不回显凭证，日志过滤覆盖两组账号密码。

Settings.account 为派生的当前账号，不能作为配置字段输入；内部 bridge.environment 从 mode 映射为 simnow/live，用于原生 profile、session 目录和幂等 namespace。此重构不改变原数据身份或创建替代目录，业务和重连共用同一个当前账号。

```toml
# 服务白名单；is_live 必填，true=实盘，false=模拟盘；TQ 不带此字段。
service_whitelist = [
    { service = "cfb", is_live = false },
    { service = "cfb", is_live = true },
]

[cfb.accounts.sandbox]
broker_id = "9999"
site = "电信2"
username = ""
password = ""

[cfb.accounts.live]
broker_id = "6020"
site = "一套"
username = ""
password = ""
```

reconnect.enabled 默认 true；interval_seconds 为严格整数 60～86400，默认 600，计时从本次失败结束开始。可省略整个配置节；设为 false 关闭自动重连，900 表示每 15 分钟。单次登录沿用 bridge.startup_timeout_seconds。已识别连接故障由同一调度器恢复：确认旧 worker 和专属 Wine 退出，再引导新终端会话；HTTP、Xvfb/Openbox、VNC 不重启，不重建镜像。具体限制见 [terminal_execution.md](cfb_terminal_execution.md)。

公共 execution.csv_confirmation_attempts 默认 3，严格整数 2～10；csv_confirmation_interval_ms 默认 100，严格整数 10～1000。用于交易后的有界 CSV 观察及三个 CSV 查询的一致性核对；不改变 GUI 按键间隔或排队期限。

execution.price_max_deviation_ratio 默认 0.05，有限数且 0 ≤ 值 < 1。控制显式限价买价超涨停、卖价低于跌停时允许自动截断的幅度；相反方向越界拒绝。0 关闭越界截断，仍修正极小浮点尾差并按买下卖上对齐 tick。完整规则及返回字段见 [HTTP 接口](cfb_api.md)。字段可省略；修改后重启生效。

每种模式的实例固定启动配置和所选账户，不通过请求或重连切换。受管容器摘要包含所选配置、账号、密码及启用的代理地址，排除备用账户；只输出摘要，不输出凭证。修改配置后再执行正式启动入口，匹配实例复用、不匹配则有序替换；其他模式的账户或其他 SDK 配置变化不重启当前实例。执行请求另核对配置身份，不能在替换失败后使用错误账户。

原 CFB 私有配置复制到本项目的 cfb 表；不提供原项目的 migrate-config、run 或 restart 入口。原项目和数据保持独立；历史会话、Journal 首次导入须在原写入者停止后完整复制到空的 data/cfb，不覆盖既有数据，也不让两个实例共享同一目录。缺少原 Journal 会失去原请求的幂等防重发记录，不能把新数据库当作原账户的延续记录。

HTTP 使用主项目 Bearer 鉴权。凭证、备份、debug 和数据不进入版本控制或镜像；原 TOML 只读挂载，通过统一加载器应用 dev/local/remote。CFB 代理默认关闭，remote 覆盖开启；原生 TCP 的严格 HTTP CONNECT 代理见集成规范。

VNC/noVNC 的模拟盘默认端口为 45174/45175，实盘为 45176/45177，分别由 cfb.vnc.sandbox.port/web_port 和 cfb.vnc.live.port/web_port 调整，启用模式之间不能重复，开启时仅发布到宿主 127.0.0.1。CFB 不再监听 45173。端口冲突明确失败，不换随机端口、不停止原项目实例。

```bash
just serve
just deploy --target=local --build --start
just cfb --is-live=false --status
just cfb --is-live=false --pause
just cfb --is-live=false --resume
just cfb --is-live=false --screenshot --output=debug/cfb-desktop.png
just cfb --is-live=false --clean
just cfb --is-live=true --target=remote --logs
```

白名单启用 cfb 时，serve 自动准备并启动执行容器；部署 --build 只构建两个配套镜像，--start 自动启动/复用 CFB 后启动主服务。执行器不等待登录才开放诊断；启动成功不表示 trading_ready。显式 just deploy --target=local|remote --stop 停止主服务及本项目两个模式的执行器，主 API 正常退出不销毁独立 CFB。构建失败保留原版本，CFB 某模式启动失败只影响该模式，仍继续启动另一模式。

运维 CLI 必须显式指定 --is-live=true|false，通过对应 Unix socket，不提供交易动作；pause 等待当前操作退出后才允许人工接管，resume 检查账户与界面。clean 只触发受管执行器内的文件治理，不操作其他项目和停止后的原卷。

旧单实例 data/cfb 的模式拆分由正式启动入口执行：先停止本项目旧单实例，锁定数据，再按 simnow-/live- 分配 sessions；state 和必要 artifacts 各保留完整副本，既有 Journal namespace 不变。完整旧目录保留为 data/cfb-legacy，不自动删除。新模式目录已有内容或迁移失败时拒绝覆盖，不使用空库代替；原 cn-futures-bridge 数据不受影响。

## HTTP 诊断

| 路径 | 含义 |
| --- | --- |
| GET /cfb/healthz | 独立执行进程存活，不代表终端就绪 |
| GET /cfb/status | 当前 Pydantic 状态对象 |
| GET /cfb/readyz | 主服务与执行器配置身份匹配且 trading_ready 为真返回 200，否则 503 |
| GET /cfb/desktop/screenshot | 虚拟屏幕 PNG；不可截图时明确报错 |
| GET /docs、/redoc、/openapi.json | Swagger、ReDoc 文档与接口模型 |

上述四条 /cfb 诊断入口均要求 is_live=true|false，不允许省略或使用旧 mode。响应带 X-Request-ID 和 no-store，不记录请求头或凭证。桌面尚未接入执行器时 stage 为 terminal_bootstrap；接入后为 terminal_execution，状态区分登录、连接、队列、暂停和 blocked。trading_ready 要求真实身份、连接、交易日和执行权就绪；窗口可见不证明交易就绪。内部执行链路及能力边界见 [terminal_execution.md](cfb_terminal_execution.md)，八个业务路由已由 [api.md](cfb_api.md) 定义并接入同一 FastAPI 服务。

状态对象的 environment/request_mode/broker_id/site 描述当前启动绑定；不显示账号密码，也不通过状态查询重新登录。实盘具有与适配范围一致的交易能力，不设强制只读模式。

/cfb/readyz 通过内部 ready 请求复用业务配置身份门禁；更新失败而保留旧实例时返回 503 SERVICE_NOT_READY，不能依据旧实例的 trading_ready 误报就绪。/cfb/status 和 /cfb/healthz 仍可诊断该执行器；它们的成功响应不证明主服务当前配置已生效。

reconnect 对象包含 enabled、interval_seconds、state、attempts、last_failure_at、last_error、next_retry_at、manual_required。state 为 idle/waiting/reconnecting/paused/manual/disabled；无账户时禁用。attempts 是本实例的自动恢复尝试次数，不含首次启动；最近失败与次数在恢复成功后保留。时间为 UTC ISO8601 或 null，定时执行使用单调时钟。暂停/停止时 next_retry_at 为 null，不代表清除原计时；恢复时期限已过可立即尝试。断线期间 healthz 仍 200，readyz 为 503，status 仍可读。

## 容量与生命周期

Python、Wine、桌面和辅助进程输出由有界收集器写为 JSON 行日志，不把子进程 stdout/stderr 绑定到无限追加文件。单文件默认 20 MiB、日志合计 200 MiB；Podman k8s-file 日志另外限制 20 MiB。

just cfb --is-live=true|false --target=local|remote --logs 在所选运行容器内读取 /data/logs/cfb-*.jsonl，先展示最近 100 条完整记录，再持续跟随所有 writer 的追加和新文件；轮转、容量删除及半行写入不会导致重复或截断输出。只读已由 LogStore 脱敏的受管 JSON，不读取终端原始文件、不改日志治理规则；没有文件时继续等待，Ctrl+C 结束。podman logs 只提供容器标准输出，不能代替这个日志入口。

`logging.max_total_bytes` 和 `artifacts.max_total_bytes` 分别控制日志及工件的总容量，300 MiB 为 314572800 字节。桥接日志在写入前检查容量，工件在创建/收尾及导出前后检查容量；后台按 `artifacts.cleanup_interval_seconds`（默认 60 秒）检查。需要腾出空间时按最旧顺序删除可清理文件，不依赖保存天数。

终端 logs 目录下已识别的时间戳诊断文件及旧启动进程日志纳入预算。活动文件通过句柄检查保护，不强制截断；无法安全治理的活动写入超限时，服务停止终端并记录不可用状态。不能仅凭后缀递归删除 Wine、账户或 flow 文件。

日志、工件或 Journal 维护失败时，停止新派发及自动恢复，主动回收本实例的 Wine、桌面和日志写入进程，再释放 GUI worker；与显式关闭共用停止流程并串行执行。监控线程已经因 WINDOW_TIMEOUT 等错误退出时也必须实际回收，不能只设置停止标记。普通启动失败保留桌面供诊断，但后续存储故障仍须停止写入。

容量故障后的诊断接口继续可用，状态为 STORAGE_UNAVAILABLE、交易未就绪，终端停止后窗口可见状态为 false；数据目录排他锁保留至实例正式退出，不能因停止写入进程而让第二个实例取得同一份数据。未决提交记录及保护工件保留，不自动重启终端。

每条桥接日志在同一跨进程锁内核算实际文件大小，包含其他 writer 的写入、轮转与终端文件增长；正常预算只枚举受管路径和大小，不扫描进程句柄。确需删除时才检查活动句柄并重新计量，淘汰最旧可删除文件；周期维护仍检查活动终端文件及单文件上限。无法腾出预算或写入失败仍标记日志存储不可用，不通过丢日志或缓存旧容量换取速度。

artifacts 中只管理明确登记的 op 工件目录，记录 active/ended/protected 状态。成功解析后释放临时 CSV；失败资料保留至容量需要淘汰，默认共享 300 MiB，无年龄清理。清理保护活动和未知提交证据，低磁盘或无法满足预算返回 STORAGE_UNAVAILABLE。

## 验证入口

just test 在宿主 uv 环境执行默认离线测试，不连接真实账户。just lint/just check 使用主项目正式入口。just test-cfb-native 显式构建独立验证镜像，运行阶段 --network=none，不挂载账户或生产数据；验证原生控件、结算识别、PNG 及真实 Wine TCP 代理/直连分支，并同时运行两个断网空账户容器，核对模式、socket、桌面和停止隔离。验证镜像和真实执行器是不同入口，不把测试数据放入生产会话。

完整交易和状态变化的在线验证仍有 [已知边界](cfb_terminal_capabilities.md)，不能由离线测试、容器存活或类型检查推导。

容量治理离线回归使用临时目录及真实 Python 写入进程，覆盖监控线程存活/已退出、三类维护故障、实例隔离、证据保留、实例锁和并发关闭；不依赖真实 Wine 或交易账号。
