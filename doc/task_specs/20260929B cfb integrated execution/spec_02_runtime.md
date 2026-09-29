# 配置、代理与容器生命周期

## 配置归属

统一 config.toml 是唯一用户配置文件；CfbConfig 保存公共账户、终端、执行、重连和文件治理设置及 enable_proxy、vnc_enabled、request_timeout_seconds；每个已启用 mode 生成固定的运行 Settings。原 HTTP api 子表、base_url 和用户配置 cfb.bridge.mode 删除，不保留兼容回退。账号环境、站点、指纹及 journal 命名空间保持不变。

CFB 守护进程复用本项目配置加载器，按显式场景读取 cfb 与公共 proxy，不启动其他 SDK。原文件只读挂载，不上传后打补丁、不写入镜像。白名单中的 cfb 项必须显式填写严格布尔值 is_live，true 对应 live、false 对应 sandbox；内部身份仍为 cfb/<mode>，重复环境直接拒绝，最多两个。白名单 mode/trading_env 旧输入不接受。公共与 overrides.remote 的 service_whitelist 都使用内联数组，后者完整替换前者。本地默认启用两种模式，远端只启用 live；每个模式使用 cfb.accounts.<mode>，未启用的模式不要求凭据业务完整。账号均空沿用只显示登录界面的行为。

## 镜像与实例

主镜像保持现有 Python/CTP 依赖；CFB 单独按原锁定终端、Wine 11、Gecko、原生源码装配，固定版本校验不削弱。CFB 构建仅限当前支持的 linux/amd64，不让这一限制扩散到未启用 CFB 的主镜像。

受管实例名 ccxt-proxy2-cfb-sandbox 和 ccxt-proxy2-cfb-live，共用一份镜像；原 cn-futures-bridge 始终不受管理。宿主 data/cfb/<mode> 分别挂到各容器的 /data，主服务访问同目录下的通信 socket；不将 journal 合并进 DuckDB，不让行情清理触碰它。CFB 私有数据首次导入独立于代码复制，不覆盖已有目标目录，不改原数据。

本地和远端编排仍由宿主 Shell/Podman 负责，运行容器不获得 Podman socket。build 接受 --config 并按白名单准备配套 CFB 镜像，记录主镜像 ID 与执行镜像 ID 的配对，upload 包含对应构建源码及锁文件，start 逐个确保已启用模式的 CFB 实例已启动，一个模式失败仍继续启动另一个。部署 start 停止已从白名单移除的本项目模式，不删除其数据。serve 若白名单完全没有 CFB 则不调用 Podman，已有执行器由显式 deploy --stop 管理。serve 也按 dev 场景执行同一准备/启动逻辑；初始化/登录异步，失败记明原因而不阻断主应用及其他 SDK。

源码清单按完整相对路径的 ASCII 字典序生成，与远端校验一致；src/cfb/terminal.lock.toml 必须排在 src/cfb/terminal/ 下的模块之前。用真实上传 Shell 的离线测试验证该组合的首次上传及随后零差异上传，避免目录分段排序导致正式部署被拒绝。

相同镜像、所选 CFB 配置身份且已加入 trading-net 时复用；仅其他 SDK 配置改变不能重启 CFB。CFB 变化时先确认旧 owner 退出再替换，构建失败保留旧实例，不操作原项目容器/卷。不因主 API 重启而重启已匹配 CFB；显式项目 stop 停止主服务及本项目两个模式的执行器。主进程关闭只释放通信资源，独立 CFB 由容器生命周期维护。

VNC/noVNC 可配置开启，宿主仅发布 127.0.0.1，cfb.vnc.sandbox.port/web_port 默认 45174/45175，cfb.vnc.live.port/web_port 默认 45176/45177；同次启用的端口不得重复；不再发布 45173 HTTP。原项目占用端口时明确记录冲突，不停止原实例。

生产启动统一使用 trading-net。在目标机停止或替换旧实例之前，执行 podman network create --ignore trading-net；首次创建，已有同名网络直接复用，不改其参数、不删除重建，也不随项目 stop/清理删除。网络准备失败时停止本次部署，保留旧实例。主服务及所有启用模式的 CFB 创建命令均带 --network=trading-net；本地、远端部署和 serve 的 CFB 启动复用同一实现。构建、预检和数据迁移的隔离容器继续使用 --network=none，不因仅构建而创建运行网络。

实例复用要求同时满足原镜像/配置/场景条件及已加入 trading-net。旧实例缺少该网络时，沿现有停止、替换和失败恢复流程迁移，保留数据及其他匹配实例；不通过临时 network connect 维护第二条更新链。宿主发布仍为 127.0.0.1，CFB 通信仍使用 Unix socket。离线真实 Shell 测试覆盖首次创建、已有网络配置保留、重复启动、网络准备失败不停止旧实例、主服务和各模式迁移，以及 serve 入口；通过 just test、just lint、just check。

## TCP 代理

cfb.enable_proxy 默认 false；overrides.remote.cfb.enable_proxy=true。读取公共 proxy.effective_http，支持 http:// 的 HTTP CONNECT 代理，关闭不继承 HTTP_PROXY 等环境代理。CFB 不依据 binance.enable_proxy 决定自身代理。

通过执行镜像内的 proxychains-ng 32/64 位动态库对 Wine/控制器 TCP 连接使用单一严格代理链。配置在私有运行目录生成，代理失效不回退直连；回环的 X11/本机控制通信保留本地连接。代理地址、账号和密码不打印。对无法表达的地址/认证内容明确拒绝，不静默丢字段。

只支持 TCP；没有为 UDP 或其他传输虚构代理支持。运行前验证库存在及位数，验证场景使用本地 HTTP CONNECT 替身观察真实连接，不以设置环境变量作为生效证明。

清理按配套关系保护准备版本、在用实例、构建依赖和其他用途的镜像，移除无用生产版本及失效配套记录，不全局 prune。重启/中断恢复必须先结束新 owner，再恢复旧实例，Journal 与数据目录始终保留。

just cfb --is-live=true|false --target=local|remote --logs 先核对实例归属，再在所选运行容器内读取 /data/logs/cfb-*.jsonl。默认展示最近 100 条受管 JSON 记录并持续跟随全部 writer，支持新增、轮转和预算删除；文件还未出现时等待，未完成的末行等待写完，不重复输出旧行。不把 podman logs 或终端原始文件当作桥接日志，不复制日志到第二份存储；凭据脱敏继续由原 LogStore 负责。离线测试使用真实 LogStore 验证双 writer、轮转、追加和脱敏结果，CLI 替身验证本地/远端实例选择及错实例拒绝。

日志、工件或 Journal 的容量治理失败时，先禁止新派发及自动恢复，再通过既有 Runtime 停止流程实际回收本实例的 Wine、桌面及日志写入进程，确认桌面停止后才释放 GUI worker。不能只设置停止标记或依赖监控线程的 finally：线程正在运行和此前因 WINDOW_TIMEOUT 等错误退出均须覆盖。故障收尾与显式停止串行，避免重复回收同一执行器。

容量故障后保留诊断服务及数据目录排他锁，直到实例正式退出；状态为 STORAGE_UNAVAILABLE、交易未就绪，不自动重启，也不删除未决提交证据换取容量。普通启动失败继续保留桌面供诊断，但不能豁免后续容量故障的实际回收。原日志、工件和 Journal 的预算及淘汰规则不变。

离线回归使用临时目录和真实、有独立进程组的 Python 日志写入者，验证监控线程存活/已退出时超限均停止写入、正常容量不停止、其他实例不受影响、未决记录和保护工件保留、故障期间不能取得第二把实例锁，以及容量故障与显式停止并发时正确收尾。覆盖日志、工件及 Journal 三类维护失败；不连接真实终端或交易服务。使用 just test-file 执行针对性回归，并通过主规范要求的 just test、just lint、just check。

## 单实例数据过渡

正式启动入口先停止本项目旧 ccxt-proxy2-cfb（及其替换备份），再取得旧数据根的排他锁，迁移完成后退出旧容器；不停止原 cn-futures-bridge。阻断旧实例与新模式并行，最多两个活动 CFB owner。

旧 data/cfb 的 sessions 按 simnow-/live- 前缀复制到对应模式；原目录结构在各自 /data 内保持不变。防重发 state 与必要 artifacts 分别完整复制到两个模式，继续按既有 namespace 查询，保留未知提交和历史账户记录，不重新生成业务身份。日志及完整旧数据保留在 data/cfb-legacy，不自动删除。

迁移先生成完整暂存目录，再切换目录；目标已有数据、未知会话目录、迁移失败或锁被占用时明确拒绝，不覆盖或以空库代替。正常完成重复执行无副作用；中断留下的未完成内容须明确处理，不带着不确定历史启动交易。启动失败不能自动恢复成旧单实例并与新模式混跑。

白名单布尔值在配置模型内部一次性转换为既有环境名称；SDK、容器路径、缓存身份与 Journal 命名空间保持不变。HTTP 与 CLI 同步使用必填 is_live，不新增输入别名或默认回退；请求和持久化边界见 [spec_04_environment.md](spec_04_environment.md)。
