# 配置场景与部署规范统一

## 任务边界

交付统一场景加载、CFB/代理配置迁移、CCXT 公共取时、原配置本地挂载、源码精确增量上传、原生平台构建及文档和测试。删除 scripts/container_proxy.py 及其旧调用、测试约定；上传原文件，不保留文本补丁兼容链。公共取时直接替换旧 HTTPX 获取链，不保留两套取时路径。

保留业务路由、SDK 故障隔离、鉴权、缓存算法、默认完整上传配置、--keep-remote-config、127.0.0.1:5123 发布、运行中旧版本保护及单实例规则。vnpy_ctp 仍在镜像依赖阶段编译并内置。平台兼容失败必须报错，不隐式模拟，不承诺 CTP 已支持所有 CPU。

停止线：离线验证、lint、类型检查通过，部署能力满足源码与配置完整性约束。公共取时完成本地镜像构建与隔离 live 只读验证；本次不上传、构建远端镜像或切换运行实例，不排查 CFB。AGENTS.md 的项目冻结由独立 chore 维护，本任务不改通用规范或推送版本历史。

## 任务规范

### 唯一配置加载链

- CCXT_PROXY_PROFILE 必须为 dev、local、remote；缺失或非法为脱敏 ConfigError。加载函数允许内部显式指定 profile，优先于环境；不能依靠容器识别或字段猜测。
- 顶层 overrides 可省略；存在时必须是表，子键只允许三个场景且各自为表。嵌套 overrides 不允许。覆盖只允许正式配置字段；未选中的场景也拒绝未知字段和非法结构，场景的必需字段和业务依赖按选中后的完整配置验证。
- 表递归按字段合并；数组和普通值整体替换。false、0、空字符串不被视为缺省；不修改原始输入对象或 TOML 文件。合并后交给既有 AppConfig 及后台配置校验。
- 开发/测试/调试入口显式选择 dev；本地部署预检和运行选择 local；远端上传预检、镜像预检和运行选择 remote。子进程和后台脚本继承明确场景。直接 uvicorn/Python 启动必须显式传入环境变量。
- 远端纯控制/构建及保留配置的上传仍只加载 deployment 字段，不要求其他账号有效；使用同一个场景合并规则和参数校验，不因此初始化 SDK。
- 公共配置三个代理开关 false，CFB 为 http://127.0.0.1:45173；overrides.remote 内三个开关 true，CFB 为 http://cn-futures-bridge:45173。沿用共享 proxy 地址和原交易 SDK 代理实现。现有私有文件及公开示例迁移，保留其他字段和凭据，不入库。

### 公共取时

唯一 fetch_public_time 入口通过独立 CCXT 异步 Binance 客户端调用 fetch_time()，固定 live 现货时间接口；不加载市场、不使用 API key 或交易注册表，也不占用交易实例锁。每次请求关闭客户端和 HTTP 会话。

使用启动配置的 binance.enable_proxy 与 proxy.effective_http，通过 aiohttp_proxy 传给异步 CCXT；关闭时明确禁用环境代理。即使没有 Binance 交易身份，开启代理但没有地址也应在配置阶段拒绝。不存在取时专用代理配置或环境猜测。

外部 HTTP 契约保持原样：无 query 参数，需 Bearer 鉴权，返回 serverTime 及原始扩展字段；CCXT 响应钩子在数值强制转换前验证非负 int64，拒绝布尔值、字符串和浮点数。保留 PUBLIC_TIME_* 错误、上游 HTTP 状态码脱敏回显、5 秒整次请求期限、no-store；不重试、不跟随重定向、不缓存或回退本机时间。外部取消传播并关闭资源。TQ 日历/映射及清理继续复用同一函数。

### 配置文件归属

本地生产直接挂所选 config.toml 和项目根 market_data.toml，均为只读；数据库/日志仍挂项目 data。预检读取同一文件和 local 场景。实例身份包含配置内容、路径与场景，换文件或修改内容后显式 start 会替换旧实例；进程内配置仍只启动时读取。

远端默认原样上传完整两份配置，在待发布区形成只读快照；不热改正在使用的旧快照。保留配置选项不读取或传输本地运行配置，继续支持已有完整快照；首次无配置拒绝。配置快照内容身份保持既有格式，实例另标记场景，避免误复用旧运行方式。

### 源码与平台

按 [spec_01_source_sync.md](spec_01_source_sync.md) 实施清单与摘要增量协议。源码、配置和数据分区，上传成功不等于启动。单独 build 只消费已提交版本，不隐式补上传。

移除 --platform=linux/amd64；镜像平台必须匹配实际 Podman 主机的平台。检查不硬编码 x86_64；status/logs/stop 不决定构建平台。依赖与源码阶段继续分离，下载/编译缓存按平台隔离。CTP 在隔离烟测中检查原生扩展可加载，失败不发布新准备版本。

## 公开接口与用户写法

以下是合并到完整配置中的真实配置片段；账户和 proxy 地址沿用现有值：

```toml
[binance]
enable_proxy = false
[kraken]
enable_proxy = false
[tq]
enable_proxy = false
[cfb]
base_url = "http://127.0.0.1:45173"

[overrides.remote.binance]
enable_proxy = true
[overrides.remote.kraken]
enable_proxy = true
[overrides.remote.tq]
enable_proxy = true
[overrides.remote.cfb]
base_url = "http://cn-futures-bridge:45173"
```

```bash
# Just 自动注入开发场景。
just serve
just test
# 直接运行需明确场景。
CCXT_PROXY_PROFILE=dev uv run --no-sync uvicorn src.main:app --host 127.0.0.1 --port 5123
# 本地挂原配置，程序采用 local 场景。
just deploy --target=local --start
# 远端部署能力支持显式上传和构建，本次取时修复不执行。
just deploy --target=remote --upload --build
# 以后需要启用已准备版本时，单独执行。
just deploy --target=remote --start
# 保留远端完整配置的既有写法。
just deploy --target=remote --upload --build --keep-remote-config
# 本次本地验收：构建包含离线烟测，然后仅验证 live 公共取时。
just deploy --target=local --build
just test-public-time-online
```

未知场景、overrides 拼写错误、未知覆盖字段或覆盖后缺少必需依赖，在启动/上传前报错且不打印配置值。合法配置上传前后字节一致；远端实际生效值由 remote 场景决定，不能仅根据公共字段判断。

## 测试、验证与阶段过渡

- 先完成实现和对应离线用例，再用 `just test` 验证全量离线契约；`just lint`、`just check` 必须通过。已有入口、夹具、子进程及迁移工具同步适配明确场景，不靠默认回退维持旧写法。
- 配置覆盖验证嵌套表、数组替换、false/0/空值、未选场景未知字段、缺失/非法场景、错误脱敏、源文件不变；验证 CFB 与三个代理开关在三个场景中的最终值。
- 本地验证实际挂载路径、配置身份变更和预检场景；远端验证配置原样传输及保留路径，代码中不得再调用上传补丁。
- 源码同步使用真实 Shell 和隔离 Podman 替身，覆盖首次全量、无变化零文件、单文件修改、删除/改名、远端文件损坏修复、陈旧基线拒绝、校验失败不发布、路径与符号链接安全、旧数据不受影响。
- 沿用构建失败保护、单实例、停止优先、资源定向清理；补原生平台匹配与不匹配验证。旧全文归档状态在首次新上传时按空清单重新传输，不再由新 build 消费旧源码格式；已有配置快照和在用镜像继续保留。
- 取时离线使用真实 CCXT 方法与 HTTP 传输替身，覆盖三个场景代理、环境代理隔离、无账号/无交易身份、严格原值及扩展字段、451/限流/重定向、网络异常、超时、取消和资源关闭。原 HTTPX 替身改为 SDK 传输替身，不保留旧取时实现的测试依赖。
- 实际执行 `just deploy --target=local --build`，包含原生 CTP 与无网络应用烟测；再执行独立入口 `just test-public-time-online`。后者用已构建镜像及只读原配置、local 场景，经本项目鉴权只读一次 live 公共时间；不初始化交易 SDK 或生产后台，不发布端口、不挂生产数据库，不上传。失败停止，不循环寻找成功样本。构建无过短总时限，在线请求及验证进程有界。
