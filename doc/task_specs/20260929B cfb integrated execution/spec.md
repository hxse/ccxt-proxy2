# CFB 模块与独立执行容器集成

## 任务边界

复制源项目的终端模块、原生 helper、容器资产、测试和文档；不修改或删除源项目及其配置。保持独立 src/cfb 模块与 按 sandbox/live 分开的 CFB 执行容器，只有主项目 FastAPI 提供 HTTP。

替换现有 CfbProxy HTTP 转发、CFB OpenAPI 快照、同步脚本及对应旧测试。源 CFB 的独立 FastAPI 应用、HTTP 启动入口和原配置加载入口不成为本项目生产入口。原任务材料完整归档，现行规则迁入 cfb_* current spec，所有经验及验证限制均有去向。

除环境选择的明确迁移外，保留八条业务路由的输入、结果、能力限制、幂等及取消语义。保持主应用与其他 SDK 故障隔离，不改 CCXT、TQ、原生 CTP 的算法。白名单使用必填布尔值 is_live（true=live、false=sandbox），公共与 remote 均为内联数组，远端只保留 CCXT/CFB 的 live；HTTP/CLI 同步迁移为必填 is_live，不保留旧 mode 输入。原 CFB 账户环境不自动切换；测试不得发送真实订单或撤单。

停止线：文档与配置迁移、统一路由和协议、独立执行镜像、白名单自动编排、离线回归及本地镜像检查完成。CFB 上游登录不可用不阻断其他服务；不以只读验证宣称交易闭环已验证。不上传远端，不操作原项目的容器或卷。

## 任务规范

完整协议与路由等价边界见 [spec_01_protocol.md](spec_01_protocol.md)，配置及容器所有权见 [spec_02_runtime.md](spec_02_runtime.md)，来源与验证迁移见 [spec_03_preservation.md](spec_03_preservation.md)，统一环境参数和验证边界见 [spec_04_environment.md](spec_04_environment.md)。

主进程只负责鉴权、请求模型、HTTP 等待和格式转换；每个模式的执行进程独占 BridgeService、Dispatcher、Journal、WorkerProcess、Wine 及图形会话。主进程不创建第二个 journal 或队列，不执行原生交易逻辑，不挂载 Podman socket。

CFB 初始化只建立客户端及本地状态，不等待 GUI 登录。协议连通与终端交易就绪分开；CFB 失败只影响对应模式的路由。已保存的幂等响应不能被主进程的交易就绪门禁提前拦截。

白名单存在 cfb 时，正式 serve 和部署启动入口准备并启动执行容器。构建时准备相应镜像，不隐式启动；未启用不准备、不启动。配置由统一加载器按 dev/local/remote 合并，不通过上传补丁改变值。

## 公开接口与用户写法

八条 /cfb 业务路径保留，GET 查询参数和 POST 请求体中的必填 is_live 选择对应实例；所选模式未启用返回 503 SERVICE_NOT_ENABLED，不可用返回明确错误，绝不跨模式回退。统一使用本项目 Bearer 鉴权。主服务新增 /cfb/status、/cfb/readyz、/cfb/healthz、/cfb/desktop/screenshot 承接原诊断能力；四条诊断路由使用必填 is_live 查询参数；全局 /readyz 仍表示主应用可服务，services 按 cfb/sandbox、cfb/live 分开。

```toml
# 服务白名单；is_live 必填，true=实盘，false=模拟盘；TQ 不带此字段。
service_whitelist = [
    { service = "cfb", is_live = false },
    { service = "cfb", is_live = true },
]

[cfb]
enable_proxy = false # 本地直连；只影响独立 CFB 执行器。
vnc_enabled = true # 允许本机 VNC/noVNC 查看桌面。
request_timeout_seconds = 300 # HTTP/IPC 等待上限，不代表交易自动取消。
[cfb.bridge]
data_dir = "/data" # 执行容器内固定数据根；宿主对应 data/cfb/<mode>。
startup_timeout_seconds = 90 # 登录和桌面初始化预算。
[cfb.accounts.sandbox]
broker_id = "9999" # 原生券商编号。
site = "电信2" # 原生站点。
username = "" # 私有配置填写，示例不含账号。
password = "" # 与 username 一起填写。
[cfb.accounts.live]
broker_id = "6020" # 现有实盘适配的券商。
site = "一套" # 所选原生站点。
username = "" # 私有配置填写，示例留空。
password = "" # 与 username 一起填写。
[cfb.vnc.sandbox]
port = 45174 # 模拟盘 VNC，仅回环发布。
web_port = 45175 # 模拟盘 noVNC。
[cfb.vnc.live]
port = 45176 # 实盘 VNC，仅回环发布。
web_port = 45177 # 实盘 noVNC。
[overrides.remote]
# 完整远端清单，其他需要启用的服务也须在此列出。
service_whitelist = [
    { service = "cfb", is_live = true },
]
[overrides.remote.cfb]
enable_proxy = true # 复用 [proxy] 的 HTTP CONNECT 地址。
```

原 CFB 的 desktop/vnc/execution/reconnect/logging/artifacts 及两组 accounts 配置保留在 cfb 子表；不再接收 cfb.base_url 或 cfb.api。公开示例逐项中文注释。省略 service_whitelist 仍表示无服务；本地默认示例和私有配置明确加入 cfb/sandbox 和 cfb/live，remote 数组仅保留 CFB live，不改变空白名单测试或部署烟测的含义。

```bash
just serve
just deploy --target=local --build --start
just deploy --target=remote --upload --build --start
just test
just test-cfb-native
just cfb --is-live=false --status
just cfb --is-live=false --pause
just cfb --is-live=false --resume
just cfb --is-live=false --clean
just cfb --is-live=false --screenshot --output=debug/cfb-desktop.png
```

cfb 运维必须显式指定 --is-live=true|false，支持 --target=local|remote，默认 local，动作互斥；--logs 持续跟随，其他动作使用唯一执行器协议，不提供交易动作。

开启 CFB 代理但缺少地址或地址不支持 HTTP CONNECT 时，在初始化终端前报错，不静默直连。无账号只启动桌面，不伪造就绪。容器和配置身份不符时按正式更新流程替换，同一模式不同时运行两个受管 GUI owner。

## 测试、验证与阶段过渡

先完成独立模块与协议，再接统一路由和配置，随后接容器编排；最终一次替换旧转发，不提供长期新旧双轨。实现期间尚未接通的 CFB 入口不得用于真实交易。

通过 just test 的全量离线回归、just lint、just check。原 CFB Python 测试迁入本仓，替换 HTTP 层夹具但保留业务不变量；原生/Capture 验证放独立无网络容器入口 just test-cfb-native，不由默认离线测试隐式联网构建。

新增真实 Unix socket 的并发、断连、帧边界、协议版本、资源释放测试，以及参数/响应/头/幂等等价测试；验证未开始取消、已开始收尾和持久化、失联不重发、未就绪时已存幂等可重放。验证每个 CFB 模式不可用不影响另一模式及其他 SDK，错模式在执行器拒绝，相同幂等键在两种模式中互不干扰。

使用隔离 Podman 替身覆盖白名单、serve/部署、配置覆盖、复用、替换、失败保护、停止与数据隔离，以及旧数据迁移/失败保留。白名单必须验证 is_live 必填严格布尔值、旧字段拒绝、内联数组作用域及远端排除 sandbox，不改变现有环境身份。镜像内检查 32 位原生构建、独立执行进程无 HTTP、代理 CONNECT 和直连分支，以及两个空账户容器同时运行和停止隔离；真实在线仅限只读和必要启动检查，独立入口执行，不运行真实交易。
