# CFB 模块与独立执行容器集成

## 任务边界

复制源项目的终端模块、原生 helper、容器资产、测试和文档；不修改或删除源项目及其配置。保持独立 src/cfb 模块与 ccxt-proxy2-cfb 执行容器，只有主项目 FastAPI 提供 HTTP。

替换现有 CfbProxy HTTP 转发、CFB OpenAPI 快照、同步脚本及对应旧测试。源 CFB 的独立 FastAPI 应用、HTTP 启动入口和原配置加载入口不成为本项目生产入口。原任务材料完整归档，现行规则迁入 cfb_* current spec，所有经验及验证限制均有去向。

保留八条业务路由的输入、默认值、结果、能力限制、幂等及取消语义。保持主应用与其他 SDK 故障隔离，不改 CCXT、TQ、原生 CTP 的算法。原 CFB 账户环境不自动切换；测试不得发送真实订单或撤单。

停止线：文档与配置迁移、统一路由和协议、独立执行镜像、白名单自动编排、离线回归及本地镜像检查完成。CFB 上游登录不可用不阻断其他服务；不以只读验证宣称交易闭环已验证。不上传远端，不操作原项目的容器或卷。

## 任务规范

完整协议与路由等价边界见 [spec_01_protocol.md](spec_01_protocol.md)，配置及容器所有权见 [spec_02_runtime.md](spec_02_runtime.md)，来源与验证迁移见 [spec_03_preservation.md](spec_03_preservation.md)。

主进程只负责鉴权、请求模型、HTTP 等待和格式转换；执行进程独占 BridgeService、Dispatcher、Journal、WorkerProcess、Wine 及图形会话。主进程不创建第二个 journal 或队列，不执行原生交易逻辑，不挂载 Podman socket。

CFB 初始化只建立客户端及本地状态，不等待 GUI 登录。协议连通与终端交易就绪分开；CFB 失败只影响自身路由。已保存的幂等响应不能被主进程的交易就绪门禁提前拦截。

白名单存在 cfb 时，正式 serve 和部署启动入口准备并启动执行容器。构建时准备相应镜像，不隐式启动；未启用不准备、不启动。配置由统一加载器按 dev/local/remote 合并，不通过上传补丁改变值。

## 公开接口与用户写法

八条 /cfb 业务路径与原参数保持不变，统一使用本项目 Bearer 鉴权。主服务新增 /cfb/status、/cfb/readyz、/cfb/healthz、/cfb/desktop/screenshot 承接原诊断能力；全局 /readyz 仍表示主应用可服务。

```toml
[[service_whitelist]]
service = "cfb"

[cfb]
enable_proxy = false # 本地直连；只影响独立 CFB 执行器。
vnc_enabled = true # 允许本机 VNC/noVNC 查看桌面。
request_timeout_seconds = 300 # HTTP/IPC 等待上限，不代表交易自动取消。
[cfb.bridge]
mode = "sandbox" # 与原 CFB 启动环境一致，请求不得改变环境。
data_dir = "/data" # 执行容器内固定数据根；宿主对应 data/cfb。
startup_timeout_seconds = 90 # 登录和桌面初始化预算。
[cfb.accounts.sandbox]
broker_id = "9999" # 原生券商编号。
site = "电信2" # 原生站点。
username = "" # 私有配置填写，示例不含账号。
password = "" # 与 username 一起填写。
[overrides.remote.cfb]
enable_proxy = true # 复用 [proxy] 的 HTTP CONNECT 地址。
```

原 CFB 的 desktop/vnc/execution/reconnect/logging/artifacts 及两组 accounts 配置保留在 cfb 子表；不再接收 cfb.base_url 或 cfb.api。公开示例逐项中文注释。省略 service_whitelist 仍表示无服务；默认示例和本次私有配置明确加入 cfb，不改变空白名单测试或部署烟测的含义。

```bash
just serve
just deploy --target=local --build --start
just deploy --target=remote --upload --build --start
just test
just test-cfb-native
just cfb --status
just cfb --pause
just cfb --resume
just cfb --clean
just cfb --screenshot --output=debug/cfb-desktop.png
```

cfb 运维支持 --target=local|remote，默认 local，动作互斥；--logs 持续跟随，其他动作使用唯一执行器协议，不提供交易动作。

开启 CFB 代理但缺少地址或地址不支持 HTTP CONNECT 时，在初始化终端前报错，不静默直连。无账号只启动桌面，不伪造就绪。容器和配置身份不符时按正式更新流程替换，不同时运行两个受管 GUI owner。

## 测试、验证与阶段过渡

先完成独立模块与协议，再接统一路由和配置，随后接容器编排；最终一次替换旧转发，不提供长期新旧双轨。实现期间尚未接通的 CFB 入口不得用于真实交易。

通过 just test 的全量离线回归、just lint、just check。原 CFB Python 测试迁入本仓，替换 HTTP 层夹具但保留业务不变量；原生/Capture 验证放独立无网络容器入口 just test-cfb-native，不由默认离线测试隐式联网构建。

新增真实 Unix socket 的并发、断连、帧边界、协议版本、资源释放测试，以及参数/响应/头/幂等等价测试；验证未开始取消、已开始收尾和持久化、失联不重发、未就绪时已存幂等可重放。验证 CFB 不可用不影响其他路由。

使用隔离 Podman 替身覆盖白名单、serve/部署、配置覆盖、复用、替换、失败保护、停止与数据隔离。镜像内检查 32 位原生构建、独立执行进程无 HTTP、代理 CONNECT 和直连分支；真实在线仅限只读和必要启动检查，独立入口执行，不运行真实交易。
