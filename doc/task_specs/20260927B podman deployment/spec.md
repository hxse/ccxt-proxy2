# Podman 构建与私有部署

## 任务边界

提供唯一入口 `just deploy --target=local|remote`，组合上传、构建、启动，另提供停止、状态、日志。远端部署改为上传本地构建源码和运行配置，在 VPS 内通过 Podman 构建；退出本机构建后传送 OCI 镜像的链路。开发与离线检查继续使用宿主 uv，不引入开发容器、镜像仓库或 manylinux。

本任务同时修复已确认的 CCXT 代理映射：Binance、Kraken Futures 和 Kraken Spot 使用统一的 HTTP/HTTPS 代理字典。远端上传副本自动开启 Binance/Kraken 的代理；本地原件及本地启动保持配置原值。保留字段、地址优先级、凭据选择、sandbox/live 及市场初始化行为，不更改代理地址、协议、端口或取消注释。

删除旧 GitHub Docker Hub workflow、Compose、docker-*、image-build、container-start 等入口；保留既有 sync/serve 与安全 argv 转发。同步受影响 current spec、README、示例及项目特有规范。任务不迁移业务路由、缓存算法，不改变其他项目容器或远端系统服务，不推送版本历史。

停止线是离线验证通过，并通过新入口在 rn 实际上传、构建、启动和检查就绪；正常更新后只保留一个可用应用版本及当前需要的构建缓存。应用上游连接失败必须据实报告，不通过修改白名单、账户或降低就绪标准掩盖。

## 任务规范

### CCXT 代理

使用已有 `proxy.effective_http` 选择地址。启用代理时仅设置 `exchange.proxies = {"http": address, "https": address}`；关闭时为 None。删除三个工厂分支对 httpProxy 的赋值，不同时填写 httpProxy/httpsProxy/proxies。两种请求协议共用原配置地址，地址自身可为 http:// 或 https://，不根据目标请求协议改写地址。

### 动作、所有权与版本

- local 支持 build/start 组合；remote 按 upload → build → start 顺序执行，与参数顺序无关。build 在 target 指定的机器执行。
- remote upload 只上传构建源码及配置，提交上传版本，不启动或构建；remote build 使用最新上传版本构建；remote start 只启动已构建的准备版本。upload+start 必须显式包含 build，避免上传新源码却启动旧镜像。
- remote build/start 可不附带 upload，以复用已有上传/准备版本。stop/status/logs 各自独占；裸命令只显示帮助，未知或冲突参数在副作用前拒绝。
- 本机只做远端上传时不要求 Podman 或本机镜像，只需要现有 Python CLI、SSH 及配置。远端只需 Podman、Shell 与常规 GNU 工具，不安装宿主 Python、uv、jq，不挂载容器管理 socket。沿用 SSH 登录账号，允许 root。
- 源码归档以摘要命名，保存在 `.container/sources/`。`.container/uploaded` 用两行保存源码归档摘要与配置身份；`.container/prepared` 继续用两行保存镜像 ID 与配置身份。严格校验后读取，禁止 source/eval 元数据。
- upload 原子提交新的 uploaded；build 经烟测和实际配置预检后原子提交 prepared。提交前失败不改变原准备版本。提交后清理失败明确报错，停止后续启动，不宣称全部回滚。
- 保留一个最新源码归档。配置快照仅保留 uploaded、prepared 和现存容器引用的部分，不累计历史发布库。纯控制操作不创建应用数据或准备版本。

### 上传与配置边界

只打包 Dockerfile、.dockerignore、pyproject.toml、uv.lock、src 内的 Python/JSON 源文件、vendor/vnpy_ctp 构建资料，以及三个运行用后台脚本。只接受固定目录及普通文件，拒绝符号链接、路径穿越、重复文件与额外内容。上传包含未提交的当前源码修改。虚拟环境、缓存、数据库、日志、版本控制目录、测试、草稿及私有配置不进入源码归档。

默认另行上传所选 config.toml 和项目 market_data.toml，完整替换本次上传版本配置。发送前由 `scripts/container_proxy.py` 在本机生成远端专用副本：将已配置的 `[binance]`、`[kraken]` 的 `enable_proxy` 设为 true，省略的开关补入，不创建缺失的交易所分组。代理地址、其他字段和注释保持原样，market_data.toml 原样传输，本地原件不变；local 的构建和启动不执行补丁。

补丁前后均解析 TOML，确认只有两个目标开关的语义变化，并复用配置加载入口校验补丁后的配置；缺少必需代理地址、非法 TOML、无法唯一且安全定位开关时，在发送上传包前明确报错，不输出配置值。正常配置使用独立 `[binance]`、`[kraken]` 表及布尔 enable_proxy；配置快照身份按补丁后内容计算。远端 build/start 直接使用该快照，不依赖宿主 Python，也不在镜像构建中修改运行配置。

`--keep-remote-config` 只影响本次 upload，完全不读写或发送本地运行配置，也不执行代理补丁，复用远端最新上传版本的配置；若尚未有源码上传版本，允许沿用现有 prepared 中的配置快照，以衔接此前已部署版本。两者均不存在时明确要求首次上传配置，不读取手改的根目录副本。

外层归档按动作和配置选项检查固定文件集合，源码压缩包另外校验摘要、成员路径及类型，构建前再次校验并解包。源码构建目录与真实配置目录物理分离；配置只读挂载，永不放进镜像。临时传输、解包和烟测文件用后删除，目录 0700、配置 0600，不输出凭据。SSH 保留主机密钥校验、BatchMode 和有限连接等待。

### 构建与清理

本地和远端使用同一个 Shell 构建实现，Python CLI 仅转发。目标为 linux/amd64；固定 Python slim 摘要和 uv 版本，多阶段构建 CTP extra，依赖和源码分层。低资源 VPS 控制依赖构建并行度，不因编译耗时长而终止正常构建。依赖缓存的 uv 下载目录不写入交付镜像。

1. 构建 dependencies 阶段并保留本项目唯一依赖缓存标签，再构建临时候选运行镜像。构建只包含源码，不读取真实配置。
2. 使用无网络、空白名单、关闭后台计划的临时配置，在候选镜像内加载 CTP 原生扩展并验证完整应用生命周期及 readyz。
3. remote build 再用候选镜像校验上传配置和持久化目录约束，不初始化 Provider。通过后才更新 latest、准备元数据并删除候选标签。失败不覆盖原可用应用镜像或启动生产实例。
4. 正常完成后清理本项目无引用的旧运行镜像、失效构建层、旧源码归档和无用配置快照。通过用途标签识别本项目，按父子关系先删子层；保留当前运行/准备镜像、最新依赖阶段、所有容器引用和外部标签引用的镜像及其祖先。依赖层与应用层均有明确项目标记，共享基础镜像不标记为本项目。
5. 不全局 prune，不删除其他项目、数据卷、数据库或有用下载缓存。应用旧版本在被运行容器引用期间保留；切换成功删除旧实例后再清理，不为满足数量立即破坏旧服务。

### 启停、锁和失败恢复

本地构建/启动使用项目操作锁；远端 upload/build/start 共用目标目录操作锁，繁忙时等待并允许取消。status/logs 不入队；stop 使用单独生命周期门禁更新停止代次并停止本项目实例。包含 start 的请求在构建、上传或等待前固定停止代次，真正启动与就绪检查时核对；stop 使在途流程取消后续启动，不被失败恢复重新启动。构建本身可完成。

容器固定名 ccxt-proxy2，并用项目/目录标签核对所有权。相同镜像、配置和运行规范复用已有实例；变化时先正常停止旧容器再替换，不能同时写 DuckDB。同名外部实例、端口冲突、Podman 失败明确报错，不强制覆盖。普通启动失败清理失败实例并恢复原容器和状态；恢复失败明确说明，显式 stop 优先。

宿主只发布 `127.0.0.1:5123:5123`，容器内部监听 0.0.0.0:5123。数据位于宿主 data/，配置只读挂载私有快照。readyz 最多等待 60 秒；诊断保存为 0600 的单份 last-startup.log，最多 64 KiB。启动成功后更新根目录两份配置副本，后续 start 的真值仍是 prepared 快照。仅使用单 Uvicorn 进程与限量容器日志，不修改系统开机自启配置。

## 公开接口与用户写法

```bash
# 本地构建、运行
just deploy --target=local --build
just deploy --target=local --build --start

# 默认上传源码和两份配置，在服务器构建并启动
just deploy --target=remote --upload --build --start
# 本次不覆盖远端配置
just deploy --target=remote --upload --build --start --keep-remote-config

# 可以分步执行
just deploy --target=remote --upload
just deploy --target=remote --build
just deploy --target=remote --start
just deploy --target=remote --build --start
just deploy --target=remote --status
just deploy --target=remote --logs
just deploy --target=remote --stop
```

`--config` 缺省沿用 CCXT_PROXY_CONFIG_PATH/config.toml，相对项目根目录解析。remote upload 默认校验完整应用配置与后台计划；保留配置或只 build/start/control 时仅需 deployment 分组。local 的纯 build 不读取真实账户或接受 --config。local+upload、remote+upload+start 缺 build、stop+start、未 upload 却使用 --keep-remote-config 均直接拒绝。

```toml
[deployment]
# SSH 别名，使用现有用户、端口、密钥。
ssh_host = "rn"
# 相对于 SSH 登录用户主目录；不得包含 .. 或使用绝对路径。
remote_dir = "dev/ccxt-proxy2"
```

配置及持久化字段保持原有接口；代理示例：

```toml
[proxy]
http = "http://user:password@proxy.example.com:3128"
[binance]
enable_proxy = false
[kraken]
enable_proxy = false
```

该片段仅说明代理字段，账户、白名单等仍需按完整配置填写。本地运行保持 false；默认远端上传只在副本中将两个开关改成 true，以同一地址覆盖 HTTP/HTTPS 目标。已有 true 保持不变；补丁不处理 TQ 或其他 Provider。

status 继续返回 container/state/image_id/prepared_image_id；state 为 absent/stopped/running，替换间隙可为 updating。它不代表 Provider 的实时连通性。源码已上传但未构建时，prepared_image_id 仍为上一个可用准备镜像，不将上传成功等同于构建成功。

宿主使用 `just sync --extra=ctp` 准备依赖，`just serve --config=config.toml` 启动；默认 127.0.0.1:5123、reload、no-sync。既有 pytest、Bruno、uv sync、run 和调试脚本参数按 argv 安全透传。旧裸 deploy、docker-*、serve-ctp、cleanup 写法退出，明确副作用入口保留 debug-cleanup-sandbox。

## 测试、验证与阶段过渡

- `just test` 验证完整离线契约；通过真实 SDK 截获 request，确认三个 CCXT 工厂对 HTTP/HTTPS 均选择同一代理，关闭时无代理、不触发冲突；不连接交易所。
- 部署测试执行真实 Shell 配合隔离 Podman 替身，覆盖源码打包排除敏感数据、内外归档校验、两份配置的默认覆盖与显式保留、缺少上传版本/配置报错、独立远端构建不调用本机 Podman。
- 验证实际上传包中两个代理开关为 true，本地原件和 market_data.toml 不变；保留配置及 local 启动不触发补丁。覆盖缺省开关、已有 true、注释/凭据/换行保留，以及非法或歧义内容、缺少代理地址在发送前拒绝。
- 覆盖 upload→build→start 顺序、构建/烟测/配置失败不发布或启动、缓存复用、更新后仅一个运行版本、依赖祖先和其他项目资源保护、失效子层优先清理、持久化数据不变。
- 保留原有重复启动、停止后启动、失败恢复、锁、停止取消在途/等待启动和安全 argv 回归。Shell 语法检查纳入离线入口。
- `just lint`、`just check` 验证改动；文件不得超过项目上限。退出 OCI 上传实现及其过时测试、文档，保留既有 prepared 快照格式作为旧部署的有效配置来源，不运行旧部署实现。
- 使用 `just deploy --target=remote --upload --build --start` 完成实际构建、隔离烟测与部署；核对远端快照只开启两个代理开关、本地配置未变、readyz、回环绑定、实例唯一和镜像/构建缓存清理。本地通过 `just deploy --target=local --start` 验证配置原值和启动状态；构建等待不设过短总时限，SSH 连接和健康请求仍有有限超时。
- 在线只允许额外 live 公共只读请求；不新增交易写测试。部署按既有配置正常初始化 Provider/后台任务，不改白名单规避失败。在线操作和构建均不进入默认离线测试。
