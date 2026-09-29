# Podman 构建与部署

开发、测试和检查在宿主 uv 环境运行。生产使用按实际构建机原生平台装配的 Podman 镜像，本地优先 rootless，远端沿用 SSH 登录账号，可使用已有 root。远端只需 SSH、Podman、Shell 与常规 GNU 工具，不需要宿主 Python、uv、jq，不挂载容器管理 socket，不自动修改系统服务或安装工具。

## 唯一入口与动作

```bash
# 本地构建、启动
just deploy --target=local --build
just deploy --target=local --start
just deploy --target=local --build --start

# 上传源码与配置，在 VPS 构建并启动
just deploy --target=remote --upload --build --start
# 仅本次保留远端已有配置
just deploy --target=remote --upload --build --start --keep-remote-config

# 按需分步执行
just deploy --target=remote --upload
just deploy --target=remote --build
just deploy --target=remote --start
just deploy --target=remote --build --start

# 控制操作独立执行，也可使用 --target=local
just deploy --target=remote --stop
just deploy --target=remote --status
just deploy --target=remote --logs
```

build 在 target 指定的机器执行。远端组合按 upload → build → start 顺序执行，与参数书写顺序无关，失败即停。upload 只保存源码与配置；build 使用最近上传的版本构建；start 使用最近成功构建的准备版本。remote 的 upload+start 必须包含 build，避免启动旧镜像。local 不接受 upload。stop/status/logs 各自独占。

裸 just deploy 和 --help 只显示帮助；带动作必须指定 target，非法参数在加载配置或操作 Podman 前拒绝。远端上传、构建和控制均不要求本机已有 Podman 或镜像。本地启动缺镜像时要求先 build，不隐式补建。

OCI 镜像上传链路已退出，不再先在本机构建再向 VPS 搬运镜像。旧 image-build、container-start、docker-*、Compose、GitHub Docker Hub 发布及裸 deploy 自动部署入口也已退出。

## 配置与 SSH 目标

```toml
[deployment]
# SSH 别名，沿用 ssh rn 的用户、端口、密钥。
ssh_host = "rn"
# 相对于 SSH 登录用户主目录，支持空格，不能使用绝对路径或 ..。
remote_dir = "dev/ccxt-proxy2"
```

字段为严格字符串，未知字段拒绝。ssh_host 只接受别名，不接受内嵌用户名、选项或整条命令。缺省分组不影响应用或本地容器；远程操作必须填写。SSH 保留 BatchMode、主机密钥校验及有限连接超时，不新增密码存储。

--config 缺省沿用 CCXT_PROXY_CONFIG_PATH/config.toml，相对项目根目录解析。默认 remote upload、本地 build/start 校验完整应用配置和后台计划。remote build/start/control 及保留配置的上传只读取本地 deployment 分组，不要求本机应用账号有效；本地 build 接受 --config，读取白名单以确定是否构建配套 CFB，不启动账户。

每次 upload 默认原样完整上传所选 config.toml 与项目根目录 market_data.toml，生成待发布配置快照。四个 SDK 的代理开关及 CFB 所选设置的远端差异由 overrides.remote 声明，配置加载器在内存中合并；上传端不修改任何运行字段。具体场景规则见[配置规范](configuration.md)。

配置预检和应用运行必须使用同一场景：local 或 remote。启用代理但缺地址、非法 TOML、未知覆盖字段或合并后配置非法，在上传/启动前报错并隐藏值。配置身份按原始文件内容计算；实例另有场景标记，不能因原文件相同误复用另一场景。

--keep-remote-config 仅与 remote+upload 使用，不成为下次默认值。它不读取或发送本地运行配置，复用远端最近上传的配置快照；没有源码上传记录时允许复用既有 prepared 配置，以衔接以前的部署。都不存在时要求首次上传配置，不读取手改的根目录副本。

远端配置快照位于 `.container/configs/<身份>/`，权限 0600，目录 0700；运行实例只读挂快照。本地生产直接只读挂所选原 config.toml 与项目 market_data.toml，不复制快照。其实例身份包含实际路径、内容和 local 场景；修改或换文件后显式 start 会替换实例。构建上下文完全不含真实配置。缓存路径和 CTP flow_path 必须位于相对 data/ 或绝对 /app/data/ 内，宿主 data/ 独立持久化。

## 上传版本与构建版本

源码白名单包含 Dockerfile、.dockerignore、锁文件、项目声明、src 的 Python/约定 JSON、vendor/vnpy_ctp 构建资料及三个后台脚本。包含当前工作区未提交源码；配置、虚拟环境、数据库、日志、历史和草稿不属于源码树。

上传先通过 inventory 取得当前版本和实际文件摘要。新 source.manifest 是完整且排序的 SHA256/相对路径清单，其摘要为版本身份；source.base 固定本轮基线，source.delta.tar.gz 只包含内容改变或新增的文件。无变化时差异包为空。首次部署及旧整包格式没有文件清单，第一次新上传完整传输，后续增量。

远端在操作锁内核对基线；其他发布者已更新则拒绝本次上传。临时受管树从旧源码复制、按完整清单删除旧文件、写入差异，再校验文件集合、普通文件类型和全部摘要。拒绝穿越路径、链接、重复、额外文件和损坏内容。删除不涉及配置与业务数据。

校验完成后提交 `.container/sources/<清单摘要>/`，内部为 files/ 与 manifest；uploaded 的两行仍为源码身份和配置身份。build 重新校验完整树，再在目标机装配镜像。尚未重新上传的旧源码格式不能由新 build 消费；已有配置快照仍可保留，已有运行实例不受迁移影响。

prepared 的两行仍为镜像 ID 与配置身份，只在构建及预检成功后更新。上传成功不代表启动；start 只使用已准备镜像和配套配置，不能任意猜测 latest。元数据不通过 source/eval 执行。提交前失败保留原有效版本；提交后清理失败明确报错并停止后续动作。

远端启动成功后在根目录保留当前配置副本，运行真值仍是 prepared 快照；本地直接挂原文件。临时传输、校验与构建目录用后删除，只保留当前上传源码、仍被上传/准备/运行引用的配置。部署不替换或删除数据库。

## 分阶段构建与资源清理

本地与远端共用 Shell 构建和生命周期实现。本机 Python CLI 只负责参数、配置、打包和转发；配置预检与应用烟测只在镜像内执行。

Dockerfile 固定 Python slim 摘要与 uv 版本，不强制 --platform；镜像平台必须匹配 Podman 主机实际平台，不隐式启用模拟器。构建锁定依赖和 CTP extra；依赖阶段先构建并保留 `localhost/ccxt-proxy2:dependencies`，运行阶段复用依赖，只复制必要源码。下载/编译缓存按 TARGETARCH 隔离。vnpy_ctp 暂时在依赖阶段编译并内置，限制编译并行度；不单独交付 wheel，不复制宿主虚拟环境。正常编译不受过短总超时打断，目标平台不兼容则在构建或原生扩展烟测中失败。

候选镜像先使用空白名单、关闭后台计划和无网络环境加载 CTP 扩展，验证应用生命周期及 readyz；remote build 再校验实际配置，不初始化 Provider。通过后更新 `localhost/ccxt-proxy2:latest` 和 prepared，删除临时候选标签。失败保留原可用版本。

构建层和运行镜像均有明确项目/用途标签。清理只针对本项目，通过镜像父子关系先删子层；保护当前镜像、prepared、最新依赖缓存、任何容器或外部镜像引用以及它们的祖先。不会全局 prune、删除其他项目、数据卷、数据库或有用下载缓存。

正常部署后只有一个应用运行版本，另保留当前构建需要的一套依赖缓存。旧实例仍引用的版本暂时保留，切换并确认新实例就绪后再删除；旧源码版本及遗留整包、无引用配置和无用构建层也一并清理，不无限积累版本。

## 启停、锁与恢复

固定容器名 ccxt-proxy2，并通过项目/目录标签确认所有权。同名非本项目容器或其他目录的实例明确拒绝操作。

- 相同镜像、配置及运行规范：复用实例并检查 readyz；停止状态则启动原实例。
- 有变化：先正常停止旧实例，再启用新实例，不允许两个进程同时访问同一 DuckDB。
- 就绪成功：删除旧实例并清理无用版本。
- 普通失败：停止并移除失败实例，恢复旧实例及原运行状态。本次命令仍返回失败；恢复失败明确报告。

本地构建/启动持本地项目操作锁；远端 upload/build/start 持目标目录操作锁，等待时提示并可取消。status/logs 不入队；stop 使用独立生命周期门禁更新停止代次并停止本项目实例，不删除配置、容器或数据。

包含 start 的流程在上传/构建/等待前记录停止代次，真正启动及就绪等待时再检查。stop 使旧流程失去后续启动资格，失败恢复也不能把已停止实例重新启动；构建或上传本身仍可完成。新的显式 start 可使用新代次启动。

readyz 最多等待 60 秒，每次请求有超时。失败实例移除前保存末尾日志到 `.container/last-startup.log`，权限 0600、最多 64 KiB，不在自动部署输出打印原始日志。容器日志限量 10 MB；不配置系统开机自启。

readyz 的就绪表示 HTTP 应用可服务；SDK 的 initializing/ready/failed/stopped 分别在 services 中展示。单个 SDK 不可用不会使部署失败或清掉已运行的容器，其路由返回统一 503，其他身份继续服务。只有应用自身不能建立 HTTP 生命周期或就绪检查失败时，才执行上述启动失败恢复。

status 返回 container/state/image_id/prepared_image_id。state 可为 absent/stopped/running，替换间隙为 updating；不代表 Provider 当前实时可用。未部署时：

```json
{"container":"ccxt-proxy2","state":"absent","image_id":null,"prepared_image_id":null}
```

## 端口与验证

宿主固定发布 `127.0.0.1:5123:5123`；容器内 Uvicorn 监听 0.0.0.0:5123。宿主与容器不能同时占用 5123，冲突明确失败，不杀其他进程。代理仍使用用户配置的可达地址，容器回环不自动指向宿主。CFB 通过共享数据目录的 Unix socket 连接，不使用容器 HTTP 地址。

```bash
ssh -N -L 15123:127.0.0.1:5123 rn
# 浏览器访问 http://127.0.0.1:15123/
```

`just test` 为离线验证，包括真实 Shell/SDK 配合隔离替身的正反用例、归档安全、资源清理、锁和恢复。实际构建/上传/启停不进入默认测试；额外在线测试通过独立入口显式运行，只请求 live 只读数据。部署启动会正常初始化配置启用的 Provider 和后台计划，包括缓存清理。宿主命令见 [Just 命令规范](commands.md)。

## CFB 配套执行器

白名单启用 cfb 时，build 同时准备独立 CFB 镜像，记录主镜像与执行镜像的配套关系；start 只消费对应版本，自动启动或复用 ccxt-proxy2-cfb。CFB 当前固定 Wine/Q72 原生资产仅支持 linux/amd64，主镜像的原生平台规则不因此改变。CFB 不启用时不准备此镜像。

CFB 代码源码、锁文件和构建资产进入既有增量清单；配置仍原样上传、内存应用场景覆盖。宿主 data/cfb 挂到执行器 /data，主容器已有 /app/data 挂载用于访问 socket。VNC/noVNC 默认 45174/45175，仅发布 127.0.0.1；不发布旧 45173。

按镜像和所选配置摘要复用，主服务重启不重启匹配 CFB；需要替换时先停止旧 owner。失败保留或恢复旧实例，协议身份检查阻止误用不匹配的账户。CFB 启动失败允许主应用继续；终端就绪由 /cfb/readyz 表示。显式项目 stop 停止两个受管实例；不会操作原 cn-futures-bridge 容器和卷。

镜像清理保留配套版本、运行实例和依赖祖先，删除失效配套记录及无用本项目版本；不全局 prune，不删除数据。CFB 原生验证镜像不视作生产旧版本。just serve 使用同一准备/启动实现，但主 API 仍运行在宿主。控制命令见 [命令规范](commands.md)，CFB 特有规则见 [运行规范](cfb_bootstrap.md)。
