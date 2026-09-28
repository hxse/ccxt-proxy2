# Podman 构建与部署

开发、测试和检查在宿主 uv 环境运行。生产使用 Linux x86_64 Podman 镜像，本地优先 rootless，远端沿用 SSH 登录账号，可使用已有 root。远端只需 SSH、Podman、Shell 与常规 GNU 工具，不需要宿主 Python、uv、jq，不挂载容器管理 socket，不自动修改系统服务或安装工具。

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

--config 缺省沿用 CCXT_PROXY_CONFIG_PATH/config.toml，相对项目根目录解析。默认 remote upload、本地 start 校验完整应用配置和后台计划。remote build/start/control 及保留配置的上传只读取本地 deployment 分组，不要求本机应用账号有效；本地纯 build 不读取运行配置，也不接受 --config。

每次 upload 默认上传所选 config.toml 与项目根目录 market_data.toml，完整替换本次上传版本的配置。发送前由 `scripts/container_proxy.py` 在本机生成远端副本：将已配置 `[binance]`、`[kraken]`、`[tq]` 的 enable_proxy 设为 true，省略的开关补入，未配置的服务分组不创建。本地三个开关默认 false；本地原件不变，本地构建、启动不执行补丁，market_data.toml 原样上传。

补丁不改代理地址、协议、端口、账户、其他字段或注释。前后解析 TOML 核对只改变目标开关，再复用应用配置校验；缺少必需代理地址、非法 TOML 或无法安全定位开关时，在发送上传包前报错，隐藏配置值。独立 `[binance]`、`[kraken]`、`[tq]` 表中的布尔 enable_proxy 是标准写法。配置身份取补丁后内容；服务器 build/start 使用已上传快照，补丁不需要远端宿主 Python。

--keep-remote-config 仅与 remote+upload 使用，不成为下次默认值。它不读取或发送本地运行配置，也不执行代理补丁，复用远端最近上传的配置快照；没有源码上传记录时允许复用既有 prepared 配置，以衔接以前的部署。都不存在时要求首次上传配置，不读取手改的根目录副本。

配置快照位于 `.container/configs/<身份>/`，权限 0600，目录 0700。身份由两份配置内容与运行规范确定；实例只读挂载快照。构建上下文完全不含真实配置。缓存路径和 CTP flow_path 必须位于相对 data/ 或绝对 /app/data/ 内，宿主 data/ 独立持久化。

## 上传版本与构建版本

源码包只包含 Dockerfile、.dockerignore、锁文件、Python 项目声明、src 的 Python/JSON 文件、vendor/vnpy_ctp 构建资料及三个后台运行脚本，包含当前未提交的源码修改。配置单独传输；虚拟环境、数据库、缓存、日志、版本历史、测试和草稿不进入源码包或镜像。

外层归档按动作核对固定文件集合；默认必须同时有两份配置，保留时两份都不能出现。内层源码归档再核对摘要、普通文件类型、成员路径及必需文件，拒绝目录穿越、符号链接、重复、缺失或其他内容。构建前重新核对摘要并解包到私有临时目录。

- `.container/sources/<摘要>.tar.gz` 保存唯一最新源码归档。
- `.container/uploaded` 两行分别保存源码摘要和配置身份，由 upload 原子提交。
- `.container/prepared` 两行分别保存镜像 ID 和配置身份，由成功 build 原子提交。

元数据只按固定格式读取，不能当 Shell 源码执行。源码上传成功不代表构建成功；build/start 缺少对应版本时明确要求先上传或构建。构建前失败保留原准备版本；提交后清理失败明确报告新版本已保存并终止后续步骤，不宣称已回滚。

start 在操作锁内读取 prepared，不能猜测任意 latest 或本机配置。启动成功后，在远端根目录保存两份当前配置副本；后续启动真值仍是 prepared 和快照。修改根目录副本不改变运行配置。

传输与构建临时目录用后删除，配置不打印到日志。源码与配置快照只保留最新上传/准备版本及现存容器引用，不维护历史发布库。部署不上传、替换或删除数据库。

## 分阶段构建与资源清理

本地与远端共用 Shell 构建和生命周期实现。本机 Python CLI 只负责参数、配置、打包和转发；配置预检与应用烟测只在镜像内执行。

Dockerfile 固定 Python slim 摘要与 uv 版本，构建锁定依赖和 CTP extra；依赖阶段先构建并保留 `localhost/ccxt-proxy2:dependencies`，运行阶段复用依赖，只复制必要源码。限制依赖构建并行度，正常编译不受过短总超时打断。

候选镜像先使用空白名单、关闭后台计划和无网络环境加载 CTP 扩展，验证应用生命周期及 readyz；remote build 再校验实际配置，不初始化 Provider。通过后更新 `localhost/ccxt-proxy2:latest` 和 prepared，删除临时候选标签。失败保留原可用版本。

构建层和运行镜像均有明确项目/用途标签。清理只针对本项目，通过镜像父子关系先删子层；保护当前镜像、prepared、最新依赖缓存、任何容器或外部镜像引用以及它们的祖先。不会全局 prune、删除其他项目、数据卷、数据库或有用下载缓存。

正常部署后只有一个应用运行版本，另保留当前构建需要的一套依赖缓存。旧实例仍引用的版本暂时保留，切换并确认新实例就绪后再删除；旧源码包、无引用配置和无用构建层也一并清理，不无限积累版本。

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

宿主固定发布 `127.0.0.1:5123:5123`；容器内 Uvicorn 监听 0.0.0.0:5123。宿主与容器不能同时占用 5123，冲突明确失败，不杀其他进程。CFB、代理等上游仍使用用户配置的可达地址，容器回环不自动指向宿主。

```bash
ssh -N -L 15123:127.0.0.1:5123 rn
# 浏览器访问 http://127.0.0.1:15123/
```

`just test` 为离线验证，包括真实 Shell/SDK 配合隔离替身的正反用例、归档安全、资源清理、锁和恢复。实际构建/上传/启停不进入默认测试；额外在线测试通过独立入口显式运行，只请求 live 只读数据。部署启动会正常初始化配置启用的 Provider 和后台计划，包括缓存清理。宿主命令见 [Just 命令规范](commands.md)。
