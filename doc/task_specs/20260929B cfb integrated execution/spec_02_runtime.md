# 配置、代理与容器生命周期

## 配置归属

统一 config.toml 是唯一用户配置文件；CfbConfig 承接原 Settings 的账户、终端、执行、重连和文件治理子表，加 enable_proxy、vnc_enabled、request_timeout_seconds。原 HTTP api 子表和 base_url 删除。账号环境、站点、指纹及 journal 命名空间保持不变。

CFB 守护进程复用本项目配置加载器，按显式场景读取 cfb 与公共 proxy，不启动其他 SDK。原文件只读挂载，不上传后打补丁、不写入镜像。默认示例和私有配置的白名单加入 cfb，未启用不启动容器。

## 镜像与实例

主镜像保持现有 Python/CTP 依赖；CFB 单独按原锁定终端、Wine 11、Gecko、原生源码装配，固定版本校验不削弱。CFB 构建仅限当前支持的 linux/amd64，不让这一限制扩散到未启用 CFB 的主镜像。

受管实例名 ccxt-proxy2-cfb，与原 cn-futures-bridge 分开。宿主 data/cfb 独立挂到 /data，主服务访问同目录下的通信 socket；不将 journal 合并进 DuckDB，不让行情清理触碰它。CFB 私有数据首次导入独立于代码复制，不覆盖已有目标目录，不改原数据。

本地和远端编排仍由宿主 Shell/Podman 负责，运行容器不获得 Podman socket。build 接受 --config 并按白名单准备配套 CFB 镜像，记录主镜像 ID 与执行镜像 ID 的配对，upload 包含对应构建源码及锁文件，start 自动确保 CFB 实例已启动。serve 也按 dev 场景执行同一准备/启动逻辑；初始化/登录异步，失败记明原因而不阻断主应用及其他 SDK。

相同镜像和所选 CFB 配置身份复用；仅其他 SDK 配置改变不能重启 CFB。CFB 变化时先确认旧 owner 退出再替换，构建失败保留旧实例，不操作原项目容器/卷。不因主 API 重启而重启已匹配 CFB；显式项目 stop 停止本项目的两个受管实例。主进程关闭只释放通信资源，独立 CFB 由容器生命周期维护。

VNC/noVNC 可配置开启，宿主仅发布 127.0.0.1，保留原 45174/45175 默认端口；不再发布 45173 HTTP。原项目占用端口时明确记录冲突，不停止原实例。

## TCP 代理

cfb.enable_proxy 默认 false；overrides.remote.cfb.enable_proxy=true。读取公共 proxy.effective_http，支持 http:// 的 HTTP CONNECT 代理，关闭不继承 HTTP_PROXY 等环境代理。CFB 不依据 binance.enable_proxy 决定自身代理。

通过执行镜像内的 proxychains-ng 32/64 位动态库对 Wine/控制器 TCP 连接使用单一严格代理链。配置在私有运行目录生成，代理失效不回退直连；回环的 X11/本机控制通信保留本地连接。代理地址、账号和密码不打印。对无法表达的地址/认证内容明确拒绝，不静默丢字段。

只支持 TCP；没有为 UDP 或其他传输虚构代理支持。运行前验证库存在及位数，验证场景使用本地 HTTP CONNECT 替身观察真实连接，不以设置环境变量作为生效证明。

清理按配套关系保护准备版本、在用实例、构建依赖和其他用途的镜像，移除无用生产版本及失效配套记录，不全局 prune。重启/中断恢复必须先结束新 owner，再恢复旧实例，Journal 与数据目录始终保留。
