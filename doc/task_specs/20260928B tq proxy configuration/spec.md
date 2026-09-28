# TQ 代理配置

## 任务边界

增加 tq.enable_proxy，缺省为 false；迁移本地 config.toml，使 Binance、Kraken、TQ 的开关均为 false。远端上传副本补丁扩展为开启已配置的三个分组。同步示例、受影响 current spec 和测试。

不变更代理地址及优先级，不创建缺失的服务分组，不改账号、白名单、路由、缓存算法或故障隔离。保留 --keep-remote-config 语义。本次实现和迁移不自动部署或重启现有容器；不改变其他 SDK 的代理行为，不推送历史。

停止线为三个开关配置、完整 TQ 传输链路、上传补丁及离线验证完成。临时连通性探针不保留为生产链路。

## 任务规范

- TQ 启用代理时使用 proxy.effective_http；启用的 TQ 服务缺少地址，在配置阶段拒绝。未配置新字段的旧 TOML 继续按 false 读取。
- TqManager 固定启动配置，向 SDK 客户端和元数据客户端传递相同有效地址；关闭开关时传递直连。不能在请求时重读配置。
- SDK 同步 HTTP、异步 HTTP、行情及交易状态 WebSocket 均遵守同一开关；后续查询、令牌请求和重新连接继续使用该设置。
- 同步 HTTP 和 WebSocket 的 SDK 适配绑定 TQ 调用上下文。共享库入口在上下文外保持原行为，不修改进程环境变量，不将 TQ 代理传播到 CCXT、CFB、公共时间或其他线程。正常退出和异常退出均恢复调用上下文。
- TQ 直连明确禁用环境代理；启用时显式传入所选代理，保持 TLS 验证、既有超时及错误契约。SDK 原有认证、消息循环和重连算法不复制重写。
- 自有日历、主连历史源的 httpx 客户端使用相同代理，保留下载超时、刷新和关闭规则；公共取时仍遵循其独立规范。
- 远端补丁只将已配置 binance、kraken、tq 的 enable_proxy 改为 true，缺省开关补入，已有 true 不重复修改。语义核对、私有文件权限、发送前配置验证沿用现有入口。
- 本地迁移只补入或修改三个开关为 false，保留凭据、其他值及注释；配置不进入版本控制或镜像。local 构建/启动不打补丁，远端保留配置选项不打补丁。

## 公开接口与用户写法

以下为应合并进完整 config.toml 的配置片段：

```toml
[proxy]
# 三个服务共用的 HTTP 代理地址，示例值需替换。
http = "http://proxy.example.com:3128"

[binance]
# 本地默认直连；远端上传副本自动改为 true。
enable_proxy = false

[kraken]
# 本地默认直连；远端上传副本自动改为 true。
enable_proxy = false

[tq]
# 同时控制认证、行情连接和日历/主连源下载。
enable_proxy = false
username = "your-tq-user"
password = "your-tq-password"
```

`just deploy --target=remote --upload --build --start` 沿用现有入口，默认上传副本内三个开关为 true。附加 `--keep-remote-config` 完全保留远端配置，不运行代理补丁。

启用 TQ 白名单及 tq.enable_proxy=true，却未提供 proxy 地址时，启动或上传前返回配置错误，错误不包含配置值。路由参数及返回结构不变。

## 测试、验证与阶段过渡

- `just test` 运行离线回归。验证旧配置缺省、显式开关、启用但缺地址、配置快照传递；同步认证、异步 HTTP、WebSocket、元数据下载均正确选路。
- 截获真实已安装 SDK/HTTP 库的请求入口，不访问外网；验证环境代理不能改变 TQ 直连，其他调用上下文和线程仍保持自己的配置，异常退出不会泄漏代理。
- 检查真实上传包三个开关均开启，本地原件、其他字段、注释及 market_data.toml 不变；覆盖缺省字段、已有 true、缺失分组、错误类型、缺地址和保留配置路径。
- `just lint`、`just check` 通过；检查本地实际配置已迁移但仍未跟踪。默认测试不得触发真实认证或交易。
- 直接替换两个开关的上传补丁规则，不保留平行实现；历史配置缺省属于字段默认值，无需运行时迁移链路。
