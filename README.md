# ccxt-proxy2

带 Bearer 鉴权的 FastAPI 代理服务，提供 CCXT 交易所接口、CTP 交易、CFB HTTP 转发、TQ 行情、Telegram 文本消息和公共时间查询。

正式支持的 CCXT 范围为 Binance USDⓈ-M linear Futures 与 Kraken Futures。模块边界、当前接口和约束从[现行规范总览](doc/current_specs/architecture.md)开始阅读。

## 本地启动

首次将 [config.example.toml](config.example.toml) 复制为项目根目录 config.toml，填写需要的配置；已有配置时直接编辑原文件。

```bash
just sync
just serve
```

CCXT、TQ、CTP 和 CFB 由统一 service_whitelist 启用，只在启动时读取配置。详细字段、白名单、鉴权及旧配置迁移工具见[配置与生命周期](doc/current_specs/configuration.md)。真实配置被 Git 和镜像构建上下文排除。

各 SDK 独立初始化，单个服务失败不影响其他路由。访问未就绪的服务返回 `503 SERVICE_NOT_READY`；`/readyz` 返回应用就绪及各服务状态，应用可用不代表每个 SDK 都已连接成功。

[market_data.toml](market_data.toml) 默认每小时采集 TQ 主连/加权并清理缓存，live 保留三万根、sandbox 清零。已有私有配置需按示例补充 market_data_client.user，引用已有 HTTP 登录用户；不需要后台任务时关闭公开计划的两个 enabled。后台仅 5m 请求映射与过渡，用户路由不受采集清单限制，详见[后台任务](doc/current_specs/market_data_jobs.md)。

服务默认监听 127.0.0.1:5123：

| 入口 | 地址 |
| --- | --- |
| Swagger UI | http://127.0.0.1:5123/docs |
| Scalar | http://127.0.0.1:5123/scalar |
| ReDoc | http://127.0.0.1:5123/redoc |
| OpenAPI | http://127.0.0.1:5123/openapi.json |

## 常用功能入口

- CCXT 行情与交易：[客户端规范](doc/current_specs/ccxt_client.md)、[OHLCV 查询](doc/current_specs/ccxt_ohlcv.md)。
- 中国期货行情、交易日历与开市状态：[TQ 规范](doc/current_specs/tq_data.md)。
- 原生期货交易：`just sync --extra=ctp` 后使用 `just serve`；见 [CTP 规范](doc/current_specs/ctp_trading.md)。
- 期货公司 GUI 联调：`just ctp-assessment` 临时安装完整 VeighNa；见[联调规范](doc/current_specs/ctp_assessment.md)。
- 独立 CFB 服务：八条同名 /cfb 路由薄转发，`just sync-cfb-docs` 同步上游自动文档；见 [CFB 规范](doc/current_specs/cfb_proxy.md)。
- 文本通知：[Telegram 规范](doc/current_specs/telegram.md)。
- 公共时间：`GET /system/fetch_time`；见[公共时间规范](doc/current_specs/system_time.md)。

CTP 后台仅安装、加载固定补丁版本的 VeighNa 交易扩展，完整 GUI 环境独立。依赖来源、补丁和 `just update-ctp` 更新方式见 [vendor/vnpy_ctp](vendor/vnpy_ctp/README.md)。

## 验证与手动请求

默认离线验证：

```bash
just test
```

显式只读在线入口为 `just test-online`，会自行启动并关闭独立 HTTP 服务，无需先运行 just serve。按模块可使用 `just test-ccxt-online`、`just test-tq-online`。测试只启用已配置的 live 行情，关闭后台计划并隔离缓存；详细边界见[验证规范](doc/current_specs/verification.md)。

Bruno 位于 [bruno](bruno)，复用本项目登录配置。单个请求用 `just bru-run`，例如：

```bash
just bru-run 'CFB/fetch_trading_status.bru'
just bru-cfb-readonly
```

下单、撤单、设置和发送消息示例标记 [STATEFUL]，按需单独运行。

## Podman 构建与部署

```bash
just deploy --target=local --build          # 分阶段构建并隔离验证
just deploy --target=local --start          # 本地启动或复用
just deploy --target=remote --upload --build --start # 上传源码、配置，在远端构建并启动
just deploy --target=remote --upload --build --start --keep-remote-config # 本次保留远端配置
```

开发和测试继续使用宿主 uv 环境。运行镜像使用 Podman（本地优先 rootless，远端沿用现有登录账号），宿主只发布 `127.0.0.1:5123`。远端无需安装 Python，只需 SSH、Podman 和常规系统工具。配置不进入镜像，数据独立挂载；不再通过 GitHub Actions 发布镜像。

远程操作需在私有 `config.toml` 添加 `[deployment]`，填写 `ssh_host = "rn"`、`remote_dir = "dev/ccxt-proxy2"`；用 `--config=config.toml.vps` 选择目标及上传配置。每次上传默认完整覆盖 `config.toml` 和 `market_data.toml`；只有显式 `--keep-remote-config` 才沿用远端已有配置，首次部署不能使用。可单独 `--upload` 保存源码及配置，之后在远端 `--build` 构建，再 `--start` 启用；`--stop`、`--status`、`--logs` 用于控制和查看。后台自身地址为 `http://127.0.0.1:5123`。动作组合、失败恢复和 SSH 隧道见[容器部署规范](doc/current_specs/container_deployment.md)。

默认远端上传会自动将配置副本中 Binance、Kraken 的 `enable_proxy` 打开，使用已填写的 `[proxy]` 地址；本地原件、代理地址及其他字段不变。本地启动按原配置运行；使用 `--keep-remote-config` 时也不执行补丁。

源码启动可用 `just serve --config=config.toml --host=127.0.0.1 --port=5123`；启动不再隐式同步依赖。完整命令职责和参数透传规则见 [Just 命令规范](doc/current_specs/commands.md)。

## 文档维护

当前规范位于 doc/current_specs；正式任务在 doc/task_specs 保留 meta、context、spec，并遵循 [AGENTS.md](AGENTS.md) 的 JJ change 命名及工作区规则。doc/archive 保存研究样本、旧迁移过程和已否决设计，不作为当前功能承诺。
