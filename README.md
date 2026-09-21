# ccxt-proxy2

带 Bearer 鉴权的 FastAPI 代理服务，提供 CCXT 交易所接口、CTP 交易、CFB HTTP 转发、TQ 行情、Telegram 文本消息和公共时间查询。

正式支持的 CCXT 范围为 Binance USDⓈ-M linear Futures 与 Kraken Futures。模块边界、当前接口和约束从[现行规范总览](doc/current_specs/architecture.md)开始阅读。

## 本地启动

首次将 [config.example.toml](config.example.toml) 复制为项目根目录 config.toml，填写需要的配置；已有配置时直接编辑原文件。

```bash
uv sync --locked
just serve
```

CCXT、TQ、CTP 和 CFB 由统一 service_whitelist 启用，只在启动时读取配置。详细字段、白名单、鉴权及旧配置迁移工具见[配置与生命周期](doc/current_specs/configuration.md)。真实配置被 Git 和镜像构建上下文排除。

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
- 原生期货交易：`uv sync --locked --extra ctp` 后使用 `just serve-ctp`；见 [CTP 规范](doc/current_specs/ctp_trading.md)。
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

显式只读在线入口为 `just test-online`，按模块可使用 `just test-ccxt-online`、`just test-tq-online`。测试使用的配置、缓存隔离和各入口的副作用边界见[验证规范](doc/current_specs/verification.md)。

Bruno 位于 [bruno](bruno)，复用本项目登录配置。单个请求用 `just bru-run`，例如：

```bash
just bru-run 'CFB/fetch_trading_status.bru'
just bru-cfb-readonly
```

下单、撤单、设置和发送消息示例标记 [STATEFUL]，按需单独运行。

## 容器部署

```bash
docker compose up -d --build
```

配置通过只读挂载提供，当前使用单 Uvicorn 进程。镜像运行方式和 CFB 容器连接地址分别见[配置规范](doc/current_specs/configuration.md)与 [CFB 规范](doc/current_specs/cfb_proxy.md)。

## 文档维护

当前规范位于 doc/current_specs；正式任务在 doc/task_specs 保留 meta、context、spec，并遵循 [AGENTS.md](AGENTS.md) 的 JJ change 命名及工作区规则。doc/archive 保存研究样本、旧迁移过程和已否决设计，不作为当前功能承诺。
