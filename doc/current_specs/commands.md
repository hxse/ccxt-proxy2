# Just 命令与宿主开发

Just 只做薄编排，默认 just 显示帮助；复杂参数处理与运行逻辑放在项目脚本。Just 源码、测试和辅助入口自动注入 CCXT_PROXY_PROFILE=dev；部署入口按 target 显式选择 local/remote，预检与运行一致。项目不使用开发容器，源码运行、测试、静态检查继续在宿主 uv 环境执行；镜像部署使用独立的 [Podman 入口](container_deployment.md)。

## 准备依赖与源码运行

```bash
just sync
just sync --extra=ctp
just serve
just serve --config=config.toml.local --host=127.0.0.1 --port=5123
```

sync 默认执行 uv sync --locked，透传 uv 原生附加参数，不隐式升级。CTP 是可选依赖，显式加 --extra=ctp；首次准备与后续同步用同一入口，不再另造 setup。

serve 使用既有 uv 环境且固定 no-sync，不在启动时安装依赖。参数只接受命名选项，端口必须在 1..65535，默认绑定 127.0.0.1:5123，并保留开发 reload。配置仍由集中加载器解析，显式 --config 优先于既有环境选择器，相对路径按项目根目录解释。缺依赖或配置错误明确报错，serve-ctp 已退出。

run 继续用于指定 Python 脚本，不与 serve 互换含义：

```bash
just run minimal_example/adjust_amount.py
```

运行脚本可能有外部副作用，取决于所选脚本，不代表离线验证。

## 参数透传与验证

新部署和源码启动入口使用命名长选项。已有 pytest、Bruno、uv sync、调试脚本以及 run 的原生位置参数保留为项目例外。Just 使用 positional-arguments 和引用 argv 转发，含空格、引号的单个参数不得拆分，不将用户输入插入 shell 源码。

```bash
just test
just test-file Test/test_container_lifecycle.py -k 'stop or ready'
just test-online
just deploy --target=local --build
just test-public-time-online
just bru-run 'CFB/fetch_trading_status.bru'
```

test 为全量离线，可透传 pytest 选项。单文件范围使用 test-file，它不追加整个 Test 目录。test-online 仍需显式调用，只运行配置支持的 live 只读行情验证；不被 test/check/lint 自动触发。各在线、离线和有状态调试范围见 [验证规范](verification.md)。

test-public-time-online 单独验证已构建本地镜像的 live 公共取时；缺镜像直接失败，不自动构建。临时容器使用 local 场景与只读原配置，仅通过鉴权访问取时路由，不启动交易 SDK 或生产后台，不发布端口、不上传。

check 执行类型检查，lint 执行静态规则检查；两者不自动改写文件。fmt 显式格式化，fix 显式执行 lint 修复，不与检查或启动合并。

## 专用操作

ctp-assessment 的 Nix/GUI 环境准备在 [ctp_assessment.sh](../../scripts/ctp_assessment.sh)，Just 只转发参数；原有依赖隔离和 GUI 行为不变。update-ctp 仍为显式的升级、构建与验证流程，不被 sync 或 serve 隐式调用。

```bash
just ctp-assessment --config=config.toml --mode=sandbox
just debug-cleanup-sandbox
```

debug-cleanup-sandbox 沿用既有调试脚本的模拟盘撤单和平仓范围，具有真实副作用；原裸 cleanup 名称退出。其他交易调试、Telegram 发送及数据清理入口仍须明确调用，命令规范调整不扩大其授权或账户范围。

宿主开发任务保持独立，前台 serve 不持有部署队列锁；容器操作的锁和停止协调由部署模块统一管理。旧 image-build、container-start 和裸 deploy 自动部署行为退出，不维护并行公开入口。
