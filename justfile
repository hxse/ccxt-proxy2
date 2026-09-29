# CCXT-Proxy2 Justfile
# 使用 `just` 命令运行常用开发任务

# 源码、测试和辅助入口明确选择开发场景；部署入口按 target 单独选择。
export CCXT_PROXY_PROFILE := "dev"

# 列出所有可用命令
default:
    @just --list

# ==================== 调试工具 (Debug Tools) ====================

# 运行 debug 目录下的脚本
# 例: just debug check_precision
[positional-arguments]
debug name:
    uv run --no-sync python debug/"$1".py

# 运行 debug/route_tests 下的单个 pytest 文件
# 例: just debug-route-test test_order_routes
[positional-arguments]
debug-route-test name:
    CCXT_STATEFUL_DEBUG=1 uv run --no-sync pytest -v -ra debug/route_tests/"$1".py

# 运行单个交易调试动作，默认 binance/future/BTC/USDT:USDT；--is-live 必填
# 例: just debug-trade open-long --amount 0.005 --is-live=false
[positional-arguments]
debug-trade action *args:
    uv run --no-sync python debug/trade_action.py "$@"

# 运行任意 Python 脚本
[positional-arguments]
run path:
    uv run --no-sync python "$1"

# 显式同步锁定依赖；CTP 使用 just sync --extra=ctp
[positional-arguments]
sync *args:
    uv sync --locked "$@"

# 宿主源码服务，支持 --config、--host、--port；不隐式同步依赖
[positional-arguments]
serve *args:
    uv run --no-sync python -m scripts.serve "$@"

# 通过正式路由执行一轮 TQ 采集，使用 market_data.toml 计划
market-data-collect:
    uv run --no-sync python scripts/collect_market_data.py

# 按计划删除本地缓存；不能放进只读在线测试
market-data-prune:
    uv run --no-sync python scripts/prune_market_data.py

# 顺序执行采集和清理；会删除超出保留规则的本地缓存
market-data-once:
    uv run --no-sync python scripts/market_data_pipeline.py

# 临时安装完整 VeighNa Trader + 官方 CTP + 风控；--is-live 必填，false 选择 ctp.test
[positional-arguments]
ctp-assessment *args:
    bash scripts/ctp_assessment.sh "$@"

# 跟随 VeighNa 官方稳定版升级交易 API；校验来源、重建补丁、锁定版本并跑离线回归
[positional-arguments]
update-ctp version="latest":
    uv run --no-sync python scripts/build_vnpy_ctp_source.py --upgrade "$1"
    uv lock --upgrade-package vnpy-ctp
    uv sync --locked --extra ctp
    CCXT_PROXY_CONFIG_PATH=Test/fixtures/config.toml uv run --no-sync python -m pytest -q

# 从本地官方源码包验证可重复构建，不修改依赖或配置
[positional-arguments]
verify-ctp-source archive:
    uv run --no-sync python scripts/build_vnpy_ctp_source.py --source "$1" --check

# 1. 通过 CcxtClient 清理已启用的 Binance/Kraken Futures sandbox
debug-cleanup-sandbox:
    just debug cleanup

# 2. 调试下单路由生命周期
debug-order:
    just debug-route-test test_order_routes

# 4. 调试精度
debug-precision:
    just debug check_precision

debug-prec:
    just debug-precision

# 5. 调试完整市场信息字段
debug-market-info:
    just debug check_market_info_full

# 6. 通过 CcxtClient 检查可确认的当前杠杆（unknown 显示 null）
debug-leverage:
    just debug debug_leverage

debug-lev:
    just debug-leverage

# 7. 隔离检查 Kraken sandbox 原生 502 行为（复用生产 registry/factory）
debug-kraken-502:
    just debug check_kraken_502

# 8. 隔离的原生 Provider 订单行为研究（复用生产 registry/factory）
debug-research-orders:
    just debug research_orders

# 9. 研究 close-all 行为
debug-research-close-all:
    just debug research_close_all

# 10. 验证响应模型
debug-verify-response-models:
    just debug verify_response_models

# 12. 验证原始字段
debug-verify-all-fields:
    just debug verify_all_fields

# 调试 TQ K 线薄转发
[positional-arguments]
debug-tq-ohlcv symbol duration_seconds="60" data_length="10000":
    uv run --no-sync python debug/tq_probe.py ohlcv --symbol "$1" --duration-seconds "$2" --data-length "$3"

# 调试 TQ Tick 薄转发
[positional-arguments]
debug-tq-tick symbol data_length="10000":
    uv run --no-sync python debug/tq_probe.py tick --symbol "$1" --data-length "$2"

# 调试 TQ 主连当前标的和历史映射
[positional-arguments]
debug-tq-underlying symbol *args:
    symbol="$1"; shift; exec uv run --no-sync python debug/tq_probe.py underlying --symbol "$symbol" "$@"

# 发送 Telegram 测试消息，需要服务端已配置 telegram
[positional-arguments]
debug-telegram-send chat text:
    uv run --no-sync python debug/telegram_probe.py --chat "$1" --text "$2"

# 13. 运行全部 route tests
debug-route-tests:
    CCXT_STATEFUL_DEBUG=1 uv run --no-sync pytest -v -ra debug/route_tests

# 14. 生成 route test 报告
debug-route-report:
    CCXT_STATEFUL_DEBUG=1 uv run --no-sync python debug/route_tests/run_tests.py

# 15. 查余额
[positional-arguments]
debug-balance *args:
    just debug-trade balance "$@"

# 16. 查持仓
[positional-arguments]
debug-positions *args:
    just debug-trade positions "$@"

# 17. 查挂单
[positional-arguments]
debug-open-orders *args:
    just debug-trade open-orders "$@"

# 18. 撤掉当前 symbol 全部挂单
[positional-arguments]
debug-cancel-all *args:
    just debug-trade cancel-all "$@"

# 19. 市价开多
[positional-arguments]
debug-open-long amount="0.005" *args:
    value="$1"; shift; just debug-trade open-long --amount "$value" "$@"

# 20. 市价开空
[positional-arguments]
debug-open-short amount="0.005" *args:
    value="$1"; shift; just debug-trade open-short --amount "$value" "$@"

# 21. 平仓，可选 side=long/short，不传则全平
[positional-arguments]
debug-close side="" *args:
    value="$1"; shift; just debug-trade close-position --side "$value" "$@"

# 22. 给多仓挂止损，触发后 sell reduceOnly
[positional-arguments]
debug-stop-loss-long trigger amount="0.005" *args:
    trigger="$1"; amount="$2"; shift 2; just debug-trade stop-loss-long --amount "$amount" --trigger-price "$trigger" "$@"

# 23. 给空仓挂止损，触发后 buy reduceOnly
[positional-arguments]
debug-stop-loss-short trigger amount="0.005" *args:
    trigger="$1"; amount="$2"; shift 2; just debug-trade stop-loss-short --amount "$amount" --trigger-price "$trigger" "$@"

# 24. 给多仓挂止盈，触发后 sell reduceOnly
[positional-arguments]
debug-take-profit-long trigger amount="0.005" *args:
    trigger="$1"; amount="$2"; shift 2; just debug-trade take-profit-long --amount "$amount" --trigger-price "$trigger" "$@"

# 25. 给空仓挂止盈，触发后 buy reduceOnly
[positional-arguments]
debug-take-profit-short trigger amount="0.005" *args:
    trigger="$1"; amount="$2"; shift 2; just debug-trade take-profit-short --amount "$amount" --trigger-price "$trigger" "$@"

# 26. 设置杠杆
[positional-arguments]
debug-set-leverage leverage *args:
    value="$1"; shift; just debug-trade set-leverage --leverage "$value" "$@"

# 27. 设置保证金模式 cross/isolated
[positional-arguments]
debug-set-margin-mode mode *args:
    value="$1"; shift; just debug-trade set-margin-mode --margin-mode "$value" "$@"

# 28. 调试所有常用项 (按顺序运行)
debug-all:
    just debug-cleanup-sandbox
    just debug-order
    just debug-precision
    just debug-leverage

# ==================== Podman ====================

# 按目标组合 --upload/--build/--start；启动自动准备 trading-net，控制动作可独立执行
[positional-arguments]
deploy *args:
    uv run --no-sync python -m scripts.container_cli "$@"

# CFB 运维必须显式 --is-live=true|false：--status/--logs/--pause/--resume/--clean/--screenshot。
[positional-arguments]
cfb *args:
    uv run --no-sync python -m scripts.cfb_cli "$@"

# 显式构建 Wine 验证镜像；断网验证原生能力及两个模式的容器隔离，不挂真实账户。
test-cfb-native:
    uv run --no-sync python -m scripts.cfb_native_test

# ==================== Bruno CLI ====================

# 运行单个 Bruno 请求或单个文件夹
# 例: just bru-run Root.bru
# 例: just bru-run 'CCXT PROXY/fetch_balance/binance.bru'
[positional-arguments]
bru-run path:
    uv run --no-sync python scripts/run_bruno.py "$1"

# 只跑基础只读请求
bru-readonly-basic:
    uv run --no-sync python scripts/run_bruno.py Root.bru Ready.bru 'CCXT PROXY/fetch_ohlcv/fetch_ohlcv_latest_limit/binance.bru' 'CCXT PROXY/fetch_ohlcv/fetch_ohlcv_latest_limit/kraken.bru' 'CCXT PROXY/fetch_balance/binance.bru' 'CCXT PROXY/fetch_market_info/binance.bru' 'CCXT PROXY/fetch_positions/binance.bru'

# 验证不会访问交易接口的稳定 CCXT error contract
bru-error-contract:
    uv run --no-sync python scripts/run_bruno.py 'CCXT PROXY/error_contract'

# 公共时间薄转发，需要项目登录账号，不需要交易所账号
bru-public-time:
    uv run --no-sync python scripts/run_bruno.py 'SYSTEM/fetch_time.bru'

# 只跑 CFB 五条 GET，默认 sandbox；模式必须匹配配置所选账户
bru-cfb-readonly:
    uv run --no-sync python scripts/run_bruno.py 'CFB/fetch_orders.bru' 'CFB/fetch_trades.bru' 'CFB/fetch_positions.bru' 'CFB/fetch_balance.bru' 'CFB/fetch_trading_status.bru'

# 只跑 TQ 只读请求，需要服务端已配置 tq
bru-tq-readonly:
    uv run --no-sync python scripts/run_bruno.py 'TQ DATA/fetch_ohlcv/main-cont.bru' 'TQ DATA/fetch_tick/main-cont.bru' 'TQ DATA/fetch_underlying_symbol/main-cont.bru' 'TQ DATA/fetch_trading_calendar/main.bru'

# 手动发送 Telegram 消息，需要服务端已配置 telegram
bru-telegram-send:
    uv run --no-sync python scripts/run_bruno.py 'TELEGRAM/send_message/main.bru'

# ==================== 代码质量 ====================

[positional-arguments]
test *args:
    uv run --no-sync pytest Test --ignore=Test/online --ignore=Test/cfb_native "$@"

# 聚合运行 public live market-data tests；不会检查私有账户或初始化 sandbox
[positional-arguments]
test-online *args:
    CCXT_PROXY_CONFIG_PATH=./config.toml CCXT_ONLINE=1 TQ_ONLINE=1 uv run --no-sync pytest -o addopts= Test/online/test_ccxt_online.py Test/online/test_tq_online.py "$@"

[positional-arguments]
test-file path *args:
    uv run --no-sync pytest "$@"

test-tq-offline:
    uv run --no-sync pytest -v -ra Test/test_tq_*.py

test-tq-online:
    CCXT_PROXY_CONFIG_PATH=./config.toml TQ_ONLINE=1 uv run --no-sync pytest -o addopts= -v -ra -s Test/online/test_tq_online.py

# 只测试 whitelist 中已启用的 Futures live public market data
test-ccxt-online:
    CCXT_PROXY_CONFIG_PATH=./config.toml CCXT_ONLINE=1 uv run --no-sync pytest -o addopts= -v -ra -s Test/online/test_ccxt_online.py

# 先构建本地镜像；隔离容器内只取一次 live 公共时间，不启动交易 SDK 或生产后台。
test-public-time-online:
    CCXT_PROXY_CONFIG_PATH=./config.toml PUBLIC_TIME_ONLINE=1 uv run --no-sync pytest -o addopts= -v -ra -s Test/online/test_public_time_online.py

test-telegram-offline:
    uv run --no-sync pytest -v -ra Test/test_telegram_*.py

# 不连接交易服务；ctp extra 安装后增加原生 SDK 与本机假 TCP 前置的生命周期检查
test-ctp-offline:
    uv run --no-sync pytest -v -ra Test/test_ctp_*.py

# 只读 CTP 查询，默认使用 sandbox；首次连接会认证、登录、确认结算
bru-ctp-readonly:
    uv run --no-sync python scripts/run_bruno.py 'CTP TRADING/fetch_orders.bru' 'CTP TRADING/fetch_trades.bru' 'CTP TRADING/fetch_positions.bru' 'CTP TRADING/fetch_balance.bru' 'CTP TRADING/fetch_trading_status.bru'

# 先使用 SERVICE LIFECYCLE/disabled.example.toml 启动服务；仅 GET，不连接交易服务
bru-service-disabled:
    CCXT_PROXY_CONFIG_PATH='bruno/SERVICE LIFECYCLE/disabled.example.toml' uv run --no-sync python scripts/run_bruno.py 'SERVICE LIFECYCLE/ready.bru' 'SERVICE LIFECYCLE/tq_disabled.bru' 'SERVICE LIFECYCLE/ctp_sandbox_disabled.bru' 'SERVICE LIFECYCLE/ccxt_sandbox_disabled.bru'

# Telegram 会真实发送消息，只能通过 stateful debug 入口显式执行
debug-telegram-stateful:
    TELEGRAM_STATEFUL_DEBUG=1 uv run --no-sync pytest -o addopts= -v -ra -s debug/test_telegram_stateful.py

fmt:
    uvx ruff format .

lint:
    uvx ruff check --select E4,E7,E9,F,I src Test scripts/container_*.py scripts/cfb_*.py scripts/serve.py

fix:
    uvx ruff check --select E4,E7,E9,F,I --fix src Test

check:
    uvx ty check
