# CCXT-Proxy2 Justfile
# 使用 `just` 命令运行常用开发任务

bru_user := `uv run --no-sync python scripts/bru_credentials.py user`
bru_password := `uv run --no-sync python scripts/bru_credentials.py password`

export BRU_USER := bru_user
export BRU_PASSWORD := bru_password

# 列出所有可用命令
default:
    @just --list

# ==================== 调试工具 (Debug Tools) ====================

# 运行 debug 目录下的脚本
# 例: just debug cleanup
debug name:
    uv run --no-sync python debug/{{name}}.py

# 运行 debug/route_tests 下的单个 pytest 文件
# 例: just debug-route-test test_order_routes
debug-route-test name:
    CCXT_STATEFUL_DEBUG=1 uv run --no-sync pytest -v -ra debug/route_tests/{{name}}.py

# 运行单个交易调试动作，默认 binance/future/sandbox/BTC/USDT:USDT
# 例: just debug-trade open-long --amount 0.005
debug-trade action *args:
    uv run --no-sync python debug/trade_action.py {{action}} {{args}}

# 运行任意 Python 脚本
run path:
    uv run --no-sync python {{path}}

# 本地启动 API 服务，供 Bruno/curl 调试
serve host="127.0.0.1" port="5123":
    uv run uvicorn src.main:app --host "{{host}}" --port "{{port}}" --reload

# 1. 通过 CcxtClient 清理已启用的 Binance/Kraken Futures sandbox
cleanup:
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
debug-tq-ohlcv symbol duration_seconds="60" data_length="10000":
    uv run --no-sync python debug/tq_probe.py ohlcv --symbol "{{symbol}}" --duration-seconds "{{duration_seconds}}" --data-length "{{data_length}}"

# 调试 TQ Tick 薄转发
debug-tq-tick symbol data_length="10000":
    uv run --no-sync python debug/tq_probe.py tick --symbol "{{symbol}}" --data-length "{{data_length}}"

# 调试 TQ 主连当前标的和历史映射
debug-tq-underlying symbol n="":
    uv run --no-sync python debug/tq_probe.py underlying --symbol "{{symbol}}" --n "{{n}}"

# 发送 Telegram 测试消息，需要服务端已配置 telegram
debug-telegram-send chat text:
    uv run --no-sync python debug/telegram_probe.py --chat "{{chat}}" --text "{{text}}"

# 13. 运行全部 route tests
debug-route-tests:
    CCXT_STATEFUL_DEBUG=1 uv run --no-sync pytest -v -ra debug/route_tests

# 14. 生成 route test 报告
debug-route-report:
    CCXT_STATEFUL_DEBUG=1 uv run --no-sync python debug/route_tests/run_tests.py

# 15. 查余额
debug-balance:
    just debug-trade balance

# 16. 查持仓
debug-positions:
    just debug-trade positions

# 17. 查挂单
debug-open-orders:
    just debug-trade open-orders

# 18. 撤掉当前 symbol 全部挂单
debug-cancel-all:
    just debug-trade cancel-all

# 19. 市价开多
debug-open-long amount="0.005":
    just debug-trade open-long --amount {{amount}}

# 20. 市价开空
debug-open-short amount="0.005":
    just debug-trade open-short --amount {{amount}}

# 21. 平仓，可选 side=long/short，不传则全平
debug-close side="":
    just debug-trade close-position --side "{{side}}"

# 22. 给多仓挂止损，触发后 sell reduceOnly
debug-stop-loss-long trigger amount="0.005":
    just debug-trade stop-loss-long --amount {{amount}} --trigger-price {{trigger}}

# 23. 给空仓挂止损，触发后 buy reduceOnly
debug-stop-loss-short trigger amount="0.005":
    just debug-trade stop-loss-short --amount {{amount}} --trigger-price {{trigger}}

# 24. 给多仓挂止盈，触发后 sell reduceOnly
debug-take-profit-long trigger amount="0.005":
    just debug-trade take-profit-long --amount {{amount}} --trigger-price {{trigger}}

# 25. 给空仓挂止盈，触发后 buy reduceOnly
debug-take-profit-short trigger amount="0.005":
    just debug-trade take-profit-short --amount {{amount}} --trigger-price {{trigger}}

# 26. 设置杠杆
debug-set-leverage leverage:
    just debug-trade set-leverage --leverage {{leverage}}

# 27. 设置保证金模式 cross/isolated
debug-set-margin-mode mode:
    just debug-trade set-margin-mode --margin-mode {{mode}}

# 28. 调试所有常用项 (按顺序运行)
debug-all:
    just cleanup
    just debug-order
    just debug-precision
    just debug-leverage

# ==================== Docker ====================

docker-up-local:
    docker compose up -d --build

docker-down-local:
    docker compose down

docker-wait-ready:
    #!/usr/bin/env bash
    set -euo pipefail
    for i in {1..60}; do
      if curl --silent --fail http://127.0.0.1:5123/readyz >/dev/null; then
        exit 0
      fi
      sleep 1
    done
    echo "Service did not become ready on http://127.0.0.1:5123/readyz within 60s" >&2
    exit 1

# ==================== Bruno CLI ====================

# 运行单个 Bruno 请求或单个文件夹
# 例: just bru-run Root.bru
# 例: just bru-run 'CCXT PROXY/fetch_balance/binance.bru'
bru-run path:
    cd bruno && bru run "{{path}}" --env-file environments/ccxt-proxy2.bru --env-var user="$BRU_USER" --env-var password="$BRU_PASSWORD" --reporter-skip-all-headers --noproxy

# 只跑基础只读请求
bru-readonly-basic:
    cd bruno && bru run Root.bru Ready.bru 'CCXT PROXY/fetch_ohlcv/fetch_ohlcv_latest_limit/binance.bru' 'CCXT PROXY/fetch_ohlcv/fetch_ohlcv_latest_limit/kraken.bru' 'CCXT PROXY/fetch_balance/binance.bru' 'CCXT PROXY/fetch_market_info/binance.bru' 'CCXT PROXY/fetch_positions/binance.bru' --env-file environments/ccxt-proxy2.bru --env-var user="$BRU_USER" --env-var password="$BRU_PASSWORD" --reporter-skip-all-headers --noproxy

# 验证不会访问交易接口的稳定 CCXT error contract
bru-error-contract:
    cd bruno && bru run 'CCXT PROXY/error_contract' --env-file environments/ccxt-proxy2.bru --env-var user="$BRU_USER" --env-var password="$BRU_PASSWORD" --reporter-skip-all-headers --noproxy

# 只跑 TQ 只读请求，需要服务端已配置 tq
bru-tq-readonly:
    cd bruno && bru run 'TQ DATA/fetch_ohlcv/main-cont.bru' 'TQ DATA/fetch_tick/main-cont.bru' 'TQ DATA/fetch_underlying_symbol/main-cont.bru' --env-file environments/ccxt-proxy2.bru --env-var user="$BRU_USER" --env-var password="$BRU_PASSWORD" --reporter-skip-all-headers --noproxy

# 手动发送 Telegram 消息，需要服务端已配置 telegram
bru-telegram-send:
    cd bruno && bru run 'TELEGRAM/send_message/main.bru' --env-file environments/ccxt-proxy2.bru --env-var user="$BRU_USER" --env-var password="$BRU_PASSWORD" --reporter-skip-all-headers --noproxy

# ==================== 代码质量 ====================

test *args:
    uv run --no-sync pytest Test --ignore=Test/online {{args}}

# 聚合运行只读 online tests；不会下单、改设置或发送消息
test-online *args:
    CCXT_ONLINE=1 TQ_ONLINE=1 uv run --no-sync pytest -o addopts= Test/online/test_ccxt_online.py Test/online/test_tq_online.py {{args}}

test-file path *args:
    uv run --no-sync pytest "{{path}}" {{args}}

test-tq-offline:
    uv run --no-sync pytest -v -ra Test/test_tq_*.py

test-tq-online:
    TQ_ONLINE=1 uv run --no-sync pytest -o addopts= -v -ra -s Test/online/test_tq_online.py

test-ccxt-online:
    CCXT_ONLINE=1 uv run --no-sync pytest -o addopts= -v -ra -s Test/online/test_ccxt_online.py

test-telegram-offline:
    uv run --no-sync pytest -v -ra Test/test_telegram_*.py

# Telegram 会真实发送消息，只能通过 stateful debug 入口显式执行
debug-telegram-stateful:
    TELEGRAM_STATEFUL_DEBUG=1 uv run --no-sync pytest -o addopts= -v -ra -s debug/test_telegram_stateful.py

fmt:
    uvx ruff format .

lint:
    uvx ruff check --select E4,E7,E9,F,I src Test

fix:
    uvx ruff check --select E4,E7,E9,F,I --fix src Test

check:
    uvx ty check
