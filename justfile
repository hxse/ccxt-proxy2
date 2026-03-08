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
# 例: just debug debug_order
debug name:
    uv run --no-sync python debug/{{name}}.py

# 运行任意 Python 脚本
run path:
    uv run --no-sync python {{path}}

# 1. 完整清理 (取消挂单/平仓)
cleanup:
    just debug cleanup

# 2. 调试下单验证 (含余额检查)
debug-order:
    just debug debug_order

# 3. 调试最小数量
debug-min:
    just debug debug_min_size

# 4. 调试精度
debug-prec:
    just debug debug_precision

# 4. 调试杠杆
debug-lev:
    just debug debug_leverage

# 5. 调试所有 (按顺序运行)
debug-all:
    just cleanup
    just debug-order
    just debug-min
    just debug-prec

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

# 只跑 sandbox 的基础只读测试
bru-sandbox-basic:
    cd bruno && bru run Root.bru 'CCXT PROXY/fetch_balance/binance.bru' 'CCXT PROXY/fetch_market_info/binance.bru' 'CCXT PROXY EXTENDED/fetch_positions/binance.bru' --env-file environments/ccxt-proxy2.bru --env-var user="$BRU_USER" --env-var password="$BRU_PASSWORD" --reporter-skip-all-headers --noproxy

# ==================== 代码质量 ====================

test *args:
    uv run --no-sync pytest Test {{args}}

fmt:
    uvx ruff format .

lint:
    uvx ruff check .

fix:
    uvx ruff check --fix .

check:
    uvx ty check
