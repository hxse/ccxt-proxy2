# CCXT-Proxy2 Justfile
# 使用 `just` 命令运行常用开发任务

# 设置 Powershell 为默认 shell
set shell := ["powershell.exe", "-c"]


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

# ==================== 代码质量 ====================

fmt:
    uvx ruff format .

lint:
    uvx ruff check .

fix:
    uvx ruff check --fix .

check:
    uvx ty check
