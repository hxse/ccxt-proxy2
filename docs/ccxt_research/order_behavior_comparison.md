# 交易所订单行为对比研究 (Binance vs Kraken)

## 1. 概述

本研究旨在对比 **Binance Futures** (USDⓈ-M) 和 **Kraken Futures** 在订单管理方面的 API 行为差异，并映射到 `ccxt-proxy` 的路由层表现。

研究基于 `ccxt` 库的沙盒环境实测数据。

---

## 2. CCXT API 行为对比

此部分描述 `ccxt` 库原生方法的行为差异。

### 2.1 API: `fetch_orders`

*   **Binance**:
    *   `fetch_orders()`: 仅返回普通订单历史。
    *   `fetch_orders(params={'stop': True})`: 仅返回条件订单历史。
    *   **结论**: 互斥。

*   **Kraken**:
    *   **统一** (推测)。

### 2.2 API: `fetch_open_orders`

*   **Binance**:
    *   `fetch_open_orders()`: **仅返回普通订单** (Limit, Market)。
    *   `fetch_open_orders(params={'stop': True})`: **仅返回条件订单** (Stop Market, Take Profit)。
    *   **结论**: 互斥。要获取完整列表，必须分别请求并合并。

*   **Kraken**:
    *   `fetch_open_orders()`: **返回所有订单** (包含 Limit 和 Stop)。
    *   **结论**: 统一。无需特殊处理。

### 2.3 API: `fetch_closed_orders`

*   **Binance**:
    *   `fetch_closed_orders()`: 仅返回普通已结订单。
    *   `fetch_closed_orders(params={'stop': True})`: 仅返回条件已结订单。
    *   **结论**: 互斥。

*   **Kraken**:
    *   **统一** (推测)。

### 2.4 API: `fetch_order`

*   **Binance**:
    *   `fetch_order(id)`: 仅能查询普通订单。若 ID 为条件订单，报错 `Order does not exist`。
    *   `fetch_order(id, params={'stop': True})`: 修改参数后可查询条件订单。
    *   **结论**: 需区分类型。

*   **Kraken**:
    *   `fetch_order(id)`: **统一**。

### 2.5 API: `cancel_order`

*   **Binance**:
    *   `cancel_order(id)`: 仅能取消普通订单。若 ID 为条件订单，报错 `Unknown order sent`。
    *   `cancel_order(id, params={'stop': True})`: 修改参数后可取消条件订单。
    *   **结论**: 需区分类型。

*   **Kraken**:
    *   `cancel_order(id)`: **统一**。无论何种类型均可取消。

### 2.4 API: `cancel_all_orders`

*   **Binance**:
    *   `cancel_all_orders()`: **仅取消普通订单**。条件单不受影响。
    *   `cancel_all_orders(params={'stop': True})`: **仅取消条件订单**。
    *   **结论**: 互斥。

*   **Kraken**:
    *   `cancel_all_orders()`: **同时取消普通订单和条件订单**。
    *   **结论**: 统一。

---

## 3. Proxy 路由层行为对比

此部分描述 `ccxt-proxy` 各个路由在当前（未修复）状态下的表现及所需修复。

### 3.1 Route: `GET /ccxt/fetch_open_orders`
*(映射 API: `fetch_open_orders`)*

*   **Binance**: 目前无法一次性获取完整挂单列表。如果策略依赖此接口获取所有挂单，会漏掉止损单。
*   **Kraken**: 正常工作，返回所有挂单。
*   **所需修复**: 检测 Binance，执行双重请求并合并。

### 3.2 Route: `GET /ccxt/fetch_closed_orders`
*(映射 API: `fetch_closed_orders`)*

*   **Binance**: 目前无法查询已触发/取消的止损单历史。
*   **Kraken**: 正常工作（需网络正常）。
*   **所需修复**: 检测 Binance，执行双重请求并合并。

### 3.3 Route: `POST /ccxt/cancel_all_orders`
*(映射 API: `cancel_all_orders`)*

*   **Binance**: 调用后，账户内可能仍残留止损单。**极大安全隐患**。
*   **Kraken**: 正常清空。
*   **所需修复**: 检测 Binance，连续执行两次取消逻辑。

### 3.4 Route: `POST /ccxt/cancel_order`
*(映射 API: `cancel_order`)*

*   **Binance**: 如果传入的是止损单 ID，请求会失败。前端必须知道订单类型并传参（目前路由不支持传参）。
*   **Kraken**: 正常工作。
*   **所需修复**: 后端捕获失败，自动带参数重试。

### 3.5 Route: `GET /ccxt/fetch_order`
*(映射 API: `fetch_order`)*

*   **Binance**: 无法通过 ID 查询止损单详情。
*   **Kraken**: 正常工作。
*   **所需修复**: 后端捕获失败，自动带参数重试。

### 3.6 Route: `POST /ccxt/close_position`
*(映射 API: `fetch_positions`, `create_order`)*

*   **Behavior**: 仅平掉 **指定 Symbol** 的当前持仓 (Close Position)。
*   **差异 (Binance vs Kraken)**:
    *   **Binance**: 仅平仓，**不取消挂单**。用户需自行调用 `cancel_all_orders`。
    *   **Kraken**: 平仓操作（Market ReduceOnly）**会自动触发交易所取消该方向的条件单** (Stop Orders)。此为交易所默认行为。
    *   **Note**: 路由层逻辑保持单纯（仅平仓）。额外清理工作留给用户或上层逻辑。
    *   **结论**: 无需修复。

---

## 4. 结论与修复状态

Binance Futures 的 API 隔离设计导致了 CCXT 的行为分裂。为了保证 `ccxt-proxy` 提供统一的、与 Kraken 一致的“标准行为”体验，**必须**在后端对上述 5 个路由（3.1-3.5）进行针对性封装。

**修复状态 (2025-01-13):**
上述 3.1 至 3.5 的修复逻辑已在 `src/tools/binance_adapter.py` 中完全实现，并集成到 `src/tools/ccxt_utils.py` 和 `src/tools/ccxt_utils_extended.py` 中。`close_all_positions` (3.6) 保持原样，符合设计预期。

## 5. 已知限制 (Known Limitations)

### 5.1 Binance Sandbox 环境限制

在 **Binance Futures Sandbox (Testnet)** 环境中进行开发和测试时，发现以下严重限制：

*   **历史订单索引严重迟滞 (Historical Data Indexing Lag)**
    *   **现象**: 当你取消一个订单（无论是 Limit 还是 Stop），其状态在系统后端确实已更新为 Canceled（可通过 `fetch_order(id)` 验证）。但是，该订单**不会立即出现**在 `fetch_closed_orders` 返回的历史列表中。
    *   **严重性**: **极高**。实测即使人为等待 >30 秒，API 依然返回空列表。
    *   **影响**: 依赖“撤单后立即核对已结历史”的自动化测试脚本（如 `debug/route_tests/test_order_routes.py`）会在 Sandbox 环境下必定失败。
    *   **结论**: 这是 Sandbox 环境特有的后端数据同步问题。在实盘（Live）环境中，通常能在数秒内同步完毕。**此问题不代表 `ccxt-proxy` 代码逻辑有误**。
