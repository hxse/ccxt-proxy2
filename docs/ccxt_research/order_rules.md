# CCXT 下单规则手册

## Part 1: API 字段说明

> 针对 4 种场景: Binance U本位/币本位, Kraken U本位/币本位

### 1.1 交易符号 (Symbol)
| 场景 | Symbol 示例 | API | 说明 |
|:---|:---|:---|:---|
| Binance U本位 | `BTC/USDT:USDT` | `markets` key | 线性合约 |
| Binance 币本位 | `BTC/USD:BTC` | `markets` key | 反向合约 |
| Kraken U本位 | `BTC/USD:USD` | `markets` key | 线性合约 |
| Kraken 币本位 | `BTC/USD:BTC` | `markets` key | 反向合约 |

### 1.2 最小数量 (limits.amount.min)
- **API**: `market['limits']['amount']['min']`
- **注意**: Kraken U本位可能返回 `None`，此时应使用 `precision.amount` 作为最小限制。

### 1.3 下单数量单位 (amount)
| 场景 | 单位 | 说明 |
|:---|:---|:---|
| U本位 | **币 (Coin)** | 想要买多少个币 |
| 币本位 | **张 (Contract)** | 想要买多少张合约 |

### 1.4 精度 (precision.amount)
- **API**: `market['precision']['amount']`
- **说明**: 下单数量必须是精度的整数倍。

### 1.5 合约乘数 (contractSize)
- **API**: `market['contractSize']`
- **Binance 币本位**: 通常 100 USD (BTC), 10 USD (ETH)
- **Kraken 币本位**: 通常 1 USD
- **U本位**: 通常 1.0 (1 contract = 1 coin)

### 1.6 合约类型 (linear/inverse)
- **API**: `market['linear']` (U本位), `market['inverse']` (币本位)


### 1.7 杠杆配置 (Leverage)
- **获取当前杠杆**: `fetch_positions()` → `position['leverage']`
  - 注意: 只有持仓时才能获取准确的当前杠杆值。
- **获取杠杆档位**: `fetch_leverage_tiers(symbol)`
  - 返回该交易对支持的最小/最大杠杆。
- **设置杠杆**: `set_leverage(leverage, symbol)`

---

## Part 2: 下单公式

### 2.1 完整计算流程

```python
def calc_order_amount(market, target_value_usd, price):
    """
    计算符合精度要求的下单数量
    
    Args:
        market: exchange.market(symbol) 返回的市场信息
        target_value_usd: 目标仓位价值 (USD)
        price: 当前价格
    """
    precision = market['precision']['amount']
    
    if market['inverse']:  # 币本位
        raw_amount = target_value_usd / market['contractSize']
    else:  # U本位
        raw_amount = target_value_usd / price
    
    # 关键：精度截断 (向下取整到精度倍数)
    final_amount = int(raw_amount / precision) * precision
    
    return final_amount
```

### 2.2 公式总表 (含精度截断)

| 场景 | 原始计算 | 精度截断 | 最终 amount |
|:---|:---|:---|:---|
| **U本位** | `raw = 目标USD / 价格` | `int(raw / precision) * precision` | 币数量 |
| **币本位** | `raw = 目标USD / contractSize` | `int(raw / precision) * precision` | 合约张数 |

### 2.3 示例计算 (买入 $1000 仓位, BTC=$50000)

| 交易所 | 场景 | contractSize | precision | raw 计算 | 截断后 amount |
|:---|:---|:---|:---|:---|:---|
| Binance | U本位 | 1 | 0.001 | `1000/50000=0.02` | **0.02** |
| Binance | 币本位 | 100 | 1 | `1000/100=10` | **10** |
| Kraken | U本位 | 1 | 0.0001 | `1000/50000=0.02` | **0.02** |
| Kraken | 币本位 | 1 | 1 | `1000/1=1000` | **1000** |

---

## Part 3: 测试报告

> 运行 `just report` 生成最新报告

### 3.1 下单验证 (Order Verification)
- **脚本**: [`debug/debug_order.py`](file:///d:/my_repo/ccxt-proxy2/debug/debug_order.py)
- **复现命令**: `just debug-order`
- **验证结论**:
  - ✅ **Binance U本位**: 下单 `0.005`，余额减少 ~22 USDT (约等于 0.005 BTC 价值)。证明 `amount` 為 **币数量**。
  - ✅ **Binance 币本位**: 下单 `1`，余额减少 ~0.0002 BTC (约等于 100 USD 价值)。证明 `amount` 為 **合约张数**。
  - ✅ **Kraken U本位**: 下单 `0.0002`，成功成交。证明 `amount` 為 **币数量**。
  - ✅ **Kraken 币本位**: 下单 `10`，成功成交。证明 `amount` 為 **合约张数**。

### 3.2 最小数量限制验证 (Min Amt Verification)
- **脚本**: [`debug/debug_min_size.py`](file:///d:/my_repo/ccxt-proxy2/debug/debug_min_size.py)
- **复现命令**: `just debug-min`
- **验证结论**:
  - ✅ **动态获取**: 脚本成功从 `limits.amount.min` 获取最小限制。
  - ✅ **Kraken 特例**: 脚本识别 Kraken 返回 `None`，自动回退使用 `precision.amount` 作为最小值，验证通过。

### 3.3 精度截断验证 (Precision Verification)
- **脚本**: [`debug/debug_precision.py`](file:///d:/my_repo/ccxt-proxy2/debug/debug_precision.py)
- **复现命令**: `just debug-prec`
- **验证结论**:
  - ✅ **Binance**: 传入非精度倍数（如 0.0015 且精度 0.001），API 报错或截断，验证了精度机制生效。

### 3.4 杠杆配置验证 (Leverage Verification)
- **脚本**: [`debug/debug_leverage.py`](file:///d:/my_repo/ccxt-proxy2/debug/debug_leverage.py)
- **复现命令**: `just debug-lev`
- **验证结论**:
  - ✅ **Binance U本位**: `fetch_leverage_tiers` 返回 10 个档位 (Max 125x)。
  - ⚠️ **Binance 币本位**: 沙盒环境未返回 Tiers 数据，可能需生产环境验证。
  - ✅ **Kraken U本位**: `fetch_leverage_tiers` 返回 6 个档位 (Max 50x)。
  - ✅ **Kraken 币本位**: 需使用 `future` 实例访问，返回 1 个合并档位 (Max 50x)。
  - ℹ️ **说明**: 杠杆信息需通过 `fetch_positions()` 获取当前持仓的实际杠杆值。
