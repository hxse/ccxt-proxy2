# Binance vs Kraken 市场数据展示对比

**数据来源**：用户手动复制的实盘网页数据
**采集时间**：2026-01-09

## 1. 数据来源链接

*   **Binance 币本位 (COIN-M)**: [https://www.binance.com/en/delivery/BTCUSD_PERPETUAL](https://www.binance.com/en/delivery/BTCUSD_PERPETUAL)
*   **Binance U本位 (USD-M)**: [https://www.binance.com/en/futures/BTCUSDT](https://www.binance.com/en/futures/BTCUSDT)
*   **Kraken 币本位 (Inverse/CM-Perp)**: [https://pro.kraken.com/app/trade/futures-btc-usd-cm-perp](https://pro.kraken.com/app/trade/futures-btc-usd-cm-perp)
*   **Kraken U本位 (Linear/Perp)**: [https://pro.kraken.com/app/trade/futures-btc-usd-perp](https://pro.kraken.com/app/trade/futures-btc-usd-perp)

---

## 2. 核心发现：交易量级差异巨大

**结论**：
1.  **Binance** 的流动性远超 Kraken（U本位约 45倍，币本位约 3500倍）。
2.  **Kraken 的币本位 (CM-Perp)** 交易量极低（仅 37万 U），几乎没有深度，**建议主力只对接 Kraken 的 U本位 (Linear)。**
3.  **Binance** 的双向流动性都很好。

---

## 3. 详细数据对比表

| 比较项 | Binance U本位 (主力) | Kraken U本位 (主力) | Binance 币本位 | Kraken 币本位 |
| :--- | :--- | :--- | :--- | :--- |
| **网页名称** | `BTCUSDT Perp` | `BTC Perp` | `BTCUSD CM Perp` | `BTC CM-Perp` |
| **推测 API Symbol** | `BTC/USDT:USDT` | `BTC/USD:USD` | `BTC/USD:BTC` | `BTC/USD:BTC` |
| **合约类型** | **Linear** (正向) | **Linear** (正向) | **Inverse** (反向) | **Inverse** (反向) |
| **24h 成交量 (USD)** | **$12,640,603,343** | **$281,000,000** | **$1,290,551,400** | **$372,000** |
| **量级差距** | 基准 | ~1/45 | ~1/10 | **~1/34,000** (忽略不计) |
| **成交量单位** | BTC | BTC & USD | Cont (张) | USD |
| **持仓量 (OI)** | $8.87B | $281M (?) | 18M Cont | $4.2M |
| **资金费率周期** | 8h | 1h | 8h | 1h |

---

## 4. 字段单位细节

### Binance
*   **BTCUSDT (U-M)**:
    *   Volume: 用 **BTC** 显示 (139k BTC ≈ 126亿 U)
    *   OI: 用 **USDT** 显示
*   **BTCUSD (Coin-M)**:
    *   Volume: 用 **Cont (张数)** 显示 (1张=100美元)
    *   OI: 用 **Cont (张数)** 显示

### Kraken
*   **BTC Perp (Linear)**:
    *   Volume: 同时显示 **BTC** 和 **USD**
    *   OI: 显示 **BTC** (网页) / API可能返回 contracts
*   **BTC CM-Perp (Inverse)**:
    *   Volume: 用 **USD** 显示
    *   OI: 用 **USD** 显示

## 5. 建议
鉴于 Kraken 币本位深度极差，建议在策略中：
*   **Kraken**: 仅交易 `BTC/USD:USD` (Linear)。
*   **Binance**: 可交易 `BTC/USDT:USDT` (Linear) 和 `BTC/USD:BTC` (Inverse)。
