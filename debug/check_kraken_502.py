import sys
import os
import ccxt

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from debug.utils import get_kraken_sandbox, print_json


def log_to_file(msg):
    print(msg)
    # 将日志追加到文件，方便查看完整 HTML 报错
    with open("debug_log_sandbox.txt", "a", encoding="utf-8") as f:
        f.write(msg + "\n")


def check_kraken_sandbox_502():
    """
    测试 Kraken Futures Sandbox 的 502 Bad Gateway 问题

    注意:
    1. 这是 Sandbox 环境测试，安全。
    2. Kraken Futures Sandbox 的历史订单接口 (fetch_closed_orders) 经常返回 502。
    3. 这通常是服务端问题，与代码逻辑无关。
    4. 实盘环境 (Live) 通常是正常的。
    """

    # Clear log file
    with open("debug_log_sandbox.txt", "w", encoding="utf-8") as f:
        f.write("Starting Sandbox Check...\n")

    try:
        log_to_file("Initializing Kraken Futures Sandbox...")
        exchange = get_kraken_sandbox("future")
        # exchange.verbose = True # 可以开启以查看请求 URL

        symbol = "BTC/USD:USD"

        # 1. Test Public API
        try:
            ticker = exchange.fetch_ticker(symbol)
            log_to_file(f"[Test 1] Fetching ticker: SUCCESS. Last={ticker.get('last')}")
        except Exception as e:
            log_to_file(f"[Test 1] Fetching ticker: FAILED. {e}")

        # 2. Test Open Orders
        try:
            open_orders = exchange.fetch_open_orders(symbol)
            log_to_file(
                f"[Test 2] Fetching open orders: SUCCESS. Count={len(open_orders)}"
            )
        except Exception as e:
            log_to_file(f"[Test 2] Fetching open orders: FAILED. {str(e)[:200]}")

        # 3. Test Closed Orders (Known Issue in Sandbox)
        try:
            log_to_file(f"[Test 3] Fetching closed orders (limit=3)...")
            log_to_file(
                "NOTE: This endpoint often fails with 502 Bad Gateway in Sandbox."
            )

            orders = exchange.fetch_closed_orders(symbol=symbol, limit=3)
            log_to_file(
                f"[Test 3] Fetching closed orders: SUCCESS. Count={len(orders)}"
            )
            if orders:
                print_json(orders[0])
        except ccxt.ExchangeError as e:
            log_to_file(
                f"[Test 3] Fetching closed orders: FAILED (ExchangeError/502 Expected)."
            )
            log_to_file(f"Error Details: {str(e)[:500]}...")  # 截断 HTML 输出
        except Exception as e:
            log_to_file(
                f"[Test 3] Fetching closed orders: FAILED ({type(e).__name__}). {str(e)[:200]}"
            )

    except Exception as e:
        log_to_file(f"CRITICAL: {e}")


if __name__ == "__main__":
    check_kraken_sandbox_502()
