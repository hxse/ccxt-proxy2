from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
import json
from pathlib import Path
from src.tools.exchange import get_binance_exchange, get_kraken_exchange


app = FastAPI()

# 添加 CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # 允许的源
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有 HTTP 方法
    allow_headers=["*"],  # 允许所有 HTTP 头
)


OHLCV_DIR = Path("./data/ohlcv")
OHLCV_DIR.mkdir(exist_ok=True)


STRATEGY_DIR = Path("./data/strategy")
STRATEGY_DIR.mkdir(exist_ok=True)


json_path = "./data/config.json"
try:
    with open(json_path, "r", encoding="utf-8") as file:
        config = json.load(file)
except FileNotFoundError:
    config = {}
    with open(json_path, "w", encoding="utf-8") as file:
        json.dump(config, file, ensure_ascii=False, indent=4)
    print("错误: 配置文件不存在。")
except json.JSONDecodeError:
    print("错误: 配置文件格式不正确。")
except Exception as e:
    print(f"发生了一个意外错误: {e}")


# 缓存目录和文件暴露目录（Docker 卷映射）
CACHE_DIR = "./data/cache"
STATIC_DIR = "./data/static"


# 创建 sandbox 实例（模拟环境）
binance_exchange_sandbox = get_binance_exchange(config, sandbox=True)
kraken_exchange_sandbox = get_kraken_exchange(config, sandbox=True)

# 创建 live 实例（实盘环境）
binance_exchange_live = get_binance_exchange(config, sandbox=False)
kraken_exchange_live = get_kraken_exchange(config, sandbox=False)

kraken_exchange_sandbox.fetchMarkets()
kraken_exchange_live.fetchMarkets()
