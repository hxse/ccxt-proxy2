from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import json
from pathlib import Path
from src.tools.exchange import get_binance_exchange, get_kraken_exchange


app = FastAPI()


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


binance_exchange = get_binance_exchange(config)
kraken_exchange = get_kraken_exchange(config)

kraken_exchange.fetchMarkets()
