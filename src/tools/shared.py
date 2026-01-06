from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import json
from pathlib import Path
from src.tools.exchange_manager import exchange_manager


app = FastAPI()

# 添加 CORS 中间件
app.add_middleware(
    CORSMiddleware,  # type: ignore
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
# 根据白名单初始化交易所实例
exchange_manager.init_from_config(config)
