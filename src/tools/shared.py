import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from time import perf_counter
from typing import Any, cast
from uuid import uuid4

import ccxt
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import ValidationError

from src.domain_errors import DomainError
from src.router.logging_utils import INTERNAL_SERVER_ERROR_DETAIL
from src.tools.ccxt_errors import map_ccxt_exception
from src.tools.config_types import AppConfig
from src.tools.exchange_manager import exchange_manager
from src.tools.logging_config import setup_logging

setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    start = perf_counter()
    app.state.exchange_registry_ready = False
    app.state.exchange_registry_initialized = []
    logger.info(
        "initializing exchange registry for {} whitelist entries",
        len(config.exchange_whitelist),
    )
    try:
        exchange_manager.init_from_config(config)
    except Exception:
        duration_ms = (perf_counter() - start) * 1000
        logger.bind(duration_ms=round(duration_ms, 2)).exception(
            "exchange registry initialization failed"
        )
        raise

    duration_ms = (perf_counter() - start) * 1000
    initialized = [
        f"{item.exchange}/{item.market}/{item.mode}"
        for item in config.exchange_whitelist
    ]
    app.state.exchange_registry_ready = True
    app.state.exchange_registry_initialized = initialized
    logger.bind(
        duration_ms=round(duration_ms, 2),
        initialized=initialized,
    ).info("exchange registry initialization completed")
    try:
        yield
    finally:
        app.state.exchange_registry_ready = False
        from src.tools.telegram_manager import telegram_manager
        from src.tools.tq_manager import tq_manager

        resources = (
            ("telegram", telegram_manager.close),
            ("tq", tq_manager.close),
            ("ccxt", exchange_manager.close),
        )
        for resource, close in resources:
            try:
                close()
            except Exception:
                logger.bind(resource=resource).exception(
                    "application resource shutdown failed"
                )


OPENAPI_TAGS = [
    {"name": "General", "description": "服务首页与基础访问入口。"},
    {"name": "Health", "description": "区分进程存活与 Provider registry 就绪状态。"},
    {"name": "Auth", "description": "OAuth2 Password Grant 与 Bearer JWT。"},
    {
        "name": "CCXT PROXY",
        "description": "Binance/Kraken 行情、账户、订单与 OHLCV 代理。",
    },
    {
        "name": "TQ DATA",
        "description": "TqSdk 实时序列与主连标的的 thin-forward 查询。",
    },
    {"name": "TELEGRAM", "description": "向配置的 chat aliases 发送 Telegram 消息。"},
    {
        "name": "File Management",
        "description": "受 strategy 根目录约束的文件列出、上传和下载。",
    },
    {"name": "Documentation", "description": "项目自带的交互式 API 文档页面。"},
]

app = FastAPI(
    title="ccxt-proxy2",
    version="1.0.0",
    description=(
        "带 Bearer 鉴权的 CCXT、TQ 和 Telegram 代理。"
        "交易/设置类 POST 路由具有真实外部副作用。"
    ),
    openapi_tags=OPENAPI_TAGS,
    lifespan=lifespan,
)

# 添加 CORS 中间件
app.add_middleware(
    cast(Any, CORSMiddleware),
    allow_origins=["http://localhost:5173"],  # 允许的源
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有 HTTP 方法
    allow_headers=["*"],  # 允许所有 HTTP 头
)


@app.middleware("http")
async def add_request_context(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or str(uuid4())
    request.state.request_id = request_id
    start = perf_counter()
    client_ip = request.client.host if request.client else "-"

    with logger.contextualize(request_id=request_id):
        response = await call_next(request)
        duration_ms = (perf_counter() - start) * 1000
        log_level = "INFO"
        if response.status_code >= 500:
            log_level = "ERROR"
        elif response.status_code >= 400:
            log_level = "WARNING"

        logger.bind(
            method=request.method,
            path=request.url.path,
            client_ip=client_ip,
            status_code=response.status_code,
            duration_ms=round(duration_ms, 2),
        ).log(log_level, "request completed")

        response.headers["X-Request-ID"] = request_id
        return response


@app.exception_handler(HTTPException)
async def handle_http_exception(request: Request, exc: HTTPException):
    request_id = getattr(request.state, "request_id", "-")
    client_ip = request.client.host if request.client else "-"

    with logger.contextualize(request_id=request_id):
        if exc.status_code >= 500 and exc.detail != INTERNAL_SERVER_ERROR_DETAIL:
            logger.bind(
                method=request.method,
                path=request.url.path,
                client_ip=client_ip,
                status_code=exc.status_code,
            ).error("http exception: {}", exc.detail)
        elif exc.status_code >= 400:
            logger.bind(
                method=request.method,
                path=request.url.path,
                client_ip=client_ip,
                status_code=exc.status_code,
            ).warning("http exception: {}", exc.detail)

    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(DomainError)
async def handle_domain_error(request: Request, exc: DomainError):
    request_id = getattr(request.state, "request_id", "-")
    with logger.contextualize(request_id=request_id):
        logger.bind(
            method=request.method,
            path=request.url.path,
            status_code=exc.status_code,
            error_code=exc.code,
        ).log("ERROR" if exc.status_code >= 500 else "WARNING", "domain error")
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(ccxt.BaseError)
async def handle_ccxt_exception(request: Request, exc: ccxt.BaseError):
    mapped = map_ccxt_exception(exc)
    request_id = getattr(request.state, "request_id", "-")
    with logger.contextualize(request_id=request_id):
        logger.bind(
            method=request.method,
            path=request.url.path,
            status_code=mapped.status_code,
            error_code=mapped.code,
            ccxt_error_type=type(exc).__name__,
        ).error("ccxt provider error")
    return JSONResponse(
        status_code=mapped.status_code,
        content={"detail": mapped.detail},
    )


@app.exception_handler(Exception)
async def handle_unexpected_exception(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", "-")
    client_ip = request.client.host if request.client else "-"

    with logger.contextualize(request_id=request_id):
        logger.bind(
            method=request.method,
            path=request.url.path,
            client_ip=client_ip,
            exception_type=type(exc).__name__,
        ).exception("unhandled application exception")

    return JSONResponse(
        status_code=500,
        content={"detail": INTERNAL_SERVER_ERROR_DETAIL},
    )


STRATEGY_DIR = Path("./data/strategy")
STRATEGY_DIR.mkdir(exist_ok=True)


config_path = Path(os.getenv("CCXT_PROXY_CONFIG_PATH", "./data/config.json"))
config: AppConfig
try:
    with config_path.open("r", encoding="utf-8") as file:
        config = AppConfig.model_validate(json.load(file))
except FileNotFoundError:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w", encoding="utf-8") as file:
        json.dump({}, file, ensure_ascii=False, indent=4)
    logger.error(
        "config.json not found, created empty placeholder file at {}", config_path
    )
    raise RuntimeError("config.json not found")
except json.JSONDecodeError:
    logger.error("config.json is invalid JSON")
    raise RuntimeError("config.json is invalid JSON")
except ValidationError as exc:
    logger.bind(
        validation_errors=exc.errors(include_url=False, include_input=False)
    ).error("config validation failed")
    raise RuntimeError("config validation failed") from exc
except Exception:
    logger.exception("unexpected error while loading config")
    raise


# 缓存目录和文件暴露目录（Docker 卷映射）
CACHE_DIR = "./data/cache"
STATIC_DIR = "./data/static"

app.state.exchange_registry_ready = False
app.state.exchange_registry_initialized = []
