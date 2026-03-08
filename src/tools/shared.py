from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi import HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import json
from pathlib import Path
from time import perf_counter
from typing import Any, cast
from uuid import uuid4
from pydantic import ValidationError
from loguru import logger
from src.router.logging_utils import INTERNAL_SERVER_ERROR_DETAIL
from src.tools.exchange_manager import exchange_manager
from src.tools.config_types import AppConfig
from src.tools.logging_config import setup_logging


setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    start = perf_counter()
    app.state.exchange_registry_ready = False
    app.state.exchange_registry_initialized = []
    app.state.exchange_registry_error = None
    logger.info(
        "initializing exchange registry for {} whitelist entries",
        len(config.exchange_whitelist),
    )
    try:
        exchange_manager.init_from_config(config)
    except Exception as exc:
        duration_ms = (perf_counter() - start) * 1000
        app.state.exchange_registry_error = str(exc)
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
    yield


app = FastAPI(lifespan=lifespan)

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


OHLCV_DIR = Path("./data/ohlcv")
OHLCV_DIR.mkdir(exist_ok=True)


STRATEGY_DIR = Path("./data/strategy")
STRATEGY_DIR.mkdir(exist_ok=True)


json_path = "./data/config.json"
config: AppConfig
try:
    with open(json_path, "r", encoding="utf-8") as file:
        config = AppConfig.model_validate(json.load(file))
except FileNotFoundError:
    with open(json_path, "w", encoding="utf-8") as file:
        json.dump({}, file, ensure_ascii=False, indent=4)
    logger.error(
        "config.json not found, created empty placeholder file at {}", json_path
    )
    raise RuntimeError("config.json not found")
except json.JSONDecodeError:
    logger.error("config.json is invalid JSON")
    raise RuntimeError("config.json is invalid JSON")
except ValidationError as exc:
    logger.bind(validation_errors=exc.errors(include_url=False)).error(
        "config validation failed"
    )
    raise RuntimeError("config validation failed") from exc
except Exception:
    logger.exception("unexpected error while loading config")
    raise


# 缓存目录和文件暴露目录（Docker 卷映射）
CACHE_DIR = "./data/cache"
STATIC_DIR = "./data/static"

app.state.exchange_registry_ready = False
app.state.exchange_registry_initialized = []
app.state.exchange_registry_error = None
