import sys
from pathlib import Path

from fastapi import status
from fastapi.responses import HTMLResponse, JSONResponse

root_path = next(
    (p for p in Path(__file__).resolve().parents if (p / "pyproject.toml").is_file()),
    None,
)
if root_path:
    sys.path.insert(0, str(root_path))


from scalar_fastapi import get_scalar_api_reference  # noqa: E402

from src.router.auth_handler import auth_router  # noqa: E402
from src.router.file_handler import file_router  # noqa: E402
from src.router.telegram_router import telegram_router  # noqa: E402
from src.router.tq_router import tq_router  # noqa: E402
from src.router.trader_router import ccxt_router  # noqa: E402
from src.tools.shared import app  # noqa: E402

app.include_router(auth_router)
app.include_router(ccxt_router)
app.include_router(file_router)
app.include_router(tq_router)
app.include_router(telegram_router)


@app.get(
    "/",
    response_class=HTMLResponse,
    tags=["General"],
    summary="服务首页",
    description="返回简单 HTML 首页，用于确认 API 服务可以访问。",
    response_description="HTML 首页。",
)
def root():
    return """
    <html>
    <head><title>ccxt-proxy2</title></head>
    <body>
        ccxt-proxy2 API
    </body>
    </html>
    """


@app.get(
    "/healthz",
    tags=["Health"],
    summary="进程存活检查",
    description="只检查 FastAPI 进程能否处理请求，不检查 Provider registry。",
    response_description="固定返回 status=ok。",
)
def healthz():
    """
    存活检查。

    只要应用进程可以处理请求，就返回 200。
    """
    return {"status": "ok"}


@app.get(
    "/readyz",
    tags=["Health"],
    summary="服务就绪检查",
    description=(
        "检查配置加载与 ExchangeManager registry 初始化是否完成。未就绪时返回 "
        "503，并附带初始化状态和已脱敏错误。"
    ),
    response_description="当前就绪状态和已初始化的 exchange identities。",
    responses={503: {"description": "Provider registry 尚未成功初始化。"}},
)
def readyz():
    """
    就绪检查。

    只有配置加载成功且交易所白名单初始化完成后，才返回 200。
    """
    if app.state.exchange_registry_ready:
        return {
            "status": "ready",
            "initialized": app.state.exchange_registry_initialized,
        }

    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "status": "not_ready",
            "initialized": app.state.exchange_registry_initialized,
            "error": app.state.exchange_registry_error,
        },
    )


@app.get(
    "/scalar",
    response_class=HTMLResponse,
    tags=["Documentation"],
    summary="Scalar API 文档页面",
    description="返回基于当前 OpenAPI schema 生成的 Scalar 交互式文档。",
    response_description="Scalar HTML 页面。",
)
async def scalar_html():
    return get_scalar_api_reference(
        title="ccxt-proxy2 API",
        openapi_url=app.openapi_url,
        # Avoid CORS issues (optional)
        # scalar_proxy_url="https://proxy.scalar.com",
    )
