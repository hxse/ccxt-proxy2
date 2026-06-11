import sys
from pathlib import Path
from fastapi import status
from fastapi.responses import JSONResponse
from fastapi.responses import HTMLResponse

root_path = next(
    (p for p in Path(__file__).resolve().parents if (p / "pyproject.toml").is_file()),
    None,
)
if root_path:
    sys.path.insert(0, str(root_path))


from src.tools.shared import app  # noqa: E402
from src.router.trader_router import ccxt_router  # noqa: E402
from src.router.file_handler import file_router  # noqa: E402
from src.router.auth_handler import auth_router  # noqa: E402
from src.router.extended_router import extended_router  # noqa: E402
from src.router.tq_router import tq_router  # noqa: E402
from scalar_fastapi import get_scalar_api_reference  # noqa: E402


app.include_router(auth_router)
app.include_router(ccxt_router)
app.include_router(extended_router)
app.include_router(file_router)
app.include_router(tq_router)


@app.get("/", response_class=HTMLResponse)
def root():
    return """
    <html>
    <head><title>主页</title></head>
    <body>
        hello world
    </body>
    </html>
    """


@app.get("/healthz", tags=["Health"])
def healthz():
    """
    存活检查。

    只要应用进程可以处理请求，就返回 200。
    """
    return {"status": "ok"}


@app.get("/readyz", tags=["Health"])
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


@app.get("/scalar", include_in_schema=False)
async def scalar_html():
    return get_scalar_api_reference(
        title="hello world",
        openapi_url=app.openapi_url,
        # Avoid CORS issues (optional)
        # scalar_proxy_url="https://proxy.scalar.com",
    )
