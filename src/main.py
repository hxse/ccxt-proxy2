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

from src.responses_system import (  # noqa: E402
    HealthResponse,
    NotReadyResponse,
    ReadyResponse,
)
from src.router.auth_handler import auth_router  # noqa: E402
from src.router.cache_router import cache_router  # noqa: E402
from src.router.cfb_router import cfb_router  # noqa: E402
from src.router.ctp_router import ctp_router  # noqa: E402
from src.router.file_handler import file_router  # noqa: E402
from src.router.system_router import system_router  # noqa: E402
from src.router.telegram_router import telegram_router  # noqa: E402
from src.router.tq_router import tq_router  # noqa: E402
from src.router.trader_router import ccxt_router  # noqa: E402
from src.tools.shared import app  # noqa: E402

app.include_router(auth_router)
app.include_router(ccxt_router)
app.include_router(ctp_router)
app.include_router(cfb_router)
app.include_router(file_router)
app.include_router(tq_router)
app.include_router(telegram_router)
app.include_router(system_router)
app.include_router(cache_router)


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
    response_model=HealthResponse,
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
    response_model=ReadyResponse,
    tags=["Health"],
    summary="服务就绪检查",
    description=(
        "检查 HTTP 应用是否可服务，并列出 service_whitelist 各身份的独立初始化状态。"
        "CFB 初始化只建立 IPC 客户端，终端交易就绪由 /cfb/readyz 单独展示。"
        "单个 SDK 初始化中或失败不阻断其他身份和 HTTP；配置只在进程启动时读取。"
        "这是启动就绪检查，不会发起实时网络探测。"
    ),
    response_description="当前启动就绪状态，以及 ccxt/交易所/市场/模式、tq、ctp/模式、cfb 形式的实例列表。",
    responses={
        503: {
            "model": NotReadyResponse,
            "description": "应用正在进入或退出服务生命周期。",
        }
    },
)
def readyz():
    """
    就绪检查。

    基础生命周期已建立即可返回 200；SDK 就绪分别由 services 表示。
    """
    runtime = app.state.service_runtime
    snapshot = runtime.snapshot()
    if snapshot["status"] == "ready":
        return snapshot

    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content=snapshot,
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

