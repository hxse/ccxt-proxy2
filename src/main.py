import sys
from pathlib import Path
from fastapi.responses import HTMLResponse

root_path = next(
    (p for p in Path(__file__).resolve().parents if (p / "pyproject.toml").is_file()),
    None,
)
if root_path:
    sys.path.insert(0, str(root_path))


from src.tools.shared import app
from src.router.trader_router import ccxt_router
from src.router.file_handler import file_router
from scalar_fastapi import get_scalar_api_reference


app.include_router(ccxt_router)
app.include_router(file_router)


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


@app.get("/scalar", include_in_schema=False)
async def scalar_html():
    return get_scalar_api_reference(
        title="hello world",
        openapi_url=app.openapi_url,
        # Avoid CORS issues (optional)
        # scalar_proxy_url="https://proxy.scalar.com",
    )
