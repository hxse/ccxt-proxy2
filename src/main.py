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
from fastapi.staticfiles import StaticFiles


from src.router.trader_router import ccxt_router
from src.router.demo_router import demo_router
from src.router.downloader_router import downloader_router

app.include_router(ccxt_router)
app.include_router(demo_router)
app.include_router(downloader_router)

app.mount("/static", StaticFiles(directory="src/router/static"), name="static")


@app.get("/", response_class=HTMLResponse)
def root():
    return """
    <html>
    <head><title>主页</title></head>
    <body>
        <h1>API 测试主页</h1>
        <a href="/test/data">测试数据 API</a><br>
        <a href="/test/ccxt">测试 CCXT API</a>
    </body>
    </html>
    """
