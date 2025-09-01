from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from src.router.html import demo_ccxt_html, demo_data_html

# 创建 APIRouter 实例
demo_router = APIRouter(prefix="/test")


# 路由：简单前端按钮测试 API
@demo_router.get("/data", response_class=HTMLResponse)
def test_data():
    default_token = "default_token"
    _demo_data_html = demo_data_html.format(default_token)
    return HTMLResponse(content=_demo_data_html)


# 路由：简单前端按钮测试 ccxt API
@demo_router.get("/ccxt", response_class=HTMLResponse)
def test_ccxt():
    your_ccxt_token_here = "your_ccxt_token_here"
    _demo_ccxt_html = demo_ccxt_html.format(your_ccxt_token_here)
    return HTMLResponse(content=_demo_ccxt_html)
