from fastapi import APIRouter
from fastapi.responses import HTMLResponse, RedirectResponse

# 创建 APIRouter 实例
demo_router = APIRouter(prefix="/test")


# 路由：简单前端按钮测试 API
@demo_router.get("/data", response_class=HTMLResponse)
def test_data():
    return RedirectResponse(url="/static/demo_data.html")


# 路由：简单前端按钮测试 ccxt API
@demo_router.get("/ccxt", response_class=HTMLResponse)
def test_ccxt():
    return RedirectResponse(url="/static/demo_ccxt.html")
