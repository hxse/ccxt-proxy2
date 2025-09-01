from fastapi import APIRouter, Depends, HTTPException
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import os

# 从主应用中导入你的交易所实例和验证函数
from src.tools.shared import verify_token, STATIC_DIR

# 创建 APIRouter 实例
downloader_router = APIRouter(
    prefix="/download",  # 设置所有路由的前缀，例如 /ccxt/balance
    dependencies=[Depends(verify_token)],  # 设置全局依赖项
)

# 文件服务器（暴露静态文件，可以 fetch 下载）
Path(STATIC_DIR).mkdir(exist_ok=True)
downloader_router.mount(
    "/files", StaticFiles(directory=STATIC_DIR, html=False), name="files"
)


@downloader_router.get("/list_files")
def list_files():
    try:
        files = [
            f
            for f in os.listdir(STATIC_DIR)
            if os.path.isfile(os.path.join(STATIC_DIR, f))
        ]
        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
