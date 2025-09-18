import shutil
from pathlib import Path
from typing import List

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import FileResponse

from src.tools.shared import config, STRATEGY_DIR
from src.router.auth_handler import manager


# 创建文件处理路由，并添加鉴权依赖
file_router = APIRouter(
    prefix="/file", dependencies=[Depends(manager)], tags=["File Management"]
)


BASE_DIR = STRATEGY_DIR.resolve()


@file_router.get("/list")
async def list_files():
    """
    返回所有文件的列表。
    """
    file_list = []
    # 使用 rglob 递归地找到所有文件
    for file_path in BASE_DIR.rglob("*"):
        if file_path.is_file():
            # 获取相对于基准目录的路径
            relative_path = file_path.relative_to(BASE_DIR)

            # 提取文件名
            filename = relative_path.name

            # 提取文件所在路径，并用 / 替换 \
            path_dir = str(relative_path.parent).replace("\\", "/")

            # 如果文件在根目录，路径为空字符串
            if path_dir == ".":
                path_dir = ""

            file_list.append({"filename": filename, "path": path_dir})

    if not file_list:
        return {"message": "No files found."}
    return {"files": file_list}


@file_router.post("/upload")
async def upload_file(path: str = Form(default=""), file: UploadFile = File(...)):
    """
    上传单个文件并保存到服务器。支持任意文件类型和相对路径。
    """
    # 结合相对路径和文件名，并进行标准化
    safe_path = (BASE_DIR / path / file.filename).resolve()

    # --- 安全检查：验证文件路径是否在 BASE_DIR 目录下 ---
    if not safe_path.is_relative_to(BASE_DIR):
        raise HTTPException(
            status_code=400,
            detail="Relative paths are not allowed outside the designated directory.",
        )

    # 确保目标文件夹存在
    safe_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(safe_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return {
            "filename": str(safe_path.relative_to(BASE_DIR)),
            "message": "file uploaded successfully.",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {e}")


@file_router.get("/download")
async def download_file(path: str = "", filename: str = ""):
    """
    根据路径和文件名下载单个文件。支持任意文件类型和相对路径。
    """
    if not filename:
        raise HTTPException(status_code=400, detail="Filename is required.")

    # --- 安全检查：验证文件路径是否在 BASE_DIR 目录下 ---
    file_path = (BASE_DIR / path / filename).resolve()
    if not file_path.is_relative_to(BASE_DIR):
        raise HTTPException(
            status_code=400,
            detail="Relative paths are not allowed outside the designated directory.",
        )

    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found.")

    return FileResponse(
        path=file_path,
        filename=file_path.name,
        media_type="application/octet-stream",
    )
