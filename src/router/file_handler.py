import shutil
from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from fastapi.responses import FileResponse
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field

from src.base_types import NonEmptyString
from src.responses_system import (
    EmptyFileListResponse,
    FileListResponse,
    FileUploadResponse,
)
from src.router.auth_handler import manager
from src.router.logging_utils import INTERNAL_SERVER_ERROR_DETAIL
from src.router.query_validation import reject_unknown_query_params
from src.tools.shared import STRATEGY_DIR

# 创建文件处理路由，并添加鉴权依赖
file_router = APIRouter(
    prefix="/file", dependencies=[Depends(manager)], tags=["File Management"]
)


BASE_DIR = STRATEGY_DIR.resolve()


class FileDownloadRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str = Field("", description="相对于 strategy 根目录的父目录")
    filename: NonEmptyString = Field(..., description="要下载的文件名")


@file_router.get(
    "/list",
    response_model=FileListResponse | EmptyFileListResponse,
    summary="列出 strategy 文件",
    description=(
        "递归列出服务端 strategy 根目录内的文件，返回相对 filename/path；"
        "目录为空时返回 No files found。"
    ),
    response_description="文件列表或空目录提示。",
    responses={
        401: {"description": "Bearer token 无效或缺失。"},
        422: {"description": "请求包含未知 query 参数。"},
    },
)
async def list_files(request: Request):
    """
    返回所有文件的列表。
    """
    reject_unknown_query_params(request, set())
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


@file_router.post(
    "/upload",
    response_model=FileUploadResponse,
    summary="上传 strategy 文件",
    description=(
        "通过 multipart/form-data 上传单个文件到 strategy 根目录下的相对 path。"
        "服务端 resolve 最终路径并拒绝 .. 或 symlink 逃逸；同名文件会被覆盖。"
    ),
    response_description="实际保存的相对文件名和成功消息。",
    responses={
        400: {"description": "文件名缺失或目标路径逃逸 strategy 根目录。"},
        401: {"description": "Bearer token 无效或缺失。"},
        422: {"description": "请求包含未知 query 参数。"},
        500: {"description": "文件写入失败，响应不会暴露内部路径。"},
    },
)
async def upload_file(
    request: Request,
    path: str = Form(default=""),
    file: UploadFile = File(...),
):
    """
    上传单个文件并保存到服务器。支持任意文件类型和相对路径。
    """
    reject_unknown_query_params(request, set())
    # 结合相对路径和文件名，并进行标准化
    if file.filename is None:
        raise HTTPException(status_code=400, detail="Filename is missing")
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
    except Exception:
        logger.bind(route="file_upload", path=path, filename=file.filename).exception(
            "file upload failed"
        )
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_DETAIL)


@file_router.get(
    "/download",
    response_class=FileResponse,
    summary="下载 strategy 文件",
    description=(
        "按相对 path 和 filename 下载 strategy 根目录内的文件。服务端拒绝 .. "
        "和 symlink 路径逃逸。"
    ),
    response_description="application/octet-stream 文件内容。",
    responses={
        200: {
            "description": "application/octet-stream 文件内容。",
            "content": {
                "application/octet-stream": {
                    "schema": {"type": "string", "format": "binary"}
                }
            },
        },
        400: {"description": "目标路径逃逸 strategy 根目录。"},
        401: {"description": "Bearer token 无效或缺失。"},
        404: {"description": "目标文件不存在。"},
        422: {"description": "filename 缺失、为空或包含未知 query 参数。"},
    },
)
async def download_file(params: Annotated[FileDownloadRequest, Query()]):
    """
    根据路径和文件名下载单个文件。支持任意文件类型和相对路径。
    """
    # --- 安全检查：验证文件路径是否在 BASE_DIR 目录下 ---
    file_path = (BASE_DIR / params.path / params.filename).resolve()
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
