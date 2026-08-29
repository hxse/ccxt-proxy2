from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok"] = Field(description="进程可以处理 HTTP 请求")


class ReadyResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ready"] = Field(description="Exchange registry 已初始化")
    initialized: list[str] = Field(
        description="已初始化的 exchange/market/mode identity"
    )


class NotReadyResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["not_ready"] = Field(description="应用不处于可服务状态")
    initialized: list[str] = Field(description="已经初始化完成的 identity")


class StrategyFileItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    filename: str = Field(description="文件名")
    path: str = Field(description="相对于 strategy 根目录的父目录")


class FileListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    files: list[StrategyFileItem]


class EmptyFileListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message: Literal["No files found."]


class FileUploadResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    filename: str = Field(description="实际保存的相对文件路径")
    message: Literal["file uploaded successfully."]
