from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ServiceErrorDetail(BaseModel):
    code: Literal["SERVICE_NOT_ENABLED", "SERVICE_NOT_READY"] = Field(
        description="未列入启动白名单，或服务尚未完成启动/已关闭。"
    )
    service: str = Field(
        description="服务身份，如 tq、ctp/sandbox、ccxt/binance/future/sandbox。"
    )


class ServiceUnavailableResponse(BaseModel):
    detail: ServiceErrorDetail | str = Field(
        description="白名单/就绪错误，或 SDK 工作线程未就绪的错误码。"
    )


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok"] = Field(description="进程可以处理 HTTP 请求")


class ReadyResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ready"] = Field(
        description="服务白名单中的全部实例已完成启动初始化"
    )
    initialized: list[str] = Field(
        description="已初始化实例，例如 ccxt/binance/future/sandbox、tq、ctp/sandbox，顺序与白名单一致。"
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
