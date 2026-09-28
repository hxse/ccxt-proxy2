from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ServiceErrorDetail(BaseModel):
    code: Literal["SERVICE_NOT_ENABLED", "SERVICE_NOT_READY"] = Field(
        description="未列入启动白名单，或所选服务初始化中、失败、已关闭/不可用。"
    )
    service: str = Field(
        description="服务身份，如 tq、ctp/sandbox、ccxt/binance/future/sandbox。"
    )


class ServiceUnavailableResponse(BaseModel):
    detail: ServiceErrorDetail = Field(description="统一的服务启用/可用性错误。")


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok"] = Field(description="进程可以处理 HTTP 请求")


class ReadyResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ready"] = Field(
        description="HTTP 应用可服务，单个 SDK 未就绪不影响此状态"
    )
    initialized: list[str] = Field(
        description="当前已就绪实例，例如 ccxt/binance/future/sandbox、tq、ctp/sandbox，顺序与白名单一致。"
    )
    services: dict[str, Literal["initializing", "ready", "failed", "stopped"]] = Field(
        description="每个白名单身份的本地生命周期状态；不进行网络探测。"
    )


class NotReadyResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["not_ready"] = Field(description="应用不处于可服务状态")
    initialized: list[str] = Field(description="已经初始化完成的 identity")
    services: dict[str, Literal["initializing", "ready", "failed", "stopped"]]


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
