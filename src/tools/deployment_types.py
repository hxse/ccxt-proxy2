"""私有部署目标；应用只校验，不在启动时连接 SSH。"""

import re
from pathlib import PurePosixPath

from pydantic import BaseModel, ConfigDict, field_validator


class DeploymentConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    ssh_host: str
    remote_dir: str = "dev/ccxt-proxy2"

    @field_validator("ssh_host")
    @classmethod
    def validate_host(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", value):
            raise ValueError("ssh_host must be an SSH host alias")
        return value

    @field_validator("remote_dir")
    @classmethod
    def validate_directory(cls, value: str) -> str:
        path = PurePosixPath(value)
        if (
            not value.strip()
            or path.is_absolute()
            or not path.parts
            or ".." in path.parts
            or any(char in value for char in "\x00\r\n:")
            or value.startswith("~")
        ):
            raise ValueError("remote_dir must be relative to the SSH user's home")
        return str(path)
