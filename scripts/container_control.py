"""本地构建/上传排队期间核对停止代次；实例控制统一由 Shell 执行。"""

from pathlib import Path

from scripts.container_common import (
    DeploymentError,
)


class DeploymentCancelled(DeploymentError):
    """显式停止使旧流程失去再次启动实例的资格。"""


def stop_generation(root: Path) -> int:
    try:
        value = int((root / ".container/stop-generation").read_text())
        if value < 0:
            raise ValueError
        return value
    except FileNotFoundError:
        return 0
    except (OSError, ValueError):
        raise DeploymentError(
            "停止代次不可读取，请检查 .container/stop-generation"
        ) from None


class StartGuard:
    def __init__(self, root: Path, expected: int | None = None):
        self.root = root
        self.expected = stop_generation(root) if expected is None else expected

    def cancelled(self):
        return stop_generation(self.root) != self.expected

    def check(self):
        if self.cancelled():
            raise DeploymentCancelled(
                "停止请求已取消本次后续启动；构建或上传结果可以保留"
            )
