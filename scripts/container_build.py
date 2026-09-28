"""本地构建薄转发，与远端使用同一个 Shell 构建实现。"""

from pathlib import Path

from scripts.container_common import command

ROOT = Path(__file__).resolve().parents[1]


def build_image():
    # 调用方已持有本地项目操作锁。
    command(
        [
            "sh",
            ROOT / "scripts/container_manage.sh",
            ROOT,
            "build-local",
            ROOT,
            "",
            "false",
            "",
            "local",
        ],
        capture=False,
        timeout=None,
    )
