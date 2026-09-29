"""本地构建薄转发，与远端使用同一个 Shell 构建实现。"""

from pathlib import Path

from scripts.container_common import command

ROOT = Path(__file__).resolve().parents[1]


def build_image(config_path: Path):
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
            config_path.resolve(),
        ],
        capture=False,
        timeout=None,
    )
