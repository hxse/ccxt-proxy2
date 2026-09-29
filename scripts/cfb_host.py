"""宿主 serve 在进入 Uvicorn 前准备 CFB；运行进程只保留 socket 客户端。"""

from pathlib import Path

from scripts.container_common import command, project_lock, require_runtime
from scripts.container_control import StartGuard
from src.cfb.layout import layout

ROOT = Path(__file__).resolve().parents[1]


def ensure_cfb(config_path: Path, config, profile: str) -> None:
    if not any(item.service == "cfb" for item in config.service_whitelist):
        return
    require_runtime()
    guard = StartGuard(ROOT)
    with project_lock(ROOT, check_cancel=guard.check):
        command(["sh", ROOT / "scripts/container_cfb_host.sh", ROOT,
                 config_path.resolve(), profile, layout(config), str(guard.expected)], capture=False, timeout=None)
