import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.tools.config_loader import load_config  # noqa: E402


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit("usage: run_bruno.py REQUEST_OR_FOLDER [...]")

    users = load_config().users
    username = next(iter(users), "")
    if not username:
        raise SystemExit("No users configured in config.toml ([users.<username>])")
    password = users[username].password
    if not password:
        raise SystemExit("Configured Bruno user has no password")

    command = [
        "bru",
        "run",
        *sys.argv[1:],
        "--env-file",
        "environments/ccxt-proxy2.bru",
        "--env-var",
        f"user={username}",
        "--env-var",
        f"password={password}",
        "--reporter-skip-all-headers",
        "--noproxy",
    ]
    return subprocess.run(command, cwd=PROJECT_ROOT / "bruno", check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
