import json
import subprocess
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit("usage: run_bruno.py REQUEST_OR_FOLDER [...]")

    project_root = Path(__file__).resolve().parents[1]
    config = json.loads((project_root / "data/config.json").read_text())
    users = config.get("users", {})
    username = next(iter(users), "")
    if not username:
        raise SystemExit("No users configured in data/config.json")
    password = users[username].get("password")
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
    return subprocess.run(command, cwd=project_root / "bruno", check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
