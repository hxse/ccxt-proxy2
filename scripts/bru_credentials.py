import json
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"user", "password"}:
        raise SystemExit("usage: bru_credentials.py [user|password]")

    config = json.loads(Path("data/config.json").read_text())
    users = config.get("users", {})
    username = next(iter(users), "")
    if not username:
        raise SystemExit("No users configured in data/config.json")

    if sys.argv[1] == "user":
        print(username)
    else:
        print(users[username]["password"])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
