"""运维 CLI 复用唯一 Unix socket 协议，不直接操作终端或数据库。"""

import argparse
import asyncio
import base64
import json
from pathlib import Path

from .client import Client
from .config import Settings
from .ipc import Kind


async def invoke(action: Kind, output: Path | None) -> int:
    client = Client(Path("/data/run/bridge.sock"))
    client.settings = Settings()
    client.timeout = 30 if action in {"pause", "resume"} else 5
    client.enabled = True
    try:
        result = await client.call(action)
        if action == "screenshot" and result.status == 200:
            if output is None:
                print(json.dumps(result.body))
            else:
                if not isinstance(result.body, dict) or not isinstance(result.body.get("base64"), str):
                    raise ValueError("invalid screenshot")
                encoded = result.body["base64"]
                assert isinstance(encoded, str)
                data = base64.b64decode(encoded, validate=True)
                output.write_bytes(data)
        else:
            print(json.dumps(result.body, ensure_ascii=False))
        return 0 if result.status < 400 else 1
    finally:
        await client.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="CFB 容器内运维")
    parser.add_argument("action", choices=["status", "health", "pause", "resume", "clean", "screenshot"])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        return asyncio.run(invoke(args.action, args.output))
    except Exception as exc:
        print("CFB 运维失败：" + type(exc).__name__)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
