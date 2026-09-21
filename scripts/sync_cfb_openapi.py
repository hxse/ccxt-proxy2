"""读取 CFB /openapi.json，只同步固定八条业务路由文档，不调用业务接口。"""

import argparse
import json
import sys
from copy import deepcopy
from pathlib import Path
from urllib.error import URLError
from urllib.request import ProxyHandler, build_opener

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.cfb_contract import CFB_ROUTES, CONTRACT_PATH  # noqa: E402


def extract_contract(document: dict) -> dict:
    if not str(document.get("openapi", "")).startswith("3."):
        raise ValueError("CFB 文档必须为 OpenAPI 3")
    paths = {}
    for path, method in CFB_ROUTES.items():
        operation = document.get("paths", {}).get(path, {}).get(method)
        if not isinstance(operation, dict) or not operation.get("responses"):
            raise ValueError(f"CFB 文档缺少 {method.upper()} {path} 或响应定义")
        paths[path] = {method: deepcopy(operation)}
    return {
        "openapi": document["openapi"],
        "info": deepcopy(document["info"]),
        "paths": paths,
        "components": deepcopy(document.get("components", {})),
    }


def sync_openapi(url: str, destination: Path = CONTRACT_PATH) -> None:
    opener = build_opener(ProxyHandler({}))
    with opener.open(url, timeout=15) as response:
        contract = extract_contract(json.load(response))
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(contract, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    temporary.replace(destination)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", nargs="?", default="http://127.0.0.1:45173/openapi.json")
    args = parser.parse_args()
    try:
        sync_openapi(args.url)
    except (OSError, URLError, ValueError, KeyError) as exc:
        print(
            f"CFB 文档同步失败：{type(exc).__name__}；未更新文档快照", file=sys.stderr
        )
        return 1
    print(f"已同步 {len(CFB_ROUTES)} 条 CFB 路由文档至 {CONTRACT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
