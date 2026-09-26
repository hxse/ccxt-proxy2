"""单轮采集；只通过带鉴权的正式 HTTP 路由。"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.tools.market_data_pipeline import run_cli  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(asyncio.run(run_cli("collect")))
