"""单轮清理；会按计划删除服务持有的本地缓存。"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.tools.market_data_pipeline import run_cli  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(asyncio.run(run_cli("prune")))
