import os
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

os.environ.setdefault(
    "CCXT_PROXY_CONFIG_PATH",
    str(Path(__file__).parent / "fixtures" / "config.toml"),
)

# 离线测试不继承开发 shell 的真实 Provider 配置；显式 online/stateful 入口除外。
if (
    Path(os.environ["CCXT_PROXY_CONFIG_PATH"]).resolve()
    == (Path(__file__).parent / "fixtures" / "config.toml").resolve()
):
    for name in list(os.environ):
        if name.startswith("CCXT_PROXY_") and name != "CCXT_PROXY_CONFIG_PATH":
            os.environ.pop(name)


@pytest.fixture
def temp_dir() -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as directory:
        yield Path(directory)
