import os
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

os.environ.setdefault(
    "CCXT_PROXY_CONFIG_PATH",
    str(Path(__file__).parent / "fixtures" / "config.json"),
)


@pytest.fixture
def temp_dir() -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as directory:
        yield Path(directory)
