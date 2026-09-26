"""本地 HTTP 生命周期检查；空服务白名单，不访问任何外部网络。"""

import ast
from pathlib import Path

import pytest

from Test.online.live_service import LiveService
from Test.test_market_data_config import job_config


def test_online_harness_starts_and_stops_only_its_isolated_server(tmp_path):
    service = LiveService(job_config(tq=False), [], tmp_path)
    try:
        service.start()
        assert service.get("/cache/summary") == {"items": []}
        assert (tmp_path / "isolated.duckdb").exists()
        with pytest.raises(AssertionError):
            service.get("/cache/prune")
        assert (tmp_path / "config.toml").stat().st_mode & 0o777 == 0o600
    finally:
        service.close()
    assert service._process is not None and service._process.poll() is not None
    assert not (tmp_path / "config.toml").exists()


def test_background_modules_and_data_routes_do_not_bypass_cache_or_sdk_boundaries():
    root = Path(__file__).resolve().parents[1]
    paths = [
        root / "src/router/tq_router.py",
        root / "src/router/cache_router.py",
        root / "src/router/trader_router.py",
    ]
    paths += list((root / "src/tools").glob("market_data_*.py"))
    paths += [
        root / "scripts" / (name + ".py")
        for name in ("collect_market_data", "prune_market_data", "market_data_pipeline")
    ]
    for path in paths:
        tree = ast.parse(path.read_text())
        imports = {
            name
            for node in ast.walk(tree)
            for name in (
                [alias.name for alias in node.names]
                if isinstance(node, ast.Import)
                else [node.module or ""]
                if isinstance(node, ast.ImportFrom)
                else []
            )
        }
        assert not any(name.split(".")[0] in {"duckdb", "tqsdk"} for name in imports), (
            path
        )
        calls = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        assert calls.isdisjoint(
            {
                "execute",
                "executemany",
                "_connection",
                "get_kline_serial",
                "query_his_cont_quotes",
                "get_trading_calendar",
            }
        ), path
