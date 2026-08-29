import ast
import re
from pathlib import Path
from urllib.parse import parse_qsl

from src.cache_tool import DuckDbOhlcvCache
from src.main import app
from src.tools.shared import config_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRUNO_ROOT = PROJECT_ROOT / "bruno"
HTTP_METHODS = ("get", "post", "put", "patch", "delete", "options", "head")


def _block(text: str, name: str) -> str | None:
    match = re.search(rf"(?ms)^{re.escape(name)} \{{\n(.*?)^\}}", text)
    return match.group(1) if match else None


def _request(file: Path) -> tuple[str, str] | None:
    text = file.read_text()
    for method in HTTP_METHODS:
        block = _block(text, method)
        if block is None:
            continue
        match = re.search(r"(?m)^\s*url:\s*(.+)$", block)
        assert match, f"request URL is missing: {file}"
        return method.upper(), match.group(1).strip()
    return None


def _enabled_query_params(text: str) -> dict[str, str]:
    block = _block(text, "params:query")
    if block is None:
        return {}
    params: dict[str, str] = {}
    for raw_line in block.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("~"):
            continue
        name, value = line.split(":", 1)
        params[name.strip()] = value.strip()
    return params


def _request_files() -> list[Path]:
    return [file for file in BRUNO_ROOT.rglob("*.bru") if _request(file)]


def test_bruno_get_query_url_and_params_blocks_are_identical():
    checked = 0
    for file in _request_files():
        request = _request(file)
        assert request is not None
        method, url = request
        if method != "GET" or "?" not in url:
            continue
        url_params = dict(parse_qsl(url.split("?", 1)[1], keep_blank_values=True))
        assert _enabled_query_params(file.read_text()) == url_params, file
        checked += 1

    assert checked >= 28


def test_bruno_environment_contains_only_shared_values():
    environment = (BRUNO_ROOT / "environments/ccxt-proxy2.bru").read_text()
    variables = _enabled_query_params(environment.replace("vars {", "params:query {"))
    secrets = re.search(r"(?ms)^vars:secret \[\n(.*?)^\]", environment)

    assert variables == {"baseUrl": "http://localhost:5123"}
    assert secrets
    assert {line.strip().rstrip(",") for line in secrets.group(1).splitlines()} == {
        "user",
        "password",
    }

    references = set()
    for file in BRUNO_ROOT.rglob("*.bru"):
        references.update(re.findall(r"\{\{([^}]+)\}\}", file.read_text()))
    assert references == {"baseUrl", "user", "password"}


def test_every_bruno_request_targets_an_existing_fastapi_route():
    paths = app.openapi()["paths"]
    routes = {
        (path, method.upper())
        for path, operations in paths.items()
        for method in operations
    }
    for file in _request_files():
        request = _request(file)
        assert request is not None
        method, url = request
        path = url.removeprefix("{{baseUrl}}").split("?", 1)[0]
        assert (path, method) in routes, f"stale Bruno route in {file}: {method} {path}"


def test_bruno_get_query_parameters_are_declared_by_openapi():
    schema = app.openapi()
    for file in _request_files():
        request = _request(file)
        assert request is not None
        method, url = request
        if method != "GET":
            continue
        path = url.removeprefix("{{baseUrl}}").split("?", 1)[0]
        declared = {
            parameter["name"]
            for parameter in schema["paths"][path]["get"].get("parameters", [])
            if parameter.get("in") == "query"
        }
        provided = set(_enabled_query_params(file.read_text()))
        assert provided <= declared, f"unknown Bruno query parameter in {file}"


def test_every_openapi_operation_has_human_documentation():
    schema = app.openapi()
    assert schema["info"]["title"] == "ccxt-proxy2"
    assert schema["info"]["description"]

    for path, operations in schema["paths"].items():
        for method, operation in operations.items():
            if method.upper() not in {name.upper() for name in HTTP_METHODS}:
                continue
            assert operation.get("summary", "").strip(), (
                f"missing summary: {method} {path}"
            )
            assert operation.get("description", "").strip(), (
                f"missing description: {method} {path}"
            )
            assert operation.get("tags"), f"missing tag: {method} {path}"
            success = operation["responses"].get("200")
            assert success, f"missing 200 response docs: {method} {path}"
            assert success["description"] != "Successful Response", (
                f"default response description: {method} {path}"
            )
            for parameter in operation.get("parameters", []):
                assert parameter.get("description", "").strip(), (
                    f"missing parameter description: {method} {path} "
                    f"{parameter['name']}"
                )
            for media_type, media in success.get("content", {}).items():
                assert media.get("schema"), (
                    f"empty 200 schema: {method} {path} {media_type}"
                )


def test_bruno_contains_the_full_fetch_ohlcv_provider_matrix():
    root = BRUNO_ROOT / "CCXT PROXY/fetch_ohlcv"
    routes = {
        "fetch_ohlcv_since_limit": "/ccxt/fetch_ohlcv/since-limit",
        "fetch_ohlcv_since_latest": "/ccxt/fetch_ohlcv/since-latest",
        "fetch_ohlcv_latest_limit": "/ccxt/fetch_ohlcv/latest-limit",
    }
    for folder, route in routes.items():
        for provider in ("binance", "kraken"):
            file = root / folder / f"{provider}.bru"
            assert file.is_file()
            request = _request(file)
            assert request is not None
            assert request[1].removeprefix("{{baseUrl}}").startswith(f"{route}?")


def test_every_bru_path_referenced_by_justfile_exists():
    justfile = (PROJECT_ROOT / "justfile").read_text()
    references = {
        quoted or plain
        for quoted, plain in re.findall(
            r"'([^']+\.bru)'|(?<![\w{}])([\w./-]+\.bru)", justfile
        )
    }
    assert references
    for reference in references:
        assert (BRUNO_ROOT / reference).is_file(), reference


def test_offline_tests_and_bruno_credentials_use_separate_config_paths():
    expected = PROJECT_ROOT / "Test/fixtures/config.json"
    justfile = (PROJECT_ROOT / "justfile").read_text()

    assert config_path.resolve() == expected.resolve()
    assert "bru_user :=" not in justfile
    assert "bru_password :=" not in justfile
    assert "export BRU_" not in justfile
    assert "scripts/run_bruno.py" in justfile
    assert "CCXT_PROXY_CONFIG_PATH=./data/config.json" in justfile


def test_mutating_bruno_requests_are_visibly_marked_stateful():
    for file in _request_files():
        request = _request(file)
        assert request is not None
        method, _ = request
        if method in {"GET", "HEAD", "OPTIONS"}:
            continue
        meta = _block(file.read_text(), "meta")
        assert meta is not None
        name = re.search(r"(?m)^\s*name:\s*(.+)$", meta)
        assert name and "[STATEFUL" in name.group(1), file


def test_bruno_readonly_recipe_references_get_requests_only():
    justfile = (PROJECT_ROOT / "justfile").read_text()
    recipe = re.search(
        r"(?m)^bru-readonly-basic:\n((?:    [^\n]*\n)+)",
        justfile,
    )
    assert recipe
    references = re.findall(
        r"'([^']+\.bru)'|(?<![\w{}])([\w./-]+\.bru)", recipe.group(1)
    )
    files = [BRUNO_ROOT / (quoted or plain) for quoted, plain in references]

    assert files
    for file in files:
        request = _request(file)
        if request is None:
            assert file.parent == BRUNO_ROOT / "environments"
            continue
        assert request[0] == "GET", file


def test_python_files_stay_within_the_400_line_limit():
    oversized = {
        str(file.relative_to(PROJECT_ROOT)): len(file.read_text().splitlines())
        for root in ("src", "Test", "scripts")
        for file in (PROJECT_ROOT / root).rglob("*.py")
        if len(file.read_text().splitlines()) > 400
    }
    assert oversized == {}


def test_documentation_files_stay_within_the_500_line_limit():
    oversized = {
        str(file.relative_to(PROJECT_ROOT)): len(file.read_text().splitlines())
        for file in (PROJECT_ROOT / "docs").rglob("*.md")
        if len(file.read_text().splitlines()) > 500
    }
    assert oversized == {}


def test_removed_parallel_ccxt_modules_do_not_return():
    removed = (
        "src/tools/ccxt_utils.py",
        "src/tools/ccxt_utils_extended.py",
        "src/tools/binance_adapter.py",
    )
    assert all(not (PROJECT_ROOT / path).exists() for path in removed)


def test_cache_module_has_no_network_or_http_framework_dependency():
    forbidden = {"ccxt", "fastapi", "tqsdk"}
    for file in (PROJECT_ROOT / "src/cache_tool").glob("*.py"):
        tree = ast.parse(file.read_text())
        imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module.split(".", 1)[0])
        assert imports.isdisjoint(forbidden), file


def test_cache_public_api_stays_io_only_and_callback_free():
    public_methods = {
        name
        for name, value in vars(DuckDbOhlcvCache).items()
        if not name.startswith("_") and callable(value)
    }

    assert public_methods == {"read_best_prefix", "write_segment", "close"}


def test_online_test_suite_contains_read_only_operations_only():
    banned_calls = {
        "add_margin",
        "cancel_all_orders",
        "cancel_order",
        "cancel_orders",
        "close_position",
        "create_limit_order",
        "create_market_order",
        "create_order",
        "create_stop_market_order",
        "create_take_profit_market_order",
        "edit_order",
        "reduce_margin",
        "send_message",
        "set_leverage",
        "set_margin_mode",
        "set_position_mode",
        "transfer",
        "withdraw",
    }
    online_files = list((PROJECT_ROOT / "Test/online").glob("test_*.py"))

    assert online_files
    for file in online_files:
        tree = ast.parse(file.read_text())
        used = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        assert used.isdisjoint(banned_calls), file

    justfile = (PROJECT_ROOT / "justfile").read_text()
    recipe = re.search(
        r"(?m)^test-online \*args:\n((?:    [^\n]*\n)+)",
        justfile,
    )
    assert recipe
    assert "TELEGRAM" not in recipe.group(1)
    assert "telegram" not in recipe.group(1)


def test_ccxt_online_suite_targets_live_identities_only():
    source = (PROJECT_ROOT / "Test/online/test_ccxt_online.py").read_text()
    tree = ast.parse(source)
    used = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    private_reads = {
        "fetch_balance",
        "fetch_closed_orders",
        "fetch_market_info",
        "fetch_my_trades",
        "fetch_open_orders",
        "fetch_order",
        "fetch_positions",
    }

    assert 'item.mode == "live"' in source
    assert '"exchange_whitelist": live_futures' in source
    assert "sandbox" not in source
    assert used.isdisjoint(private_reads)
