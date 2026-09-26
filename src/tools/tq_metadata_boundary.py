"""第一方源码的元数据调用边界检查；不执行被检查代码。"""

import ast
from pathlib import Path

LEGACY = {
    "get_trading_calendar",
    "query_his_cont_quotes",
    "query_symbol_info",
    "_get_trading_calendar",
    "_init_chinese_rest_days",
    "TqContCalendar",
}
# 普通行情仍允许 SDK 内部调用 get_quote；第一方业务不得用其当前标的旁路。
CURRENT_MAPPING = {
    "get_quote",
    "get_quote_list",
    "query_graphql",
    "fetch_current_underlying",
    "resolve_underlying",
    "get_underlying",
}
ADAPTER = "src/tools/tq_trading_status.py"
REFERENCE = "Test/helpers/tq_metadata_reference.py"


def violations(source: str, path: str) -> list[str]:
    errors = []
    for node in ast.walk(ast.parse(source)):
        name = None
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module.startswith("Test.helpers.tq_metadata_reference"):
                name = "production import of offline reference"
            elif module.startswith("tqsdk.calendar") or (
                module == "tqsdk"
                and any(alias.name == "calendar" for alias in node.names)
            ):
                name = module
            elif (
                module in {"tqsdk", "tqsdk.api"}
                and any(alias.name == "TqApi" for alias in node.names)
                and path != ADAPTER
            ):
                name = "raw TqApi import"
        elif isinstance(node, ast.Import) and any(
            alias.name.startswith("tqsdk.calendar") for alias in node.names
        ):
            name = "tqsdk.calendar"
        elif isinstance(node, ast.Attribute) and (
            node.attr in LEGACY | CURRENT_MAPPING or node.attr == "TqApi"
        ):
            name = node.attr
        elif (
            isinstance(node, ast.Name)
            and isinstance(node.ctx, ast.Load)
            and node.id in LEGACY | CURRENT_MAPPING
        ):
            name = node.id
        elif isinstance(node, ast.Call):
            if (
                isinstance(node.func, ast.Name)
                and node.func.id == "getattr"
                and len(node.args) > 1
            ):
                arg = node.args[1]
                if isinstance(
                    arg, ast.Constant
                ) and arg.value in LEGACY | CURRENT_MAPPING | {"TqApi"}:
                    name = str(arg.value)
            if isinstance(node.func, ast.Name) and node.func.id == "TqApi":
                name = "raw TqApi instance"
        elif (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name in LEGACY | CURRENT_MAPPING
            and path != ADAPTER
        ):
            name = node.name
        if name and not (
            path == REFERENCE
            and name in LEGACY - {"query_symbol_info"} | {"tqsdk.calendar", "tqsdk"}
        ):
            errors.append(
                f"{path}:{getattr(node, 'lineno', 0)}: forbidden metadata access: {name}"
            )
    return errors


def repository_violations(root: Path) -> list[str]:
    errors = []
    for directory in ("src", "scripts", "script", "debug"):
        for path in (root / directory).rglob("*.py"):
            errors.extend(
                violations(path.read_text(), path.relative_to(root).as_posix())
            )
    return errors
