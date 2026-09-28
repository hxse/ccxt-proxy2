"""将上游 OpenAPI 原文合入本项目文档；只补充代理自身的鉴权和网络错误。"""

from copy import deepcopy
from typing import Any

from fastapi import FastAPI

from src.cfb_contract import CFB_ROUTES, load_contract

PROXY_ERROR_SCHEMA = {
    "type": "object",
    "required": ["detail"],
    "properties": {
        "detail": {
            "type": "object",
            "required": ["code"],
            "properties": {"code": {"type": "string"}, "service": {"type": "string"}},
        }
    },
}
PROXY_ERRORS = {
    "503": "CFB 未启用、初始化中/失败，或代理连接、读取及等待超时：detail.code=SERVICE_NOT_ENABLED/SERVICE_NOT_READY，service=cfb；不会自动重试，写请求失败须先核对上游结果。",
}


def namespace_refs(value: Any) -> Any:
    """仅给组件引用加前缀，包含撤单 discriminator 中的映射引用。"""
    if isinstance(value, dict):
        return {key: namespace_refs(item) for key, item in value.items()}
    if isinstance(value, list):
        return [namespace_refs(item) for item in value]
    if isinstance(value, str) and value.startswith("#/components/"):
        prefix, name = value.rsplit("/", 1)
        return f"{prefix}/Cfb_{name}"
    return value


def install_cfb_openapi(app: FastAPI) -> None:
    source = load_contract()
    original_openapi = app.openapi

    def openapi() -> dict[str, Any]:
        if app.openapi_schema is not None:
            return app.openapi_schema
        schema = original_openapi()
        for section, components in source.get("components", {}).items():
            if section == "securitySchemes":
                continue  # 使用本项目 Bearer 鉴权。
            schema.setdefault("components", {}).setdefault(section, {}).update(
                {
                    f"Cfb_{name}": namespace_refs(value)
                    for name, value in components.items()
                }
            )
        for path, method in CFB_ROUTES.items():
            local = schema["paths"][path][method]
            operation = namespace_refs(deepcopy(source["paths"][path][method]))
            operation.update(
                tags=["CFB"],
                security=local["security"],
                operationId=local["operationId"],
            )
            operation["responses"]["401"] = {
                "description": "未通过本项目 Bearer 鉴权。"
            }
            for code, description in PROXY_ERRORS.items():
                response = operation["responses"].setdefault(code, {"description": ""})
                response["description"] += " " + description
                content = response.setdefault("content", {}).setdefault(
                    "application/json", {}
                )
                upstream_schema = content.get("schema")
                content["schema"] = (
                    {"anyOf": [upstream_schema, deepcopy(PROXY_ERROR_SCHEMA)]}
                    if upstream_schema
                    else deepcopy(PROXY_ERROR_SCHEMA)
                )
            schema["paths"][path][method] = operation
        app.openapi_schema = schema
        return schema

    setattr(app, "openapi", openapi)
