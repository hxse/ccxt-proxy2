"""场景覆盖的唯一合并规则；不读环境、不写文件、不初始化服务。"""

import copy
from typing import get_args, get_origin

from pydantic import BaseModel, TypeAdapter

from src.tools.config_types import AppConfig

PROFILE_VARIABLE = "CCXT_PROXY_PROFILE"
PROFILES = frozenset({"dev", "local", "remote"})


def _model(annotation):
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    for item in get_args(annotation):
        if isinstance(item, type) and issubclass(item, BaseModel):
            return item
    return None


def _validate_patch(patch: dict, model: type[BaseModel]) -> None:
    for name, value in patch.items():
        field = model.model_fields.get(name)
        if field is None:
            field = next(
                (item for item in model.model_fields.values() if item.alias == name),
                None,
            )
        if field is None:
            raise ValueError("unknown override field")
        annotation = field.annotation
        nested = _model(annotation)
        if nested is not None:
            if not isinstance(value, dict):
                raise ValueError("override table expected")
            _validate_patch(value, nested)
        elif get_origin(annotation) is dict and (args := get_args(annotation)):
            if not isinstance(value, dict):
                raise ValueError("override table expected")
            nested = _model(args[1])
            if nested is None:
                TypeAdapter(field.rebuild_annotation()).validate_python(value)
            else:
                for item in value.values():
                    if not isinstance(item, dict):
                        raise ValueError("override table expected")
                    _validate_patch(item, nested)
        else:
            TypeAdapter(field.rebuild_annotation()).validate_python(value)


def _merge(base: dict, patch: dict) -> dict:
    result = copy.deepcopy(base)
    for name, value in patch.items():
        if isinstance(value, dict) and isinstance(result.get(name), dict):
            result[name] = _merge(result[name], value)
        else:
            result[name] = copy.deepcopy(value)
    return result


def apply_profile(payload: dict, profile: str) -> dict:
    overrides = payload.get("overrides", {})
    if not isinstance(overrides, dict) or set(overrides) - PROFILES:
        raise ValueError("invalid override profiles")
    for patch in overrides.values():
        if not isinstance(patch, dict):
            raise ValueError("override table expected")
        _validate_patch(patch, AppConfig)
    base = {name: value for name, value in payload.items() if name != "overrides"}
    return _merge(base, overrides.get(profile, {}))
