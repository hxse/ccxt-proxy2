"""旧配置的一次性白名单转换；运行中的应用不调用这个入口。"""

from typing import Any

from src.tools.config_loader import ConfigError


def migrate_whitelist_payload(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    if "service_whitelist" in result:
        if "exchange_whitelist" in result:
            raise ConfigError(
                "Both whitelist formats are present; resolve the conflict before migration"
            )
        return result
    entries = [
        {**item, "service": "ccxt"} for item in result.pop("exchange_whitelist", [])
    ]
    # 旧 TQ / CTP 按“存在配置”隐式启用；迁移成显式条目，保持原有访问范围。
    if result.get("tq") is not None:
        entries.append({"service": "tq"})
    for name, mode in (("test", "sandbox"), ("live", "live")):
        if (result.get("ctp") or {}).get(name) is not None:
            entries.append({"service": "ctp", "mode": mode})
    result["service_whitelist"] = entries
    return result
