"""只修改远端上传副本中的三个服务代理开关，保留本地配置原件。"""

import copy
import re
import tomllib
from pathlib import Path

from scripts.container_common import DeploymentError, write_private
from src.tools.config_loader import load_config


def _payload(text: str) -> dict:
    try:
        return tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        raise DeploymentError("远端代理补丁无法解析 TOML；配置值已隐藏") from None


def _enable(text: str, exchange: str, configured: dict) -> str:
    if configured.get("enable_proxy") is True:
        return text
    newline = "\r\n" if "\r\n" in text else "\n"
    header = re.compile(
        rf"(?m)^[ \t]*\[[ \t]*(?:{exchange}|\"{exchange}\"|'{exchange}')[ \t]*\]"
        r"[ \t]*(?:#[^\r\n]*)?(?:\r?\n|$)"
    )
    matches = list(header.finditer(text))
    if not matches:
        # 只有子表时，可补上尚未显式定义的父表；最终语义校验会拒绝冲突。
        return text + newline + f"[{exchange}]{newline}enable_proxy = true{newline}"
    if len(matches) != 1:
        raise DeploymentError(f"远端代理补丁无法唯一定位 [{exchange}]；配置值已隐藏")
    start = matches[0].end()
    following = re.search(r"(?m)^[ \t]*\[", text[start:])
    end = start + following.start() if following else len(text)
    block = text[start:end]
    key = re.compile(
        r"(?m)^([ \t]*(?:enable_proxy|\"enable_proxy\"|'enable_proxy')[ \t]*=[ \t]*)"
        r"(true|false)(?=[ \t]*(?:#[^\r\n]*)?\r?$)"
    )
    if "enable_proxy" not in configured:
        separator = "" if start and text[start - 1] == "\n" else newline
        block = separator + f"enable_proxy = true{newline}" + block
    else:
        if len(list(key.finditer(block))) != 1:
            raise DeploymentError(
                f"远端代理补丁要求 [{exchange}] 下有独立的布尔 enable_proxy；配置值已隐藏"
            )
        block = key.sub(lambda match: match[1] + "true", block)
    return text[:start] + block + text[end:]


def prepare_remote_config(source: Path, target: Path) -> None:
    try:
        text = source.read_bytes().decode("utf-8")
    except UnicodeError:
        raise DeploymentError("远端配置必须为 UTF-8 TOML；配置值已隐藏") from None
    original = _payload(text)
    expected = copy.deepcopy(original)
    for exchange in ("binance", "kraken", "tq"):
        configured = original.get(exchange)
        if configured is None:
            continue
        if not isinstance(configured, dict):
            raise DeploymentError(f"远端配置的 [{exchange}] 必须为表；配置值已隐藏")
        text = _enable(text, exchange, configured)
        expected[exchange]["enable_proxy"] = True
    # 防止注释或多行字符串里的相似文字被误改，不能只信任文本匹配。
    if _payload(text) != expected:
        raise DeploymentError("远端代理补丁修改了目标开关以外的配置，已拒绝上传")
    write_private(target, text.encode("utf-8"))
    load_config(target)
