"""只加载 VeighNa 的交易扩展，不导入网关、行情扩展或 vnpy 框架。"""

import sys
import threading
from importlib import machinery, metadata, util
from typing import Any

_load_lock = threading.Lock()
_td_api: Any = None


def load_td_api() -> Any:
    global _td_api
    with _load_lock:
        if _td_api is not None:
            return _td_api
        distribution = metadata.distribution("vnpy_ctp")
        if not distribution.version.endswith("+ccxtproxy.2"):
            raise RuntimeError(
                "CTP 需要项目固定的 VeighNa 补丁版本；"
                "请执行 uv sync --locked --extra ctp 并重启服务。"
            )
        candidates = [
            distribution.locate_file(path)
            for path in distribution.files or ()
            if path.parent.as_posix() == "vnpy_ctp/api"
            and path.name.startswith("vnctptd.")
            and path.name.endswith(tuple(machinery.EXTENSION_SUFFIXES))
        ]
        if len(candidates) != 1:
            raise RuntimeError("找不到当前 Python 对应的 VeighNa CTP 交易扩展。")
        # 直接加载扩展文件，避免执行 vnpy_ctp.__init__ 中的 CtpGateway 导入。
        name = "_ccxt_proxy_ctp.vnctptd"
        spec = util.spec_from_file_location(name, str(candidates[0]))
        if spec is None or spec.loader is None:
            raise RuntimeError("无法加载 VeighNa CTP 交易扩展。")
        module = util.module_from_spec(spec)
        sys.modules[name] = module
        try:
            spec.loader.exec_module(module)
            _td_api = module.TdApi
        except BaseException:
            sys.modules.pop(name, None)
            raise
        return _td_api
