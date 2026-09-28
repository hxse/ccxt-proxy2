"""把 SDK 未提供的代理注入限制在 TQ 调用上下文，不改变其他服务选路。"""

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from threading import Lock

import requests
import websockets


@dataclass(frozen=True)
class _Network:
    proxy_url: str | None


_network: ContextVar[_Network | None] = ContextVar("tq_network", default=None)
_install_lock = Lock()
_installed = False


def _install() -> None:
    global _installed
    with _install_lock:
        if _installed:
            return
        request = requests.Session.request
        connect = websockets.connect

        @wraps(request)
        def scoped_request(session, method, url, *args, **kwargs):
            network = _network.get()
            if network is None:
                return request(session, method, url, *args, **kwargs)
            kwargs["proxies"] = {
                "http": network.proxy_url,
                "https": network.proxy_url,
                "all": None,
            }
            trust_env = session.trust_env
            session.trust_env = False
            try:
                return request(session, method, url, *args, **kwargs)
            finally:
                session.trust_env = trust_env

        @wraps(connect)
        def scoped_connect(*args, **kwargs):
            network = _network.get()
            if network is not None:
                # None 明确直连；不让 websockets 默认读取宿主环境代理。
                kwargs["proxy"] = network.proxy_url
            return connect(*args, **kwargs)

        # 保留原调用参数与返回对象；这是集中、一次性的运行时入口适配。
        setattr(requests.Session, "request", scoped_request)
        setattr(websockets, "connect", scoped_connect)
        _installed = True


@contextmanager
def tq_network(proxy_url: str | None):
    # SDK 首次导入也会通过 requests 下载通知，需在导入之前进入上下文。
    _install()
    token = _network.set(_Network(proxy_url))
    try:
        yield
    finally:
        _network.reset(token)
