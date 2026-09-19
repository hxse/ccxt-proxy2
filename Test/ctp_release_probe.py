"""子进程内用本机假 TCP 前置验证真实 SDK 释放，不登录、不发送交易请求。"""

import queue
import socket
import sys
import tempfile
import threading
import time

from src.tools.config_types import CtpAccountConfig, CtpConfig
from src.tools.ctp_session import CtpSession
from src.tools.ctp_spi import create_api


def run_probe(scenario: str) -> None:
    accepted: queue.Queue[socket.socket] = queue.Queue()
    entered = threading.Event()
    returned = threading.Event()

    with socket.socket() as listener, tempfile.TemporaryDirectory() as path:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(5)

        def accept() -> None:
            connection, _ = listener.accept()
            connection.settimeout(5)
            connection.recv(4096)
            accepted.put(connection)

        server = threading.Thread(target=accept, daemon=True)
        server.start()

        def factory(callbacks):
            api = create_api(callbacks)

            def on_disconnect(reason):
                callbacks.on_disconnected()
                entered.set()
                if scenario == "callback_in_progress":
                    # 回调尚未返回时让主线程执行 close；旧 SDK 会持 GIL 等待本线程。
                    time.sleep(0.2)
                returned.set()

            api.onFrontDisconnected = on_disconnect
            return api

        account = CtpAccountConfig(
            trader_front=f"tcp://127.0.0.1:{listener.getsockname()[1]}",
            broker_id="9999",
            investor_id="test-user",
            password="test-password",
        )
        session = CtpSession(account, CtpConfig(flow_path=path), "sandbox", factory)
        connection = accepted.get(timeout=5)
        server.join(timeout=1)
        old_interval = sys.getswitchinterval()
        try:
            if scenario == "callback_waiting_for_gil":
                # 固定竞态：原生断线回调等待 GIL，主线程持 GIL 进入 Release。
                sys.setswitchinterval(10)
                connection.close()
                deadline = time.monotonic() + 0.2
                while time.monotonic() < deadline:
                    pass
            else:
                connection.close()
                assert entered.wait(5), "native disconnect callback did not arrive"
            session.close()
            if scenario == "callback_in_progress":
                assert returned.is_set(), "exit must let the native callback finish"
            session.close()
            session.close()  # 会话重复关闭不能再次调用原生 exit。
        finally:
            sys.setswitchinterval(old_interval)
            connection.close()
        print("released")


if __name__ == "__main__":
    run_probe(sys.argv[1])
