"""真实 Win32 控件与 CONNECT 端点；禁止连接终端或柜台。"""

import socketserver
import subprocess
import threading
from contextlib import contextmanager

import pytest

from src.cfb.config import Settings
from src.cfb.network import prepare_proxy, process_environment


@pytest.mark.parametrize("name", ["browser", "settlement", "gui_readiness", "control_scopes"])
def test_preserved_native_fixtures(wine_environment, name):
    result = subprocess.run(["wine", f"/fixtures/{name}.exe"], env=wine_environment,
                            capture_output=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert b"PASS" in result.stdout


@contextmanager
def endpoint(*, proxy):
    observed = []

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            self.connection.settimeout(5)
            if proxy:
                observed.append(self.rfile.readline(1024))
                while self.rfile.readline(1024) != b"\r\n":
                    pass
                self.wfile.write(b"HTTP/1.0 200 Connection established\r\n\r\n")
                self.wfile.flush()
            data = self.connection.recv(64)
            if data:
                observed.append(data)
                self.connection.sendall(data)

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield server.server_address[1], observed
        finally:
            server.shutdown()
            thread.join(5)


def test_wine_proxy_and_direct_branches(wine_environment, tmp_path):
    settings = Settings.model_validate({"bridge": {"data_dir": str(tmp_path)}})
    command = ["wine", "/fixtures/connect.exe"]
    with endpoint(proxy=True) as (port, observed):
        prepare_proxy(settings, f"http://127.0.0.1:{port}")
        result = subprocess.run([*command, "203.0.113.77", "12345"],
                                env=process_environment(settings, wine_environment), capture_output=True, timeout=20)
        assert result.returncode == 0, result.stdout + result.stderr
        assert observed == [b"CONNECT 203.0.113.77:12345 HTTP/1.0\r\n", b"cfb-native-connect"]
    settings._proxy_config = None
    with endpoint(proxy=False) as (port, observed):
        result = subprocess.run([*command, "127.0.0.1", str(port)],
                                env=process_environment(settings, wine_environment), capture_output=True, timeout=20)
        assert result.returncode == 0, result.stdout + result.stderr
        assert observed == [b"cfb-native-connect"]
    result = subprocess.run([*command, "203.0.113.77", "12345"],
                            env=process_environment(settings, wine_environment), capture_output=True, timeout=10)
    assert result.returncode == 3  # --network=none 下不可直达这个文档地址。
