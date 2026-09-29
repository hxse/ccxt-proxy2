"""离线 Wine 夹具使用全新临时前缀，不读取或复制真实账户。"""

import os
import subprocess
import time

import pytest


@pytest.fixture(scope="session")
def wine_environment(tmp_path_factory):
    root = tmp_path_factory.mktemp("wine")
    env = dict(os.environ, DISPLAY=":199", WINEARCH="win32", WINEPREFIX=str(root / "prefix"),
               WINEDEBUG="-all", WINEDLLOVERRIDES="mscoree,mshtml=")
    display = subprocess.Popen(["Xvfb", ":199", "-screen", "0", "1280x800x24", "-nolisten", "tcp"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 5
        while subprocess.run(["xdpyinfo"], env=env, capture_output=True, timeout=2).returncode:
            assert display.poll() is None and time.monotonic() < deadline
            time.sleep(.05)
        subprocess.run(["wineboot", "-u"], env=env, check=True, capture_output=True, timeout=60)
        yield env
    finally:
        subprocess.run(["wineserver", "-k"], env=env, timeout=10, capture_output=True)
        display.terminate()
        display.wait(timeout=5)
