"""从实际补丁包编译队列，检查原生数据所有权；不加载交易 SDK。"""

import shutil
import subprocess
import sys
import sysconfig
import tarfile
from pathlib import Path

import pybind11
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def queue_probe(tmp_path_factory):
    compiler = shutil.which("c++")
    if compiler is None or sys.platform != "linux":
        pytest.skip("Native queue probe requires a Linux C++ compiler")
    directory = tmp_path_factory.mktemp("ctp-queue-probe")
    with tarfile.open(next((ROOT / "vendor/vnpy_ctp").glob("*.tar.gz"))) as archive:
        member = next(m for m in archive if m.name.endswith("/api/vnctp/vnctp.h"))
        header = archive.extractfile(member)
        assert header is not None
        (directory / "vnctp.h").write_bytes(header.read())
    module_path = directory / (
        "_ctp_queue_probe" + sysconfig.get_config_var("EXT_SUFFIX")
    )
    result = subprocess.run(
        [
            compiler,
            "-shared",
            "-fPIC",
            "-pthread",
            "-std=c++17",
            "-O0",
            "-I" + str(directory),
            "-I" + pybind11.get_include(),
            "-I" + sysconfig.get_path("include"),
            str(ROOT / "Test/ctp_queue_probe.cpp"),
            "-o",
            str(module_path),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    return module_path


@pytest.mark.parametrize(
    "scenario", ["pending", "consumed", "destructor", "late", "concurrent", "waiting"]
)
def test_callback_payloads_are_freed_once_and_consumers_stop(queue_probe, scenario):
    # 子进程隔离 C++ 崩溃/死锁；失败也不能拖住整个离线 suite。
    code = """
import importlib.util
import sys
spec = importlib.util.spec_from_file_location('_ctp_queue_probe', sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
module.verify(sys.argv[2])
print('released exactly once')
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(queue_probe), scenario],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "released exactly once" in result.stdout
