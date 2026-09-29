"""容量故障必须回收真实写入进程，同时保护实例锁和交易证据；全部离线。"""

import os
import select
import signal
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from src.cfb.artifacts import ArtifactStore
from src.cfb.config import ArtifactConfig, BridgeConfig, LoggingConfig, Settings
from src.cfb.dispatch import Dispatcher
from src.cfb.errors import BridgeError
from src.cfb.logging_store import LogStore
from src.cfb.models import Operation
from src.cfb.runtime import Runtime
from src.cfb.service import BridgeService

WRITER = """
import sys, time
with open(sys.argv[1], 'ab', buffering=0) as stream:
    stream.write(b'x' * int(sys.argv[2]))
    print('ready', flush=True)
    while True:
        if sys.argv[3] == 'grow':
            stream.write(b'x')
        time.sleep(.01)
"""


@pytest.fixture
def writers():
    processes = []

    def start(path: Path, size: int = 8, *, grow: bool = False):
        path.parent.mkdir(parents=True, exist_ok=True)
        process = subprocess.Popen(
            [sys.executable, '-c', WRITER, str(path), str(size), 'grow' if grow else 'idle'],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, start_new_session=True,
        )
        processes.append(process)
        assert process.stdout is not None
        assert select.select([process.stdout], [], [], 5)[0], '离线写入进程未启动'
        assert process.stdout.readline() == b'ready\n'
        return process

    yield start
    for process in processes:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=3)
        if process.stdout is not None:
            process.stdout.close()


@pytest.fixture
def service(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    settings = Settings(
        bridge=BridgeConfig(data_dir=tmp_path / 'cfb'),
        logging=LoggingConfig(max_file_bytes=512, max_total_bytes=2048),
        artifacts=ArtifactConfig(min_free_bytes=1),
    )
    value = BridgeService(settings)
    value.runtime.acquire()
    value.logs = LogStore(settings)
    value.artifacts = ArtifactStore(settings)
    value.dispatcher = Dispatcher(settings, value.runtime)
    # 只触发一轮周期检查，不让离线测试等待生产环境的 60 秒间隔。
    checks = iter((False, True))
    monkeypatch.setattr(value.stopping, 'wait', lambda timeout: next(checks))
    yield value
    value.stop()


def start_monitor(runtime: Runtime, running: bool) -> None:
    if running:
        runtime.state = 'window_visible'
        runtime.window_visible = True

    def monitor() -> None:
        if running:
            runtime.stop_event.wait()
            runtime._stop_processes()
        else:
            # 表示启动失败后保留桌面和写入进程、监控线程已经退出的状态。
            runtime._fail('WINDOW_TIMEOUT', '离线启动超时')

    runtime.worker = threading.Thread(target=monitor, daemon=True)
    runtime.worker.start()
    if not running:
        runtime.worker.join(timeout=2)
        assert not runtime.worker.is_alive()


def assert_session_owned(runtime: Runtime) -> None:
    contender = Runtime(runtime.settings)
    with pytest.raises(BridgeError) as error:
        contender.acquire()
    assert error.value.code == 'SESSION_IN_USE'


@pytest.mark.parametrize('monitor_running', [False, True])
def test_storage_failure_stops_writer_and_preserves_session_and_evidence(
    service: BridgeService, writers, tmp_path: Path, monitor_running: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert service.logs is not None and service.artifacts is not None
    assert service.dispatcher is not None
    raw_log = service.logs.root / 'terminal.log'
    writer = writers(raw_log, 4096, grow=True)
    other_writer = writers(tmp_path / 'another-instance' / 'terminal.log', grow=True)
    service.runtime.processes['terminal'] = writer
    start_monitor(service.runtime, monitor_running)

    operation = Operation(action='create_limit_order', parameters={
        'is_live': False, 'exchange_id': 'CZCE', 'instrument_id': 'RM701',
        'side': 'buy', 'offset': 'open', 'volume': 1, 'price': 2323,
    })
    journal = service.dispatcher.journal
    journal.admit('offline-unknown', operation, 'offline-key')
    journal.phase('offline-unknown', 'sending', effect='unknown')
    artifact = service.artifacts.create()
    evidence = artifact / 'evidence.txt'
    evidence.write_text('保留未决提交证据')
    service.artifacts.finish(artifact, keep=True, protect=True)

    released = []
    terminate = service.dispatcher.terminate_after_desktop

    def terminate_after_desktop() -> None:
        assert writer.poll() is not None, '桌面仍在写入时不得释放执行器'
        terminate()
        released.append(True)

    monkeypatch.setattr(service.dispatcher, 'terminate_after_desktop', terminate_after_desktop)
    service._maintain()

    status = service.status()
    assert status.error is not None
    assert status.error.code == 'STORAGE_UNAVAILABLE'
    assert not status.trading_ready
    assert not status.terminal_window_visible
    assert service.dispatcher.stopping
    assert writer.wait(timeout=2) is not None
    assert released == [True]
    assert other_writer.poll() is None
    assert evidence.read_text() == '保留未决提交证据'
    assert journal.unresolved() == 1
    assert_session_owned(service.runtime)

    service.stop()
    replacement = Runtime(service.settings)
    replacement.acquire()
    replacement.stop()


def test_normal_maintenance_keeps_terminal_running(service: BridgeService, writers) -> None:
    assert service.logs is not None
    writer = writers(service.logs.root / 'terminal.log')
    service.runtime.processes['terminal'] = writer
    start_monitor(service.runtime, True)

    service._maintain()

    assert writer.poll() is None
    assert service.status().error is None
    assert not service.runtime.stop_event.is_set()
    assert_session_owned(service.runtime)


@pytest.mark.parametrize('storage', ['artifacts', 'journal'])
def test_other_storage_failures_use_the_same_shutdown(
    service: BridgeService, writers, monkeypatch: pytest.MonkeyPatch, storage: str,
) -> None:
    assert service.logs is not None and service.artifacts is not None
    assert service.dispatcher is not None
    writer = writers(service.logs.root / 'terminal.log')
    service.runtime.processes['terminal'] = writer
    start_monitor(service.runtime, False)

    def unavailable() -> None:
        raise BridgeError('STORAGE_UNAVAILABLE', '离线容量故障')

    target, method = ((service.artifacts, 'clean') if storage == 'artifacts'
                      else (service.dispatcher.journal, 'maintain'))
    monkeypatch.setattr(target, method, unavailable)
    service._maintain()

    assert writer.wait(timeout=2) is not None
    status = service.status()
    assert status.error is not None
    assert status.error.code == 'STORAGE_UNAVAILABLE'
    assert_session_owned(service.runtime)


def test_storage_failure_and_explicit_stop_can_overlap(service: BridgeService, writers) -> None:
    assert service.logs is not None
    writer = writers(service.logs.root / 'terminal.log', 4096, grow=True)
    service.runtime.processes['terminal'] = writer
    start_monitor(service.runtime, False)
    barrier = threading.Barrier(2)

    def run(action) -> None:
        barrier.wait(timeout=3)
        action()

    with ThreadPoolExecutor(max_workers=2) as pool:
        maintenance = pool.submit(run, service._maintain)
        shutdown = pool.submit(run, service.stop)
        maintenance.result(timeout=5)
        shutdown.result(timeout=5)
    assert writer.wait(timeout=2) is not None
    replacement = Runtime(service.settings)
    replacement.acquire()
    replacement.stop()
