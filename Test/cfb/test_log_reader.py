"""日志读取覆盖真实脱敏 writer、并存写入、轮转删除及半行写入。"""

import json
import logging

import pytest
from pydantic import SecretStr

from src.cfb import log_reader
from src.cfb.config import (
    AccountConfig,
    AccountsConfig,
    BridgeConfig,
    LoggingConfig,
    Settings,
)
from src.cfb.log_reader import LogReader
from src.cfb.logging_store import LogStore


def record(message):
    return logging.LogRecord("offline", logging.INFO, __file__, 0, message, (), None)


def messages(lines):
    return [json.loads(line)["message"] for line in lines]


def settings(root, **kwargs):
    return Settings(bridge=BridgeConfig(data_dir=root), **kwargs)


def test_tail_and_follow_cover_both_active_writers_without_duplicates(tmp_path):
    first, second = LogStore(settings(tmp_path)), LogStore(settings(tmp_path))
    for writer, message in ((first, "old"), (second, "worker"), (first, "daemon")):
        writer.emit(record(message))
    reader = LogReader(first.root, tail=2)
    assert messages(reader.read()) == ["worker", "daemon"]
    assert reader.read() == []
    first.emit(record("daemon-append"))
    second.emit(record("worker-append"))
    assert messages(reader.read()) == ["daemon-append", "worker-append"]
    assert reader.read() == []


def test_real_rotation_and_budget_deletion_remain_followable(tmp_path):
    sink = LogStore(settings(tmp_path,
        logging=LoggingConfig(max_file_bytes=512, max_total_bytes=1024)))
    reader = LogReader(sink.root)
    assert reader.read() == []
    initial = sink.current
    for index in range(12):
        sink.emit(record(f"entry-{index}"))
        assert not sink.failed
        assert messages(reader.read()) == [f"entry-{index}"]
        assert reader.read() == []
    assert sink.current != initial and not initial.exists()


def test_reader_preserves_logstore_redaction_and_ignores_other_files(tmp_path):
    sink = LogStore(settings(tmp_path, accounts=AccountsConfig(sandbox=AccountConfig(
        username=SecretStr("offline-account"), password=SecretStr("offline-password")))))
    sink.emit(record("offline-account used offline-password"))
    (sink.root / "terminal.log").write_text("raw-secret")
    outside = tmp_path / "outside.jsonl"
    outside.write_text("private-file")
    (sink.root / "cfb-link.jsonl").symlink_to(outside)
    result = LogReader(sink.root).read()
    assert messages(result) == ["[redacted] used [redacted]"]
    assert "offline-account" not in "".join(result)
    assert "offline-password" not in "".join(result)


def test_initial_tail_and_follow_wait_for_complete_unicode_line(tmp_path):
    path = tmp_path / "cfb-offline.jsonl"
    complete = [json.dumps({"timestamp": str(i).zfill(4), "message": "旧日志" * 100},
                           ensure_ascii=False).encode() + b"\n" for i in range(30)]
    pending = json.dumps({"timestamp": "0030", "message": "末行"}, ensure_ascii=False).encode()
    split = pending.index("末".encode()) + 1
    path.write_bytes(b"".join(complete) + pending[:split])
    reader = LogReader(tmp_path, tail=2)
    assert reader.read() == [line.decode() for line in complete[-2:]]
    assert reader.read() == []
    with path.open("ab") as stream:
        stream.write(pending[split:] + b"\n")
    assert messages(reader.read()) == ["末行"]
    assert reader.read() == []


def test_new_file_after_empty_directory_and_corrupt_record(tmp_path):
    root = tmp_path / "logs"
    reader = LogReader(root)
    assert reader.read() == []
    root.mkdir()
    path = root / "cfb-new.jsonl"
    path.write_text('{"timestamp":"now","message":"new"}\n')
    assert messages(reader.read()) == ["new"]
    with path.open("a") as stream:
        stream.write('invalid-json\n')
    with pytest.raises(ValueError):
        reader.read()


def test_log_command_prints_history_then_follows_new_records(tmp_path, monkeypatch, capsys):
    sink = LogStore(settings(tmp_path))
    sink.emit(record("history"))
    reader = LogReader(sink.root)
    monkeypatch.setattr(log_reader, "LogReader", lambda root: reader)
    rounds = []

    def next_poll(seconds):
        if rounds:
            raise KeyboardInterrupt
        rounds.append(seconds)
        sink.emit(record("followed"))

    monkeypatch.setattr(log_reader.time, "sleep", next_poll)
    assert log_reader.main() == 130
    assert messages(capsys.readouterr().out.splitlines()) == ["history", "followed"]
