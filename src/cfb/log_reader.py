"""跟随已脱敏的受管 JSON 日志，覆盖多个 writer 和容量轮转。"""

import json
import os
import sys
import time
from pathlib import Path
from typing import BinaryIO


def _tail_start(stream: BinaryIO, count: int) -> int:
    """只从尾部读足够的完整行；未写完的末行留给后续读取。"""
    end = position = stream.seek(0, 2)
    data = b""
    while position and data.count(b"\n") <= count:
        size = min(position, 8192)
        position -= size
        stream.seek(position)
        data = stream.read(size) + data
    return end - len(b"\n".join(data.split(b"\n")[-count - 1:]))


class LogReader:
    def __init__(self, root: Path, tail: int = 100):
        self.root = root
        self.tail = tail
        self.initial = True
        self.cursors: dict[Path, tuple[int, int, int]] = {}

    def read(self) -> list[str]:
        paths = {path for path in self.root.glob("cfb-*.jsonl")
                 if path.is_file() and not path.is_symlink()}
        self.cursors = {path: value for path, value in self.cursors.items() if path in paths}
        rows: list[tuple[str, str]] = []
        for path in sorted(paths):
            try:
                with path.open("rb") as stream:
                    info = os.fstat(stream.fileno())
                    identity = (info.st_dev, info.st_ino)
                    prior = self.cursors.get(path)
                    if self.initial:
                        position = _tail_start(stream, self.tail)
                    elif prior and prior[:2] == identity and prior[2] <= info.st_size:
                        position = prior[2]
                    else:
                        position = 0
                    stream.seek(position)
                    while line := stream.readline():
                        if not line.endswith(b"\n"):
                            break
                        record = json.loads(line)
                        if not isinstance(record, dict) or not isinstance(record.get("timestamp"), str):
                            raise ValueError("invalid managed log record")
                        rows.append((record["timestamp"], line.decode("utf-8")))
                        position = stream.tell()
                    self.cursors[path] = (*identity, position)
            except FileNotFoundError:
                # LogStore 可在轮转或容量治理时删除已列出的文件。
                self.cursors.pop(path, None)
        rows.sort(key=lambda item: item[0])
        if self.initial:
            rows = rows[-self.tail:]
            self.initial = False
        return [line for _, line in rows]


def main() -> int:
    reader = LogReader(Path("/data/logs"))
    try:
        while True:
            for line in reader.read():
                print(line, end="", flush=True)
            time.sleep(.25)
    except KeyboardInterrupt:
        return 130
    except BrokenPipeError:
        return 0
    except (OSError, ValueError):
        print("CFB 受管日志读取失败，请检查日志目录和 JSON 格式。", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
