import asyncio
from io import BytesIO
from pathlib import Path

import pytest
from fastapi import HTTPException, UploadFile
from fastapi.responses import FileResponse

from src.router import file_handler


def _upload(filename: str, content: bytes) -> UploadFile:
    return UploadFile(file=BytesIO(content), filename=filename)


def test_upload_list_and_download_stay_inside_strategy_directory(
    temp_dir, monkeypatch
):
    base = temp_dir.resolve()
    monkeypatch.setattr(file_handler, "BASE_DIR", base)

    uploaded = asyncio.run(
        file_handler.upload_file("nested", _upload("strategy.py", b"print('ok')"))
    )
    listed = asyncio.run(file_handler.list_files())
    downloaded = asyncio.run(
        file_handler.download_file(path="nested", filename="strategy.py")
    )

    assert uploaded == {
        "filename": "nested/strategy.py",
        "message": "file uploaded successfully.",
    }
    assert listed == {"files": [{"filename": "strategy.py", "path": "nested"}]}
    assert isinstance(downloaded, FileResponse)
    assert Path(downloaded.path) == base / "nested/strategy.py"


@pytest.mark.parametrize("path", ["..", "../outside", "nested/../../outside"])
def test_upload_rejects_path_traversal_without_writing(temp_dir, monkeypatch, path):
    base = temp_dir.resolve()
    monkeypatch.setattr(file_handler, "BASE_DIR", base)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(file_handler.upload_file(path, _upload("escape.txt", b"secret")))

    assert exc_info.value.status_code == 400
    assert list(base.rglob("escape.txt")) == []


@pytest.mark.parametrize(
    ("path", "filename"),
    [("..", "escape.txt"), ("nested", "../../escape.txt")],
)
def test_download_rejects_path_traversal(temp_dir, monkeypatch, path, filename):
    monkeypatch.setattr(file_handler, "BASE_DIR", temp_dir.resolve())

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(file_handler.download_file(path=path, filename=filename))

    assert exc_info.value.status_code == 400


def test_download_distinguishes_missing_name_and_missing_file(temp_dir, monkeypatch):
    monkeypatch.setattr(file_handler, "BASE_DIR", temp_dir.resolve())

    with pytest.raises(HTTPException) as missing_name:
        asyncio.run(file_handler.download_file())
    with pytest.raises(HTTPException) as missing_file:
        asyncio.run(file_handler.download_file(filename="missing.txt"))

    assert missing_name.value.status_code == 400
    assert missing_file.value.status_code == 404


def test_symlink_cannot_escape_strategy_directory(temp_dir, monkeypatch):
    base = temp_dir / "strategy"
    outside = temp_dir / "outside"
    base.mkdir()
    outside.mkdir()
    (base / "linked").symlink_to(outside, target_is_directory=True)
    (outside / "existing.txt").write_text("outside")
    monkeypatch.setattr(file_handler, "BASE_DIR", base.resolve())

    with pytest.raises(HTTPException) as upload_error:
        asyncio.run(
            file_handler.upload_file("linked", _upload("new.txt", b"outside"))
        )
    with pytest.raises(HTTPException) as download_error:
        asyncio.run(
            file_handler.download_file(path="linked", filename="existing.txt")
        )

    assert upload_error.value.status_code == 400
    assert download_error.value.status_code == 400
    assert not (outside / "new.txt").exists()
