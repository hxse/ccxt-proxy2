"""CTP 依赖来源约束和升级时的响应字段检查，全部离线。"""

import io
import json
import ssl
import tarfile

import pytest

from scripts import build_vnpy_ctp_source as builder


def test_tampered_upstream_is_rejected_before_unpacking():
    with pytest.raises(ValueError, match="checksum mismatch"):
        builder.build_archive(b"tampered", {"sha256": "0" * 64})


@pytest.mark.parametrize(
    "url", ["http://files.pythonhosted.org/file", "https://other.example/file"]
)
def test_source_download_rejects_unexpected_origins(url, monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("Unexpected origin must be rejected before a request")

    monkeypatch.setattr(builder, "urlopen", unexpected)
    with pytest.raises(ValueError, match="origin"):
        builder.download(url, "files.pythonhosted.org")


def test_source_download_keeps_tls_verification_enabled(monkeypatch):
    url = "https://files.pythonhosted.org/source.tar.gz"

    class Source(io.BytesIO):
        url: str

    def open_source(request_url, *, timeout, context):
        assert request_url == url and timeout == 60
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname
        response = Source(b"verified source")
        response.url = url
        return response

    monkeypatch.setattr(builder, "urlopen", open_source)
    assert builder.download(url, "files.pythonhosted.org") == b"verified source"


def test_upgrade_selects_official_non_yanked_source(monkeypatch):
    manifest = json.loads((builder.VENDOR / "upstream.json").read_text())
    data = {
        "info": {"version": manifest["version"]},
        "urls": [
            {"packagetype": "bdist_wheel"},
            {"packagetype": "sdist", "yanked": True},
            {
                "packagetype": "sdist",
                "url": manifest["url"],
                "digests": {"sha256": manifest["sha256"]},
            },
        ],
    }

    def download(url, hostname):
        assert url == "https://pypi.org/pypi/vnpy_ctp/json"
        assert hostname == "pypi.org"
        return json.dumps(data).encode()

    monkeypatch.setattr(builder, "download", download)
    assert builder.published_release("latest") == manifest


def test_upstream_response_changes_require_explicit_model_review(tmp_path):
    path = next(builder.VENDOR.glob("*.tar.gz"))
    with tarfile.open(fileobj=io.BytesIO(path.read_bytes())) as archive:
        member = next(m for m in archive if m.name.endswith("/" + builder.CPP))
        source = archive.extractfile(member)
        assert source is not None
        code = source.read().decode()
    target = tmp_path / builder.CPP
    target.parent.mkdir(parents=True)
    target.write_text(code)
    builder.check_response_fields(tmp_path)
    position = code.index("void TdApi::processRspQryOrder(")
    field = code.index('data["BrokerID"]', position)
    target.write_text(code[:field] + 'data["FutureField"] = 0;\n' + code[field:])
    with pytest.raises(ValueError, match="Review CtpOrder.*FutureField"):
        builder.check_response_fields(tmp_path)


@pytest.mark.parametrize("changed_part", ["allocation", "cast", "delete", "assignment"])
def test_upstream_ownership_changes_stop_rebuilding(changed_part):
    source = """
CThostFtdcOrderField *task_data = new CThostFtdcOrderField();
task.task_data = task_data;
CThostFtdcOrderField *task_data = (CThostFtdcOrderField*)task->task_data;
delete task_data;
CThostFtdcRspInfoField *task_error = new CThostFtdcRspInfoField();
task.task_error = task_error;
CThostFtdcRspInfoField *task_error = (CThostFtdcRspInfoField*)task->task_error;
delete task_error;
"""
    changes = {
        "allocation": ("new CThostFtdcOrderField()", "allocate_order()"),
        "cast": ("(CThostFtdcOrderField*)task->task_data", "get_order(task)"),
        "delete": ("delete task_data;", "free_order(task_data);"),
        "assignment": ("task.task_data = task_data;", "task.set_data(task_data);"),
    }
    before, after = changes[changed_part]
    with pytest.raises(
        ValueError, match="Review upstream callback ownership for task_data"
    ):
        builder.own_callback_payloads(source.replace(before, after))
