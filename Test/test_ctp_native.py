"""VeighNa 交易模块的离线边界检查；不登录，只连接本机假 TCP 前置。"""

import importlib.util
import re
import shutil
import subprocess
import sys
import sysconfig
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.ctp_records_account import CtpPosition, CtpTradingAccount
from src.ctp_records_trading import CtpOrder, CtpTrade
from src.responses_ctp import CtpInstrumentStatus
from src.tools import ctp_native
from src.tools.ctp_callbacks import CtpCallbacks, CtpError
from src.tools.ctp_spi import create_api
from Test.ctp_fakes import ctp_config

ROOT = Path(__file__).resolve().parents[1]
HAS_SDK = importlib.util.find_spec("vnpy_ctp") is not None
requires_sdk = pytest.mark.skipif(
    not HAS_SDK, reason="optional ctp extra is not installed"
)


@pytest.mark.parametrize("installed_version", ["6.7.11.4", "6.7.11.4+ccxtproxy.1"])
def test_sdk_factory_rejects_an_unpatched_installation(
    tmp_path, monkeypatch, installed_version
):
    monkeypatch.setattr(ctp_native, "_td_api", None)
    monkeypatch.setattr(
        ctp_native.metadata,
        "distribution",
        lambda name: SimpleNamespace(version=installed_version),
    )
    config = ctp_config(tmp_path)
    callbacks = CtpCallbacks(config.test, "sandbox")
    with pytest.raises(CtpError) as caught:
        create_api(callbacks)
    assert caught.value.status_code == 503
    assert caught.value.detail["code"] == "CTP_SDK_UNAVAILABLE"
    assert "uv sync --locked --extra ctp" in caught.value.detail["message"]


@requires_sdk
@pytest.mark.parametrize(
    "scenario", ["callback_waiting_for_gil", "callback_in_progress"]
)
def test_native_release_finishes_while_a_disconnect_callback_needs_the_gil(scenario):
    try:
        result = subprocess.run(
            [sys.executable, "-m", "Test.ctp_release_probe", scenario],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=12,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("CTP exit deadlocked with a native callback waiting for the GIL")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "released" in result.stdout


@pytest.fixture(scope="module")
def native_sources(tmp_path_factory):
    directory = tmp_path_factory.mktemp("vnpy-ctp-source")
    archive_path = next((ROOT / "vendor/vnpy_ctp").glob("*.tar.gz"))
    suffixes = {
        "vnctp.h": "/api/vnctp/vnctp.h",
        "ThostFtdcUserApiStruct.h": "/api/include/ctp/ThostFtdcUserApiStruct.h",
    }
    with tarfile.open(archive_path) as archive:
        for name, suffix in suffixes.items():
            member = next(member for member in archive if member.name.endswith(suffix))
            source = archive.extractfile(member)
            assert source is not None
            (directory / name).write_bytes(source.read())
    return directory


def test_documented_records_cover_all_native_fields(native_sources):
    header = (native_sources / "ThostFtdcUserApiStruct.h").read_text(errors="replace")
    for native_name, model in [
        ("OrderField", CtpOrder),
        ("TradeField", CtpTrade),
        ("InvestorPositionField", CtpPosition),
        ("TradingAccountField", CtpTradingAccount),
        ("InstrumentStatusField", CtpInstrumentStatus),
    ]:
        body = re.search(
            r"struct CThostFtdc" + native_name + r"\s*\{(.*?)\n\};", header, re.S
        )
        assert body, native_name
        fields = {
            name
            for name in re.findall(r"\bTThostFtdc\w+\s+(\w+)\s*;", body.group(1))
            if not name.startswith("reserve")
        }
        assert set(model.model_fields) == fields


@requires_sdk
def test_status_spi_copies_callback_dictionary_and_invalidates_on_disconnect(tmp_path):
    config = ctp_config(tmp_path)
    callbacks = CtpCallbacks(config.test, "sandbox")
    api = create_api(callbacks)  # No Create/Init, no native connection.
    api.onFrontConnected()
    record = dict(
        ExchangeID="SHFE",
        InstrumentID="rb",
        InstrumentStatus="2",
        EnterTime="09:00:00",
        EnterReason="1",
    )
    api.onRtnInstrumentStatus(record)
    record["InstrumentStatus"] = "6"
    connected, snapshot = callbacks.status_snapshot.read("SHFE", "rb")
    assert connected and snapshot is not None and snapshot["InstrumentStatus"] == "2"
    api.onFrontDisconnected(0)
    assert callbacks.status_snapshot.read("SHFE", "rb") == (False, None)


@requires_sdk
@pytest.mark.parametrize(
    ("method", "callback"),
    [
        ("ReqQryInstrument", "onRspQryInstrument"),
        ("ReqQryDepthMarketData", "onRspQryDepthMarketData"),
    ],
)
def test_price_query_callbacks_reach_pending_request(tmp_path, method, callback):
    from src.tools.ctp_callbacks import Pending

    config = ctp_config(tmp_path)
    callbacks = CtpCallbacks(config.test, "sandbox")
    callbacks.pending = Pending(method, 42, None)
    api = create_api(callbacks)
    data = {"InstrumentID": "m2701", "ExchangeID": "DCE"}
    getattr(api, callback)(data, {}, 42, True)
    data["InstrumentID"] = "changed"
    assert callbacks.pending.event.is_set()
    assert callbacks.pending.rows == [{"InstrumentID": "m2701", "ExchangeID": "DCE"}]


@requires_sdk
def test_only_the_trading_extension_is_installed_and_loaded():
    code = """
import importlib.metadata as metadata
import sys
from src.tools.ctp_native import load_td_api
assert load_td_api() is load_td_api()
installed = {d.metadata['Name'].lower().replace('_', '-') for d in metadata.distributions()}
assert not installed & {'vnpy', 'pyside6', 'pyqt5', 'pyqt6', 'ctpwrapper'}
assert 'vnpy_ctp' not in sys.modules
assert not any(name.startswith(('vnpy.', 'vnpy_ctp.gateway', 'vnpy_ctp.api.vnctpmd')) for name in sys.modules)
dist = metadata.distribution('vnpy_ctp')
assert not any('vnctpmd.' in str(path) or 'thostmduserapi' in str(path) for path in dist.files)
print('trading API only')
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@requires_sdk
def test_native_sdk_initialization_and_release_without_any_front():
    code = """
import tempfile
from src.tools.ctp_native import load_td_api
with tempfile.TemporaryDirectory() as path:
    for _ in range(20):
        api = load_td_api()()
        api.createFtdcTraderApi(path + '/', True)
        api.subscribePrivateTopic(2)
        api.subscribePublicTopic(2)
        api.init()
        api.exit()
print('released')
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "released" in result.stdout


@requires_sdk
def test_forwarded_requests_are_accepted_by_native_bindings_without_a_front(tmp_path):
    from src.tools.ctp_manager import CtpManager
    from src.types_ctp import (
        CtpAccountQuery,
        CtpCancelByExchange,
        CtpCancelBySession,
        CtpLimitOrderRequest,
        CtpMarketOrderRequest,
        CtpOrderQuery,
        CtpPositionQuery,
        CtpTradeQuery,
    )
    from Test.ctp_fakes import FakeFactory

    fake = FakeFactory()
    manager = CtpManager(ctp_config(tmp_path), fake)
    client = manager.get_client("sandbox")
    values = dict(
        exchange_id="SHFE", instrument_id="rb2610", side="buy", offset="open", volume=1
    )
    try:
        client.create_order(CtpMarketOrderRequest.model_validate(values))
        client.create_order(
            CtpLimitOrderRequest.model_validate(values | {"price": 3500})
        )
        client.cancel_order(
            CtpCancelByExchange(
                by="exchange_order",
                exchange_id="SHFE",
                instrument_id="rb2610",
                order_sys_id="       42",
            )
        )
        client.cancel_order(
            CtpCancelBySession(
                by="session_order",
                exchange_id="SHFE",
                instrument_id="rb2610",
                front_id=7,
                session_id=9,
                order_ref="11",
            )
        )
        client.fetch_orders(CtpOrderQuery())
        client.fetch_trades(CtpTradeQuery())
        client.fetch_positions(CtpPositionQuery())
        client.fetch_balance(CtpAccountQuery())
    finally:
        manager.close()
    api = ctp_native.load_td_api()()
    api.createFtdcTraderApi(str(tmp_path) + "/", True)
    api.init()
    checked = set()
    try:
        for method, fields, request_id in fake.apis[0].requests:
            if method not in {
                "ReqOrderInsert",
                "ReqOrderAction",
                "ReqQryOrder",
                "ReqQryTrade",
                "ReqQryInvestorPosition",
                "ReqQryTradingAccount",
            }:
                continue  # Never call real authentication/login/collection, even offline.
            result = getattr(api, method[0].lower() + method[1:])(fields, request_id)
            assert (
                result == -1
            )  # Argument conversion succeeded; there is no front to send to.
            checked.add(method)
    finally:
        api.exit()
    assert len(checked) == 6


def test_native_chinese_decoding_without_system_gb18030_locale(native_sources):
    import pybind11

    compiler = shutil.which("c++")
    if compiler is None or sys.platform != "linux":
        pytest.skip("Native codec probe requires a Linux C++ compiler")
    source = native_sources / "codec_probe.cpp"
    source.write_text("""#include "vnctp.h"
PYBIND11_MODULE(_ctp_codec_probe, m) {
    m.def("decode", [](pybind11::bytes data) { return toUtf(data.cast<std::string>()); });
}
""")
    module_path = native_sources / (
        "_ctp_codec_probe" + sysconfig.get_config_var("EXT_SUFFIX")
    )
    result = subprocess.run(
        [
            compiler,
            "-shared",
            "-fPIC",
            "-std=c++17",
            "-O0",
            "-I" + pybind11.get_include(),
            "-I" + sysconfig.get_path("include"),
            str(source),
            "-o",
            str(module_path),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    spec = importlib.util.spec_from_file_location("_ctp_codec_probe", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.decode("正常：报单已接受".encode("gb18030")) == "正常：报单已接受"
    assert module.decode(b"bad\xff") == "bad\ufffd"
    assert module.decode(b"") == ""
