import asyncio

import httpx
import pytest

from src.tools.config_loader import ConfigError, load_config
from src.tools.config_types import TqConfig
from src.tools.tq_manager import TqManager
from src.tools.tq_metadata_source import MetadataSource
from Test.test_tq_metadata_conversion import EVENTS, HOLIDAYS

PROXY = "http://user:password@proxy.example:3128"


@pytest.mark.parametrize("setting", ["", "enable_proxy = false\n"])
def test_old_and_explicit_local_config_use_direct_connections(tmp_path, setting):
    path = tmp_path / "config.toml"
    path.write_text(
        'SECRET = "offline"\nservice_whitelist = [{service="tq"}]\n'
        '[tq]\nusername = "offline"\n' + setting
    )
    config = load_config(path)
    assert config.tq is not None and config.tq.enable_proxy is False


def test_active_tq_proxy_requires_address_without_disclosing_values(tmp_path):
    path = tmp_path / "config.toml"
    text = (
        'SECRET = "private-sentinel"\nservice_whitelist = [{service="tq"}]\n'
        '[tq]\nusername = "private-sentinel"\nenable_proxy = true\n'
    )
    path.write_text(text)
    with pytest.raises(ConfigError, match="Invalid TOML configuration") as caught:
        load_config(path)
    assert "private-sentinel" not in str(caught.value)
    path.write_text(text + f'[proxy]\nhttp = "{PROXY}"\n')
    config = load_config(path)
    assert config.tq is not None and config.tq.enable_proxy is True


@pytest.mark.parametrize("enabled", [False, True])
def test_manager_freezes_same_proxy_for_sdk_and_metadata(tmp_path, enabled):
    config = TqConfig(enable_proxy=enabled)
    manager = TqManager(config, tmp_path / "tq.lock", proxy_url=PROXY)
    config.enable_proxy = not enabled
    expected = PROXY if enabled else None
    assert manager._client.proxy_url == expected
    assert manager._metadata_query().source._proxy_url == expected
    asyncio.run(manager.close_metadata())
    manager.close()


@pytest.mark.parametrize("proxy", [None, PROXY])
@pytest.mark.parametrize("kind", ["calendar", "mapping"])
def test_metadata_download_selects_proxy_and_ignores_environment(
    monkeypatch, proxy, kind
):
    monkeypatch.setenv("HTTPS_PROXY", "http://environment.example:8080")
    calls = []

    def transport(**kwargs):
        selected = kwargs.get("proxy")

        def respond(request):
            calls.append((selected, request.url.host))
            payload = HOLIDAYS if kind == "calendar" else EVENTS
            return httpx.Response(200, json=payload)

        return httpx.MockTransport(respond)

    monkeypatch.setattr("httpx._client.AsyncHTTPTransport", transport)

    async def run():
        source = MetadataSource(proxy_url=proxy)
        try:
            await source.fetch(kind, {"Authorization": "Bearer offline"})
            assert len(calls) == 1 and calls[0][1] == "files.shinnytech.com"
            selected = calls[0][0]
            if proxy is None:
                assert selected is None
            else:
                assert str(selected.url) == "http://proxy.example:3128"
                assert selected.raw_auth == (b"user", b"password")
        finally:
            await source.close()
        assert source._client is not None and source._client.is_closed

    asyncio.run(run())


@pytest.mark.parametrize("proxy", [None, PROXY])
def test_sdk_async_http_session_uses_same_proxy(monkeypatch, proxy):
    import aiohttp
    from tqsdk import TqAuth

    from src.tools.tq_trading_status import TradingStatusTqApi

    monkeypatch.setenv("HTTPS_PROXY", "http://environment.example:8080")
    calls = []

    class CapturedRequest(Exception):
        pass

    async def connect(self, request, *args, **kwargs):
        calls.append(request)
        raise CapturedRequest()

    monkeypatch.setattr(aiohttp.TCPConnector, "connect", connect)

    async def run():
        api = object.__new__(TradingStatusTqApi)
        api._proxy_url = proxy
        api._auth = TqAuth("offline", "offline")
        api._auth._access_token = "offline"
        api._http_session_internal = None
        session = api._http_session
        try:
            assert api._http_session is session
            with pytest.raises(CapturedRequest):
                await session.get("https://api.shinnytech.com/offline")
            assert len(calls) == 1
            assert (str(calls[0].proxy) if calls[0].proxy else None) == proxy
            assert calls[0].headers["Authorization"] == "Bearer offline"
            if proxy:
                assert calls[0].proxy.user == "user"
                assert calls[0].proxy.password == "password"
        finally:
            await session.close()

    asyncio.run(run())
