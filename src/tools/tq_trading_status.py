"""跟踪 TQ 独立交易状态连接，防止重连前的 SDK 缓存被当作当前状态。"""

import aiohttp
from tqsdk import TqApi

from src.tools.tq_errors import TqLegacyMetadataCallForbidden
from src.tools.tq_status_snapshot import TqStatusSnapshot


class TradingStatusTqApi(TqApi):
    def __init__(
        self,
        *args,
        status_snapshot: TqStatusSnapshot | None = None,
        proxy_url: str | None = None,
        **kwargs,
    ):
        self.status_snapshot = status_snapshot or TqStatusSnapshot()
        self._proxy_url = proxy_url
        super().__init__(*args, **kwargs)

    @property
    def _http_session(self):
        # SDK 自有异步合约/交易状态查询也必须沿用同一代理设置。
        if self._http_session_internal is None or self._http_session_internal.closed:
            self._http_session_internal = aiohttp.ClientSession(
                headers=self._base_headers, proxy=self._proxy_url, trust_env=False
            )
        return self._http_session_internal

    def has_trading_status_permission(self) -> bool:
        """与 get_trading_status 使用同一权限检查，仅由 SDK 线程读取。"""
        return self._auth is not None and self._auth._has_feature("tq_trading_status")

    def get_trading_calendar(self, *args, **kwargs):
        raise TqLegacyMetadataCallForbidden("get_trading_calendar")

    def query_his_cont_quotes(self, *args, **kwargs):
        raise TqLegacyMetadataCallForbidden("query_his_cont_quotes")

    def query_symbol_info(self, *args, **kwargs):
        # 当前关联可与真实行情及历史源不同步，业务统一使用历史事件源。
        raise TqLegacyMetadataCallForbidden("query_symbol_info")

    async def _fetch_msg(self):
        await super()._fetch_msg()
        # SDK 没有公开连接状态 API。这里在 _merge_diff 去重之前观察收到的数据，
        # 重连后即使状态值与断线前相同，也能确认它来自新连接。
        # 此处只读取 SDK 数据包，不修改 SDK 缓存、订阅或重连行为。
        self.status_snapshot.observe(self._pending_diffs)
