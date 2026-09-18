"""跟踪 TQ 独立交易状态连接，防止重连前的 SDK 缓存被当作当前状态。"""

from tqsdk import TqApi

from src.tools.tq_status_snapshot import TqStatusSnapshot


class TradingStatusTqApi(TqApi):
    def __init__(
        self, *args, status_snapshot: TqStatusSnapshot | None = None, **kwargs
    ):
        self.status_snapshot = status_snapshot or TqStatusSnapshot()
        super().__init__(*args, **kwargs)

    def has_trading_status_permission(self) -> bool:
        """与 get_trading_status 使用同一权限检查，仅由 SDK 线程读取。"""
        return self._auth is not None and self._auth._has_feature("tq_trading_status")

    async def _fetch_msg(self):
        await super()._fetch_msg()
        # SDK 没有公开连接状态 API。这里在 _merge_diff 去重之前观察收到的数据，
        # 重连后即使状态值与断线前相同，也能确认它来自新连接。
        # 此处只读取 SDK 数据包，不修改 SDK 缓存、订阅或重连行为。
        self.status_snapshot.observe(self._pending_diffs)
