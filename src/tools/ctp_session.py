"""一个 CTP 连接的生命周期及同步等待；不重试、不缓存查询、不维护订单账本。"""

import secrets
import time
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryFile
from typing import Any

from src.base_types import ModeType
from src.tools.config_types import CtpAccountConfig, CtpConfig
from src.tools.ctp_callbacks import CtpCallbacks, CtpError, Pending
from src.tools.ctp_spi import create_api

ApiFactory = Callable[[CtpCallbacks], Any]
SESSION_METHODS = {"ReqAuthenticate", "ReqUserLogin", "ReqSettlementInfoConfirm"}


class CtpSession:
    def __init__(
        self,
        account: CtpAccountConfig,
        config: CtpConfig,
        mode: ModeType,
        factory: ApiFactory = create_api,
    ):
        self.account, self.config, self.mode = account, config, mode
        self.callbacks = CtpCallbacks(account, mode)
        self.login: dict[str, Any] = {}
        self.needs_reset = False
        self._ready_generation = -1
        self._request_id = 0
        self._order_ref = 0
        self._next_query_at = 0.0
        self.api: Any = None
        self._created = False
        self._init_called = False
        try:
            path = Path(config.flow_path) / mode
            path.mkdir(parents=True, exist_ok=True)
            # 原生库无法正常打开 flow 文件时可能退出进程，先检查目录可写。
            with TemporaryFile(dir=path):
                pass
            self.api = factory(self.callbacks)
            self.api.createFtdcTraderApi(
                str(path.resolve()) + "/", account.production_mode
            )
            self._created = True
            self.api.registerFront(account.trader_front)
            self.api.subscribePrivateTopic(2)  # QUICK 模式。
            self.api.subscribePublicTopic(2)
            self._init_called = True
            self.api.init()
        except Exception as exc:
            self.close()
            if isinstance(exc, CtpError):
                raise
            raise self.callbacks.error(
                503,
                "CTP_SDK_UNAVAILABLE",
                "CTP 初始化失败，请检查 ctp 依赖、共享库和 flow_path。",
            ) from exc

    @property
    def credentials(self) -> dict[str, str]:
        return {
            "BrokerID": self.account.broker_id,
            "InvestorID": self.account.investor_id,
        }

    @property
    def user_id(self) -> str:
        return self.account.user_id or self.account.investor_id

    def ensure_ready(self) -> None:
        state = self.callbacks
        if state.closed:
            raise state.error(503, "CTP_CLIENT_CLOSED", "CTP 连接已关闭。")
        if not state.connected.wait(self.config.connect_timeout_seconds):
            self.needs_reset = True
            raise state.error(503, "CTP_CONNECT_TIMEOUT", "等待 CTP 交易前置连接超时。")
        with state.lock:
            generation = state.generation
            if self._ready_generation == generation:
                return
        auth = {"BrokerID": self.account.broker_id, "UserID": self.user_id}
        try:
            if self.account.app_id and self.account.auth_code:
                self.request(
                    "ReqAuthenticate",
                    {
                        **auth,
                        "AppID": self.account.app_id,
                        "AuthCode": self.account.auth_code.get_secret_value(),
                    },
                )
            _, rows = self.request(
                "ReqUserLogin",
                {**auth, "Password": self.account.password.get_secret_value()},
            )
            if (
                len(rows) != 1
                or not {"FrontID", "SessionID", "TradingDay", "MaxOrderRef"}
                <= rows[0].keys()
            ):
                raise state.error(
                    502, "CTP_INVALID_RESPONSE", "CTP 登录回报缺少会话信息。"
                )
            self.login = rows[0]
            self._order_ref = max(
                self._order_ref, int(self.login["MaxOrderRef"].strip() or "0")
            )
            self.request(
                "ReqSettlementInfoConfirm",
                self.credentials,
            )
            with state.lock:
                if generation != state.generation or not state.connected.is_set():
                    raise state.error(
                        503, "CTP_DISCONNECTED", "登录过程中 CTP 连接中断。"
                    )
                self._ready_generation = generation
        except Exception:
            self.needs_reset = True
            raise

    def next_order_ref(self) -> str:
        self._order_ref += 1
        if self._order_ref > 999_999_999_999:
            raise self.callbacks.error(
                503, "CTP_ORDER_REF_EXHAUSTED", "CTP OrderRef 已超过 12 位数字。"
            )
        return str(self._order_ref)

    def request(
        self,
        method: str,
        fields: dict[str, Any],
        identity: dict[str, Any] | None = None,
    ) -> tuple[int, list[dict[str, Any]]]:
        state = self.callbacks
        if method.startswith("ReqQry"):
            delay = self._next_query_at - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            self._next_query_at = time.monotonic() + self.config.query_interval_seconds
        with state.lock:
            if (
                state.closed
                or not state.connected.is_set()
                or (
                    method not in SESSION_METHODS
                    and self._ready_generation != state.generation
                )
            ):
                raise state.error(
                    503, "CTP_DISCONNECTED", "CTP 当前连接未就绪，业务请求尚未发送。"
                )
            self._request_id += 1
            pending = Pending(method, self._request_id, identity)
            native_fields = dict(fields)
            if pending.write:
                # OrderMemo 为 char[13]；12 个 ASCII 字符提供 72 位随机关联标识。
                pending.order_memo = secrets.token_urlsafe(9)
                native_fields["OrderMemo"] = pending.order_memo
                native_fields["RequestID"] = pending.request_id
                if method == "ReqOrderAction":
                    native_fields["OrderActionRef"] = pending.request_id
            state.pending = pending
            try:
                # 回调可能先于 Req* 返回，先登记请求再发送。
                result = getattr(self.api, method[0].lower() + method[1:])(
                    native_fields, pending.request_id
                )
            except Exception as exc:
                self.needs_reset = True
                error = state.error(
                    502,
                    "OPERATION_STATUS_UNKNOWN"
                    if pending.write
                    else "CTP_REQUEST_FAILED",
                    "调用 CTP SDK 异常；已发送的写操作请先对账。",
                )
                state.pending = None
                raise error from exc
            if result != 0:
                self.needs_reset = result == -1
                error = state.error(
                    429 if result in {-2, -3} else 503,
                    "CTP_SEND_FAILED",
                    "CTP Req* 返回非零，请求未成功发送；服务未重试。",
                    return_code=result,
                )
                state.pending = None
                raise error
        pending.event.wait(self.config.request_timeout_seconds)
        with state.lock:
            try:
                if not pending.event.is_set():
                    self.needs_reset = True
                    raise state.error(
                        502 if pending.write else 504,
                        "OPERATION_STATUS_UNKNOWN" if pending.write else "CTP_TIMEOUT",
                        "等待 CTP 完整回报超时；写操作可能已生效，请先查询订单及成交。",
                    )
                if pending.error:
                    raise pending.error
                return pending.request_id, pending.rows
            finally:
                state.pending = None

    def close(self) -> None:
        self.callbacks.close()
        api, self.api = self.api, None
        if api is not None and self._created:
            # CTP 在创建后、init 前释放会崩溃。初始化失败时也需先启动线程再退出。
            if not self._init_called:
                self._init_called = True
                api.init()
            # exit 清理积压回调并等待工作线程结束，不能持有 callback lock。
            api.exit()
