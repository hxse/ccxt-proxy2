"""CTP 回调桥接：每个连接只有一个在途 Req*，状态快照独立读取。"""

import math
import threading
from dataclasses import dataclass, field
from typing import Any

from fastapi import HTTPException

from src.base_types import ModeType
from src.responses_ctp import CtpErrorDetail
from src.tools.config_types import CtpAccountConfig
from src.tools.ctp_status_snapshot import CtpStatusSnapshot


class CtpError(HTTPException):
    detail: dict[str, Any]

    def __init__(
        self, status: int, code: str, message: str, mode: ModeType, **context: Any
    ):
        super().__init__(
            status,
            CtpErrorDetail(
                code=code, message=message, mode=mode, **context
            ).model_dump(),
        )


def snapshot(record: Any) -> dict[str, Any]:
    """复制 VeighNa 回调字典，只做 JSON 所需转换；不保存可变的上游对象。"""
    result = {}
    for key, value in record.items():
        if key.startswith("reserve"):
            continue
        if isinstance(value, bytes):
            value = value.decode("gbk", errors="replace")
        if isinstance(value, str):
            value = value.rstrip("\x00")  # 保留 OrderSysID 的前导空格。
        if isinstance(value, float) and (
            not math.isfinite(value) or abs(value) > 1e308
        ):
            value = None
        result[key] = value
    return result


@dataclass
class Pending:
    method: str
    request_id: int
    identity: dict[str, Any] | None
    order_memo: str | None = None
    event: threading.Event = field(default_factory=threading.Event)
    rows: list[dict[str, Any]] = field(default_factory=list)
    error: CtpError | None = None

    @property
    def write(self) -> bool:
        return self.method in {"ReqOrderInsert", "ReqOrderAction"}

    def fail(self, error: CtpError) -> None:
        if not self.event.is_set():
            self.error = error
            self.event.set()


class CtpCallbacks:
    def __init__(self, account: CtpAccountConfig, mode: ModeType):
        self.mode = mode
        self.lock = threading.RLock()
        self.connected = threading.Event()
        self.generation = 0
        self.closed = False
        self.pending: Pending | None = None
        self.status_snapshot = CtpStatusSnapshot()
        self._account_identity = (account.broker_id, account.investor_id)
        self._secrets = [account.password.get_secret_value()]
        if account.auth_code:
            self._secrets.append(account.auth_code.get_secret_value())

    def error(self, status: int, code: str, message: str, **context: Any) -> CtpError:
        for secret in sorted(self._secrets, key=len, reverse=True):
            message = message.replace(secret, "***")
        if self.pending is not None:
            context = {
                "request_id": self.pending.request_id,
                "order_identity": self.pending.identity,
                **context,
            }
        return CtpError(status, code, message, self.mode, **context)

    def on_connected(self) -> None:
        with self.lock:
            if not self.closed:
                self.connected.set()
                self.status_snapshot.connect()

    def on_disconnected(self) -> None:
        # 先使快照失效，即使业务回调锁正在被 Req* 占用，状态读取也不等它。
        self.status_snapshot.disconnect()
        with self.lock:
            self.connected.clear()
            self.generation += 1
            # 与并发的连接回调最终保持同一顺序；前一次失效保证 HTTP 不等待此锁。
            self.status_snapshot.disconnect()
            if self.pending is not None:
                write = self.pending.write
                self.pending.fail(
                    self.error(
                        502 if write else 503,
                        "OPERATION_STATUS_UNKNOWN" if write else "CTP_DISCONNECTED",
                        "CTP 连接中断；已发送的写操作可能已生效，请先查询订单与成交。",
                    )
                )

    def on_instrument_status(self, data: Any) -> None:
        """公共流按品种推送；复制回调字典，仅保留当前连接的最新通知。"""
        if data:
            self.status_snapshot.update(snapshot(data))

    def _reject(self, pending: Pending, info: dict[str, Any]) -> None:
        code = {
            "ReqOrderInsert": "CTP_ORDER_REJECTED",
            "ReqOrderAction": "CTP_CANCEL_REJECTED",
            "ReqAuthenticate": "CTP_AUTH_FAILED",
            "ReqUserLogin": "CTP_AUTH_FAILED",
            "ReqSettlementInfoConfirm": "CTP_SETTLEMENT_FAILED",
        }.get(pending.method, "CTP_QUERY_FAILED")
        status = (
            409 if pending.method == "ReqOrderAction" else 422 if pending.write else 502
        )
        pending.fail(
            self.error(
                status,
                code,
                str(info.get("ErrorMsg") or "CTP 拒绝请求。"),
                ctp_error_id=info.get("ErrorID"),
            )
        )

    def on_response(
        self, method: str, data: Any, info: Any, request_id: int, last: bool
    ) -> None:
        with self.lock:
            pending = self.pending
            if (
                pending is None
                or pending.event.is_set()
                or pending.request_id != request_id
            ):
                return
            if method not in {pending.method, "OnRspError"}:
                return
            error = snapshot(info) if info is not None else {}
            if error.get("ErrorID", 0):
                self._reject(pending, error)
                return
            # OnRspOrder* 无错误不代表交易所确认，仍等待 OnRtnOrder。
            if pending.write or method == "OnRspError":
                return
            # VeighNa 用空字典表示 CTP 空指针，查询末包不能产生空记录。
            if data:
                pending.rows.append(snapshot(data))
            if last:
                pending.event.set()

    @staticmethod
    def _matches(identity: dict[str, Any], order: dict[str, Any]) -> bool:
        if identity["instrument_id"] != order.get("InstrumentID"):
            return False
        if identity.get("order_sys_id") is not None:
            return (identity["exchange_id"], identity["order_sys_id"]) == (
                order.get("ExchangeID"),
                order.get("OrderSysID"),
            )
        return all(
            identity[key] == order.get(native)
            for key, native in (
                ("front_id", "FrontID"),
                ("session_id", "SessionID"),
                ("order_ref", "OrderRef"),
            )
        )

    def _same_account(self, record: dict[str, Any]) -> bool:
        return (
            record.get("BrokerID"),
            record.get("InvestorID"),
        ) == self._account_identity

    def on_order(self, data: Any) -> None:
        with self.lock:
            pending = self.pending
            if (
                pending is None
                or not pending.write
                or pending.event.is_set()
                or data is None
            ):
                return
            order = snapshot(data)
            if (
                pending.identity is None
                or not self._same_account(order)
                or not self._matches(pending.identity, order)
            ):
                return
            status, submit = order.get("OrderStatus"), order.get("OrderSubmitStatus")
            insert = pending.method == "ReqOrderInsert"
            if insert and submit == "4":
                self._reject(pending, {"ErrorMsg": order.get("StatusMsg")})
            elif not insert and submit == "5":
                # 订单身份不能区分同一订单上的多次撤单；仅接受本次操作的拒绝。
                if pending.order_memo and order.get("OrderMemo") == pending.order_memo:
                    self._reject(pending, {"ErrorMsg": order.get("StatusMsg")})
            elif not insert and status == "0":
                self._reject(pending, {"ErrorMsg": "订单已全部成交，无法撤销。"})
            elif (not insert and status == "5") or (
                insert and (submit == "3" or status in {"0", "1", "2", "3", "4", "5"})
            ):
                pending.rows.append(order)
                pending.event.set()

    def on_error_return(self, method: str, data: Any, info: Any) -> None:
        with self.lock:
            pending = self.pending
            if (
                pending is None
                or pending.method != method
                or pending.event.is_set()
                or data is None
                or info is None
            ):
                return
            record = snapshot(data)
            # OnErrRtn* 来自账户私有流，其他会话可使用相同 RequestID/OrderRef。
            # 每次操作的随机回显码必需；缺失或归属不明时继续等可确认的回报。
            if (
                pending.identity is None
                or not pending.order_memo
                or record.get("OrderMemo") != pending.order_memo
                or not self._same_account(record)
                or record.get("RequestID") not in {None, 0, pending.request_id}
            ):
                return
            if method == "ReqOrderInsert":
                matched = all(
                    record.get(native) == pending.identity[key]
                    for key, native in (
                        ("order_ref", "OrderRef"),
                        ("instrument_id", "InstrumentID"),
                        ("exchange_id", "ExchangeID"),
                    )
                )
            else:
                matched = record.get("OrderActionRef") in {
                    None,
                    0,
                    pending.request_id,
                } and self._matches(pending.identity, record)
            if matched:
                error = snapshot(info)
                if error.get("ErrorID", 0):
                    self._reject(pending, error)

    def close(self) -> None:
        self.status_snapshot.disconnect(closed=True)
        with self.lock:
            self.closed = True
            self.on_disconnected()
