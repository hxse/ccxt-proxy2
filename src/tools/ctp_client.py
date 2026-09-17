"""一个 sandbox/live 账户的薄转发；锁只覆盖该账户的单次 HTTP 操作。"""

import threading
from typing import Any

from pydantic import BaseModel, ValidationError

from src.base_types import ModeType
from src.responses_ctp import (
    CtpAccountsResponse,
    CtpOrderResponse,
    CtpOrdersResponse,
    CtpPositionsResponse,
    CtpTradesResponse,
)
from src.tools.config_types import CtpAccountConfig, CtpConfig
from src.tools.ctp_callbacks import CtpError
from src.tools.ctp_session import ApiFactory, CtpSession
from src.tools.ctp_spi import create_api
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

QUERY_FIELDS = {
    "exchange_id": "ExchangeID",
    "instrument_id": "InstrumentID",
    "invest_unit_id": "InvestUnitID",
    "order_sys_id": "OrderSysID",
    "insert_time_start": "InsertTimeStart",
    "insert_time_end": "InsertTimeEnd",
    "trade_id": "TradeID",
    "trade_time_start": "TradeTimeStart",
    "trade_time_end": "TradeTimeEnd",
    "currency_id": "CurrencyID",
}


class CtpClient:
    def __init__(
        self,
        account: CtpAccountConfig,
        config: CtpConfig,
        mode: ModeType,
        factory: ApiFactory = create_api,
    ):
        self._account, self._config, self.mode, self._factory = (
            account,
            config,
            mode,
            factory,
        )
        self._lock = threading.Lock()
        self._session: CtpSession | None = None
        self._closed = False

    def _get_session(self) -> CtpSession:
        if self._closed:
            raise CtpError(503, "CTP_CLIENT_CLOSED", "CTP 客户端已关闭。", self.mode)
        if self._session is not None and self._session.needs_reset:
            old, self._session = self._session, None
            old.close()
        if self._session is None:
            self._session = CtpSession(
                self._account, self._config, self.mode, self._factory
            )
        self._session.ensure_ready()
        return self._session

    @staticmethod
    def _result[T: BaseModel](
        session: CtpSession,
        model: type[T],
        request_id: int,
        key: str,
        data: Any,
        identity: dict[str, Any] | None = None,
    ) -> T:
        try:
            return model.model_validate(
                {
                    "mode": session.mode,
                    "request_id": request_id,
                    "trading_day": session.login["TradingDay"],
                    key: data,
                }
            )
        except ValidationError as exc:
            raise session.callbacks.error(
                502,
                "OPERATION_STATUS_UNKNOWN" if identity else "CTP_INVALID_RESPONSE",
                "CTP 回报结构异常；写操作请先查询订单及成交对账。",
                request_id=request_id,
                order_identity=identity,
            ) from exc

    def create_order(
        self, request: CtpMarketOrderRequest | CtpLimitOrderRequest
    ) -> CtpOrderResponse:
        with self._lock:
            session = self._get_session()
            identity = {
                "exchange_id": request.exchange_id,
                "instrument_id": request.instrument_id,
                "order_ref": session.next_order_ref(),
                "front_id": session.login["FrontID"],
                "session_id": session.login["SessionID"],
            }
            price_type, price, tif = "1", 0.0, "IOC"
            if isinstance(request, CtpLimitOrderRequest):
                price_type, price, tif = "2", request.price, request.time_in_force
            fields = {
                **session.credentials,
                "UserID": session.user_id,
                "ExchangeID": request.exchange_id,
                "InstrumentID": request.instrument_id,
                "OrderRef": identity["order_ref"],
                "InvestUnitID": request.invest_unit_id,
                "Direction": {"buy": "0", "sell": "1"}[request.side],
                "CombOffsetFlag": {
                    "open": "0",
                    "close": "1",
                    "close_today": "3",
                    "close_yesterday": "4",
                }[request.offset],
                "CombHedgeFlag": {"speculation": "1", "arbitrage": "2", "hedge": "3"}[
                    request.hedge_flag
                ],
                "OrderPriceType": price_type,
                "LimitPrice": price,
                "VolumeTotalOriginal": request.volume,
                "TimeCondition": "3" if tif == "GFD" else "1",
                "VolumeCondition": "3" if tif == "FOK" else "1",
                "MinVolume": request.volume if tif == "FOK" else 1,
                "ContingentCondition": "1",
                "ForceCloseReason": "0",
                "IsAutoSuspend": 0,
                "UserForceClose": 0,
            }
            request_id, rows = session.request(
                "ReqOrderInsert", "InputOrderField", fields, identity
            )
            return self._result(
                session,
                CtpOrderResponse,
                request_id,
                "order",
                rows[0] if rows else None,
                identity,
            )

    def cancel_order(
        self, request: CtpCancelByExchange | CtpCancelBySession
    ) -> CtpOrderResponse:
        with self._lock:
            session = self._get_session()
            identity: dict[str, Any] = {
                "exchange_id": request.exchange_id,
                "instrument_id": request.instrument_id,
            }
            fields: dict[str, Any] = {
                **session.credentials,
                "UserID": session.user_id,
                "ExchangeID": request.exchange_id,
                "InstrumentID": request.instrument_id,
                "InvestUnitID": request.invest_unit_id,
                "ActionFlag": "0",
            }
            if isinstance(request, CtpCancelByExchange):
                fields["OrderSysID"] = request.order_sys_id
                identity["order_sys_id"] = request.order_sys_id
            else:
                fields.update(
                    FrontID=request.front_id,
                    SessionID=request.session_id,
                    OrderRef=request.order_ref,
                )
                identity.update(
                    front_id=request.front_id,
                    session_id=request.session_id,
                    order_ref=request.order_ref,
                )
            request_id, rows = session.request(
                "ReqOrderAction", "InputOrderActionField", fields, identity
            )
            return self._result(
                session,
                CtpOrderResponse,
                request_id,
                "order",
                rows[0] if rows else None,
                identity,
            )

    def _query[T: BaseModel](
        self, request: BaseModel, method: str, structure: str, model: type[T], key: str
    ) -> T:
        fields = {
            QUERY_FIELDS[name]: value if value is not None else ""
            for name, value in request.model_dump(exclude={"mode"}).items()
        }
        if method == "ReqQryTradingAccount":
            fields["BizType"] = "1"  # 期货账户。
        with self._lock:
            session = self._get_session()
            request_id, rows = session.request(
                method, structure, {**session.credentials, **fields}
            )
            return self._result(session, model, request_id, key, rows)

    def fetch_orders(self, request: CtpOrderQuery) -> CtpOrdersResponse:
        return self._query(
            request, "ReqQryOrder", "QryOrderField", CtpOrdersResponse, "orders"
        )

    def fetch_trades(self, request: CtpTradeQuery) -> CtpTradesResponse:
        return self._query(
            request, "ReqQryTrade", "QryTradeField", CtpTradesResponse, "trades"
        )

    def fetch_positions(self, request: CtpPositionQuery) -> CtpPositionsResponse:
        return self._query(
            request,
            "ReqQryInvestorPosition",
            "QryInvestorPositionField",
            CtpPositionsResponse,
            "positions",
        )

    def fetch_balance(self, request: CtpAccountQuery) -> CtpAccountsResponse:
        return self._query(
            request,
            "ReqQryTradingAccount",
            "QryTradingAccountField",
            CtpAccountsResponse,
            "accounts",
        )

    def close(self) -> None:
        with self._lock:
            self._closed = True
            session, self._session = self._session, None
            if session is not None:
                session.close()
