"""延迟加载 VeighNa 交易 API；把原生字典回调转交现有请求与状态处理。"""

from typing import Any

from src.tools.ctp_callbacks import CtpCallbacks
from src.tools.ctp_native import load_td_api


def create_api(callbacks: CtpCallbacks) -> Any:
    try:
        td_api = load_td_api()
    except (ImportError, OSError, RuntimeError) as exc:
        raise callbacks.error(
            503,
            "CTP_SDK_UNAVAILABLE",
            "CTP 需要项目固定的 VeighNa 交易扩展；请执行 uv sync --locked --extra ctp 并重启服务。",
        ) from exc

    class Trader(td_api):
        def onFrontConnected(self) -> None:
            callbacks.on_connected()

        def onFrontDisconnected(self, nReason) -> None:
            callbacks.on_disconnected()

        def onRspAuthenticate(
            self, pRspAuthenticateField, pRspInfo, nRequestID, bIsLast
        ) -> None:
            callbacks.on_response(
                "ReqAuthenticate", pRspAuthenticateField, pRspInfo, nRequestID, bIsLast
            )

        def onRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast) -> None:
            callbacks.on_response(
                "ReqUserLogin", pRspUserLogin, pRspInfo, nRequestID, bIsLast
            )

        def onRspSettlementInfoConfirm(
            self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast
        ) -> None:
            callbacks.on_response(
                "ReqSettlementInfoConfirm",
                pSettlementInfoConfirm,
                pRspInfo,
                nRequestID,
                bIsLast,
            )

        def onRspQryOrder(self, pOrder, pRspInfo, nRequestID, bIsLast) -> None:
            callbacks.on_response("ReqQryOrder", pOrder, pRspInfo, nRequestID, bIsLast)

        def onRspQryInstrument(
            self, pInstrument, pRspInfo, nRequestID, bIsLast
        ) -> None:
            callbacks.on_response(
                "ReqQryInstrument", pInstrument, pRspInfo, nRequestID, bIsLast
            )

        def onRspQryDepthMarketData(
            self, pDepthMarketData, pRspInfo, nRequestID, bIsLast
        ) -> None:
            callbacks.on_response(
                "ReqQryDepthMarketData", pDepthMarketData, pRspInfo, nRequestID, bIsLast
            )

        def onRspQryTrade(self, pTrade, pRspInfo, nRequestID, bIsLast) -> None:
            callbacks.on_response("ReqQryTrade", pTrade, pRspInfo, nRequestID, bIsLast)

        def onRspQryInvestorPosition(
            self, pInvestorPosition, pRspInfo, nRequestID, bIsLast
        ) -> None:
            callbacks.on_response(
                "ReqQryInvestorPosition",
                pInvestorPosition,
                pRspInfo,
                nRequestID,
                bIsLast,
            )

        def onRspQryTradingAccount(
            self, pTradingAccount, pRspInfo, nRequestID, bIsLast
        ) -> None:
            callbacks.on_response(
                "ReqQryTradingAccount", pTradingAccount, pRspInfo, nRequestID, bIsLast
            )

        def onRspOrderInsert(self, pInputOrder, pRspInfo, nRequestID, bIsLast) -> None:
            callbacks.on_response(
                "ReqOrderInsert", pInputOrder, pRspInfo, nRequestID, bIsLast
            )

        def onRspOrderAction(
            self, pInputOrderAction, pRspInfo, nRequestID, bIsLast
        ) -> None:
            callbacks.on_response(
                "ReqOrderAction", pInputOrderAction, pRspInfo, nRequestID, bIsLast
            )

        def onRspError(self, pRspInfo, nRequestID, bIsLast) -> None:
            callbacks.on_response("OnRspError", None, pRspInfo, nRequestID, bIsLast)

        def onRtnOrder(self, pOrder) -> None:
            callbacks.on_order(pOrder)

        def onRtnInstrumentStatus(self, pInstrumentStatus) -> None:
            callbacks.on_instrument_status(pInstrumentStatus)

        def onErrRtnOrderInsert(self, pInputOrder, pRspInfo) -> None:
            callbacks.on_error_return("ReqOrderInsert", pInputOrder, pRspInfo)

        def onErrRtnOrderAction(self, pOrderAction, pRspInfo) -> None:
            callbacks.on_error_return("ReqOrderAction", pOrderAction, pRspInfo)

    return Trader()
