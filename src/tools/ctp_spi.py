"""延迟加载 ctpwrapper；无 CTP 配置时文档及其他接口不加载原生库。"""

from typing import Any

from src.tools.ctp_callbacks import CtpCallbacks

PATCHED_CTPWRAPPER_VERSION = "6.7.13+ccxtproxy.1"


def create_api(callbacks: CtpCallbacks) -> tuple[Any, Any]:
    import ctpwrapper

    if ctpwrapper.__version__ != PATCHED_CTPWRAPPER_VERSION:
        raise callbacks.error(
            503,
            "CTP_SDK_UNAVAILABLE",
            "CTP 需要项目固定的释放补丁版本；请执行 uv sync --locked --extra ctp 并重启服务。",
        )
    from ctpwrapper import ApiStructure, TraderApiPy

    class Trader(TraderApiPy):
        def OnFrontConnected(self) -> None:
            callbacks.on_connected()

        def OnFrontDisconnected(self, nReason) -> None:
            callbacks.on_disconnected()

        def OnRspAuthenticate(
            self, pRspAuthenticateField, pRspInfo, nRequestID, bIsLast
        ) -> None:
            callbacks.on_response(
                "ReqAuthenticate", pRspAuthenticateField, pRspInfo, nRequestID, bIsLast
            )

        def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast) -> None:
            callbacks.on_response(
                "ReqUserLogin", pRspUserLogin, pRspInfo, nRequestID, bIsLast
            )

        def OnRspSettlementInfoConfirm(
            self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast
        ) -> None:
            callbacks.on_response(
                "ReqSettlementInfoConfirm",
                pSettlementInfoConfirm,
                pRspInfo,
                nRequestID,
                bIsLast,
            )

        def OnRspQryOrder(self, pOrder, pRspInfo, nRequestID, bIsLast) -> None:
            callbacks.on_response("ReqQryOrder", pOrder, pRspInfo, nRequestID, bIsLast)

        def OnRspQryTrade(self, pTrade, pRspInfo, nRequestID, bIsLast) -> None:
            callbacks.on_response("ReqQryTrade", pTrade, pRspInfo, nRequestID, bIsLast)

        def OnRspQryInvestorPosition(
            self, pInvestorPosition, pRspInfo, nRequestID, bIsLast
        ) -> None:
            callbacks.on_response(
                "ReqQryInvestorPosition",
                pInvestorPosition,
                pRspInfo,
                nRequestID,
                bIsLast,
            )

        def OnRspQryTradingAccount(
            self, pTradingAccount, pRspInfo, nRequestID, bIsLast
        ) -> None:
            callbacks.on_response(
                "ReqQryTradingAccount", pTradingAccount, pRspInfo, nRequestID, bIsLast
            )

        def OnRspOrderInsert(self, pInputOrder, pRspInfo, nRequestID, bIsLast) -> None:
            callbacks.on_response(
                "ReqOrderInsert", pInputOrder, pRspInfo, nRequestID, bIsLast
            )

        def OnRspOrderAction(
            self, pInputOrderAction, pRspInfo, nRequestID, bIsLast
        ) -> None:
            callbacks.on_response(
                "ReqOrderAction", pInputOrderAction, pRspInfo, nRequestID, bIsLast
            )

        def OnRspError(self, pRspInfo, nRequestID, bIsLast) -> None:
            callbacks.on_response("OnRspError", None, pRspInfo, nRequestID, bIsLast)

        def OnRtnOrder(self, pOrder) -> None:
            callbacks.on_order(pOrder)

        def OnErrRtnOrderInsert(self, pInputOrder, pRspInfo) -> None:
            callbacks.on_error_return("ReqOrderInsert", pInputOrder, pRspInfo)

        def OnErrRtnOrderAction(self, pOrderAction, pRspInfo) -> None:
            callbacks.on_error_return("ReqOrderAction", pOrderAction, pRspInfo)

    return Trader(), ApiStructure
