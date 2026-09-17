"""无网络的 CTP 前置替身，驱动真实 client/session/callback 路径。"""

from types import SimpleNamespace

from src.ctp_records_account import CtpPosition, CtpTradingAccount
from src.ctp_records_trading import CtpOrder, CtpTrade
from src.tools.config_types import CtpConfig


class Record:
    def __init__(self, **values):
        self.values = values

    def to_dict(self):
        return self.values.copy()


def record(model, **values):
    fields = {
        name: "" if field.annotation is str else 0 if field.annotation is int else 0.0
        for name, field in model.model_fields.items()
    }
    return Record(**(fields | values))


def ctp_config(path):
    return CtpConfig.model_validate(
        {
            "test": {
                "trader_front": "tcp://127.0.0.1:10001",
                "broker_id": "9999",
                "investor_id": "sim-user",
                "password": "sim-password",
                "app_id": "test-app",
                "auth_code": "test-auth",
            },
            "live": {
                "trader_front": "tcp://127.0.0.1:10002",
                "broker_id": "1234",
                "investor_id": "live-user",
                "password": "live-password",
            },
            "flow_path": str(path),
            "request_timeout_seconds": 0.5,
            "connect_timeout_seconds": 0.5,
        }
    ).model_copy(update={"query_interval_seconds": 0})


class FakeApi:
    def __init__(self, callbacks):
        self.callbacks = callbacks
        self.requests = []
        self.hooks = {}
        self.released = 0
        self.initialized = False
        self.queries = {}
        self.front = None
        self.flow = None
        self.session_id = 9

    def Create(self, path, production_mode):
        self.flow, self.production_mode = path, production_mode

    def RegisterFront(self, front):
        self.front = front

    def SubscribePrivateTopic(self, resume, sequence):
        assert (resume, sequence) == (2, 0)

    def SubscribePublicTopic(self, resume):
        assert resume == 2

    def Init(self):
        self.initialized = True
        self.callbacks.on_connected()

    def Release(self):
        assert self.initialized
        self.released += 1

    def __getattr__(self, method):
        if not method.startswith("Req"):
            raise AttributeError(method)

        def send(data, request_id):
            self.requests.append((method, data.to_dict(), request_id))
            if method in self.hooks:
                return self.hooks[method](data, request_id)
            if method == "ReqUserLogin":
                reply = Record(
                    FrontID=7,
                    SessionID=self.session_id,
                    TradingDay="20260917",
                    MaxOrderRef="10",
                )
                self.callbacks.on_response(method, reply, None, request_id, True)
            elif method in {"ReqOrderInsert", "ReqOrderAction"}:
                fields = {
                    k: v
                    for k, v in data.to_dict().items()
                    if k in CtpOrder.model_fields
                }
                order = record(
                    CtpOrder,
                    **(
                        fields
                        | {
                            "FrontID": fields.get("FrontID", 7),
                            "SessionID": fields.get("SessionID", self.session_id),
                            "OrderRef": fields.get("OrderRef", "11"),
                            "OrderSysID": fields.get("OrderSysID", "       42"),
                            "OrderStatus": "3" if method == "ReqOrderInsert" else "5",
                            "OrderSubmitStatus": "3",
                            "TradingDay": "20260917",
                            "StatusMsg": "正常",
                        }
                    ),
                )
                self.callbacks.on_order(order)
            else:
                for row in self.queries.get(method, []):
                    self.callbacks.on_response(method, row, None, request_id, False)
                self.callbacks.on_response(method, None, None, request_id, True)
            return 0

        return send


class FakeFactory:
    def __init__(self):
        self.apis = []
        self.setup = lambda api: None

    def __call__(self, callbacks):
        api = FakeApi(callbacks)
        self.apis.append(api)
        self.setup(api)
        names = (
            "ReqAuthenticateField",
            "ReqUserLoginField",
            "SettlementInfoConfirmField",
            "InputOrderField",
            "InputOrderActionField",
            "QryOrderField",
            "QryTradeField",
            "QryInvestorPositionField",
            "QryTradingAccountField",
        )
        return api, SimpleNamespace(**dict.fromkeys(names, Record))


QUERY_CASES = [
    ("fetch_orders", "ReqQryOrder", "orders", CtpOrder),
    ("fetch_trades", "ReqQryTrade", "trades", CtpTrade),
    ("fetch_positions", "ReqQryInvestorPosition", "positions", CtpPosition),
    ("fetch_balance", "ReqQryTradingAccount", "accounts", CtpTradingAccount),
]
