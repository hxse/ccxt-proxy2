# CTP 交易薄转发

## 安装与配置

CTP 是可选原生依赖，使用上游提供的 Linux/Windows x86-64 SDK。源码构建需要 C++ 编译器，Linux 运行需要 libstdc++。项目 Docker 的构建阶段安装编译工具，运行镜像只保留依赖和应用代码。项目仅构建、安装和加载交易扩展，不安装 vnpy 框架、Qt 或行情扩展。

依赖固定到项目内的补丁源码包，修复退出时持有 GIL 等待回调线程导致的死锁、积压回调的内存泄漏，以及精简系统上的中文解码。本地和 Docker 使用相同版本；安装未包含这些修复的版本时 CTP 接口返回 `CTP_SDK_UNAVAILABLE` 并提示同步依赖。补丁、来源校验及重建说明见 [vendor/vnpy_ctp](../../vendor/vnpy_ctp/README.md)。

```bash
uv sync --locked --extra ctp
uv run --no-sync uvicorn src.main:app --host 127.0.0.1 --port 5123
```

使用项目根目录 `config.toml`，也支持进程环境变量 `CCXT_PROXY_CONFIG_PATH` 指定其他 TOML 文件。把下列分组加入现有配置，或参考根目录 `config.example.toml` 的 CTP 分组。所有 YOUR_*、示例主机及端口必须替换为实际信息。

```toml
[ctp]
flow_path = "./data/ctp"
connect_timeout_seconds = 10
request_timeout_seconds = 10
query_interval_seconds = 1.1

[ctp.test]
trader_front = "tcp://simnow-trade-front.example:12345"
broker_id = "9999"
investor_id = "YOUR_USER"
password = "YOUR_PASSWORD"
app_id = "YOUR_APP_ID"
auth_code = "YOUR_AUTH_CODE"
production_mode = true
```

实盘使用 `[ctp.live]`，字段与 `[ctp.test]` 相同。配置继续经过既有 `AppConfig` 校验；环境变量不覆盖账号字段，修改文件后需重启。可选账户整组省略或留在注释中表示未配置。

- `mode=sandbox`（默认）使用 `ctp.test`，可以接入 SimNow 或期货公司仿真前置。
- `mode=live` 使用 `ctp.live`，结构与 test 相同，填写期货公司实盘前置、账户及认证信息。
- 只有 `service_whitelist` 中列出的 ctp/模式会启用；未启用时 HTTP 503 `SERVICE_NOT_ENABLED`。白名单引用缺失的账号配置会导致启动失败，不切换模式。
- `user_id` 可选，默认使用 `investor_id`。`app_id/auth_code` 必须同时配置或同时省略；只有前置不要求客户端认证时才能省略。
- `production_mode` 是底层 SDK 生产/评测密钥模式，不是模拟/实盘开关；按前置要求设置。SimNow 前置也可能使用生产密钥。
- 密码及 AuthCode 使用 SecretStr；错误文本会脱敏。HTTP 请求不接受账户、密码、AuthCode 或交易前置地址。
- sandbox/live 分别使用 `flow_path/sandbox` 和 `flow_path/live`，各自复用一个长期连接。沿用项目单 Uvicorn process 部署方式。
- 白名单内的模式在启动阶段完成认证、登录和结算确认，然后再接受 HTTP 请求。断线恢复使用启动时的配置快照；运行中修改配置无效。
- `/readyz` 包含统一白名单的全部初始化结果，例如 ctp/sandbox。任一启用实例初始化失败，应用清理资源并退出；没有启用的模式不连接。OpenAPI 保留所有接口。

SimNow 的账户、服务说明和当前前置信息以 [SimNow 官网](https://www.simnow.com.cn/)及[产品与服务](https://www.simnow.com.cn/product.action)为准。常规仿真与专用 API 测试环境的交易时段、行情和结算服务可能不同，配合 TQ 实时行情测试时应选择对应环境。模拟账户使用虚拟资金。

## 路由和返回模型

所有路由复用 Bearer 鉴权。写操作用 JSON body；读取用 query；拒绝未声明的字段。

| 方法 | 路由 | CTP API | 成功结果字段 |
| --- | --- | --- | --- |
| POST | `/ctp/create_market_order` | ReqOrderInsert | `order: CtpOrder` |
| POST | `/ctp/create_limit_order` | ReqOrderInsert | `order: CtpOrder` |
| POST | `/ctp/cancel_order` | ReqOrderAction | `order: CtpOrder` |
| GET | `/ctp/fetch_orders` | ReqQryOrder | `orders: list[CtpOrder]` |
| GET | `/ctp/fetch_trades` | ReqQryTrade | `trades: list[CtpTrade]` |
| GET | `/ctp/fetch_positions` | ReqQryInvestorPosition | `positions: list[CtpPosition]` |
| GET | `/ctp/fetch_balance` | ReqQryTradingAccount | `accounts: list[CtpTradingAccount]` |
| GET | `/ctp/fetch_trading_status` | OnRtnInstrumentStatus | `is_open/raw_status/reason/data` |

下单、撤单和 ReqQry* 查询的成功响应还包含 `mode`、`request_id`（CTP nRequestID）、`trading_day`（登录回报的 YYYYMMDD 交易日）。`request_id` 与 HTTP `X-Request-ID` 不同，只在对应 CTP 连接内关联请求。交易状态来自主动通知，不包含 request_id/trading_day。

返回模型逐字段定义原生结构体的有效业务字段，包括订单系统编号、会话标识、提交/成交状态、冻结持仓、保证金、盈亏等。只省略 SDK 标记为无效的 reserve* 字段。

- 字符串保留 CTP 字段名和原始编码；`OrderStatus` 等状态的编码含义显示在 `/docs`。
- `OrderSysID` 保留前导空格，不转整数。日期、时间原样返回，不作时区或夜盘交易日推算。
- 数量为整数手数；资金金额按账户行的 `CurrencyID` 计价。
- 无效 double（DBL_MAX、NaN、Infinity）转为 JSON null。
- 查询无记录时返回空数组；不同持仓方向、今昨仓和套保标志的记录不合并。

## 当前交易状态

```text
GET /ctp/fetch_trading_status?mode=sandbox&exchange_id=SHFE&product_id=rb
```

默认 `mode=sandbox`，状态仅代表对应的 SimNow/仿真前置；判断实盘环境应显式传 `mode=live` 并配置 `[ctp.live]`。

状态按品种查询：请求用 `product_id=rb`，直接匹配通知的 InstrumentID。区分大小写，不把 rb2610 自动转换为 rb，也不回退其他交易所或模式。未推送的品种返回未知。原生回调字段见当前依赖源码中的 `CThostFtdcInstrumentStatusField`。

```json
{
  "mode": "sandbox",
  "exchange_id": "SHFE",
  "product_id": "rb",
  "is_open": true,
  "raw_status": "2",
  "reason": null,
  "data": {
    "ExchangeID": "SHFE",
    "InstrumentID": "rb",
    "ExchangeInstID": "rb",
    "SettlementGroupID": "",
    "InstrumentStatus": "2",
    "TradingSegmentSN": 1,
    "EnterTime": "09:00:00",
    "EnterReason": "1"
  }
}
```

仅 `"2"` 连续交易返回 `is_open=true`。`"0"` 开盘前、`"1"` 非交易、`"3"` 集合竞价报单、`"4"` 集合竞价价格平衡、`"5"` 集合竞价撮合、`"6"` 收盘、`"7"` 交易处理中返回 false。未知编码保留 raw_status/data，但 is_open=null、reason=unrecognized_status。

启动时按配置超时完成认证、登录、结算确认。HTTP 状态请求只读取独立短锁保护的最新通知，不获取账户操作锁、业务回调关联锁或等待 SDK，不会创建连接或触发重登。尚未收到时 reason=not_received，已断线为 disconnected，无连接实例或关闭后为 unavailable，均返回 HTTP 200、is_open=null、raw_status=null、data=null。未启用/未完成应用启动仍返回 503，异常通知结构返回 502。其余订单/持仓/资金查询和交易操作继续串行处理。

每个连接只保留各交易所/品种的最新通知，断线和关闭后清空，重连后重新等待通知。EnterTime 没有日期，不用来计算有效期；没有状态变化可以长时间没有新通知。不根据日历或超时推算休市，也不把交易状态当成下单一定成功的保证。

## 开仓与平仓

限价单示例：

```json
{
  "mode": "sandbox",
  "exchange_id": "SHFE",
  "instrument_id": "rb2610",
  "side": "buy",
  "offset": "open",
  "volume": 1,
  "price": 3500,
  "time_in_force": "GFD"
}
```

合约与价格仅为格式示例，实际请求应填写可交易合约及自己选定的价格。

| 操作 | side | offset |
| --- | --- | --- |
| 开多 | buy | open |
| 开空 | sell | open |
| 平多 | sell | close / close_today / close_yesterday |
| 平空 | buy | close / close_today / close_yesterday |

`volume` 是正整数手数。上游校验交易权限、资金、可平仓位、价格步长和涨跌停范围。调用方按交易所规则选择平仓、平今、平昨，服务不自动查仓拆单或平全部。

- 市价接口不接受 price，固定 AnyPrice + IOC + AV，LimitPrice=0。
- 限价接口 price 必填；GFD（默认）=GFD+AV，IOC=IOC+AV，FOK=IOC+CV。
- 原生市价和指令组合是否支持由上游决定。不支持时返回明确错误，不查 TQ 价格合成市价单、不自动降级。
- 主连先通过既有 TQ 接口解析；TQ 的 SHFE.rb2610 由调用方拆成 exchange_id=SHFE、instrument_id=rb2610。CTP 路由不接受主连或指数代码。

下单成功返回收到的订单快照，不保证成交或全部成交。服务等待匹配的 OnRtnOrder 接受/订单状态回报；OnRspOrderInsert、OnErrRtnOrderInsert 和订单拒绝回报均处理为错误。成交明细使用 fetch_trades 查询。

## 撤单与查询

撤单只支持 Delete，`by` 选择一种定位方式：

```json
{
  "mode": "sandbox",
  "by": "exchange_order",
  "exchange_id": "SHFE",
  "instrument_id": "rb2610",
  "order_sys_id": "       12345"
}
```

另一种为 `by=session_order`，传原订单的 `front_id`、`session_id`、`order_ref`，同时传 exchange_id/instrument_id。两种请求在 OpenAPI 中用 oneOf 展示；不允许混用字段，不自动查询补齐信息。

撤单成功等待对应 OnRtnOrder 的 OrderStatus=5；只撤未成交部分，已成交部分保留。使用原订单会话标识，不替换成重新登录后的会话。

查询示例：

```text
GET /ctp/fetch_orders?mode=sandbox&exchange_id=SHFE&instrument_id=rb2610
GET /ctp/fetch_trades?mode=sandbox&exchange_id=SHFE&instrument_id=rb2610
GET /ctp/fetch_positions?mode=sandbox
GET /ctp/fetch_balance?mode=sandbox&currency_id=CNY
```

订单/成交还支持原生时间和编号过滤，详细参数见 `/docs`。它们查询 CTP 当前可查询交易日的数据，不是任意历史日期数据库。

## 回调、错误与边界

一个账户的 HTTP 操作串行处理；查询按 query_interval_seconds 间隔发送。回调在自己的短锁内按请求或订单标识关联，并在 C 内存失效前复制结构体。查询等待 bIsLast 后才返回完整结果。两个模式相互独立。

每次下单/撤单用原生 `OrderMemo` 携带独立的 12 字符随机关联码，仅保存在当前等待请求中。`OnErrRtnOrderInsert/Action` 属于账户私有流，需同时匹配关联码、账户和订单身份；非零 RequestID/OrderActionRef 也必须相符。其他会话、迟到或缺少关联码的错误回报不结束当前 HTTP 请求。若最终没有可确认的回报，写操作按超时返回 `OPERATION_STATUS_UNKNOWN`，不能据此认定拒单。`OnRsp*` 继续按本会话请求号关联；订单状态通知核对原订单身份，撤单拒绝另需确认本次操作归属。

关联码由服务生成，不是 HTTP 输入参数或幂等键。订单结果和查询仍返回原生 OrderMemo；需用实际 SimNow/实盘前置验证回显支持，旧前置不回显时采用上述保守的超时语义。

| 情况 | HTTP | detail.code |
| --- | --- | --- |
| 参数错误 | 422 | FastAPI detail 错误列表 |
| 未配置/SDK 不可用/连接超时 | 503 | CTP_NOT_CONFIGURED / CTP_SDK_UNAVAILABLE / CTP_CONNECT_TIMEOUT |
| Req* 返回 -2/-3 | 429 | CTP_SEND_FAILED，含 return_code |
| 登录/认证/结算确认失败 | 502 | CTP_AUTH_FAILED / CTP_SETTLEMENT_FAILED |
| 上游拒绝下单 | 422 | CTP_ORDER_REJECTED |
| 上游拒绝撤单/订单已全部成交 | 409 | CTP_CANCEL_REJECTED |
| 查询缺少完整回报 | 504 | CTP_TIMEOUT |
| 写操作发送后超时/断线 | 502 | OPERATION_STATUS_UNKNOWN |

业务错误 detail 包含 mode、可得的 request_id、return_code、ctp_error_id、message 和 order_identity。只有 StatusMsg 的拒单回报可能没有 ErrorID。超时不能证明没有下单；调用方应先查订单和成交对账，不直接重试。

服务不自动重试、不维护订单/成交账本、不缓存、不计算可平仓位或均价，也不提供历史回填。晚于 HTTP 完成的状态变化通过显式查询获取。OrderRef 自动递增，但不是 HTTP 幂等键。

## 验证与维护

```bash
just test-ctp-offline
just bru-ctp-readonly
```

默认测试使用假前置，检查参数映射、隔离、回调关联、错误、超时、重连、释放及 OpenAPI。安装 CTP extra 后另在子进程验证原生 init/exit，并使用本机临时 TCP 假前置重现断线回调与释放并发。源码检查覆盖结构体字段、中文解码和回调队列的内存释放。子进程带硬超时，不连接模拟盘/实盘、不发送交易请求。

Bruno 的 `CTP TRADING` 文件夹提供请求样例。下单和撤单标记为 `[STATEFUL]`，需手动选择执行；只读 recipe 不包含交易写操作，服务启动时已经登录及确认结算。

尚需使用实际 SimNow 账户完成登录、下单、撤单与查询验收；离线替身通过不代表真实前置已连通。实盘前置的账号权限及认证信息由期货公司提供。

VeighNa 的 `subscribePrivateTopic` 接收单个重传模式参数，项目使用 QUICK（2）。原生对象经过 init 后再 exit；退出时不得持有回调锁，补丁在 pybind11 调用边界释放 GIL，并清理队列内尚未处理的数据。session 保证重复关闭不会再次调用原生 exit。这些 SDK 细节封装在私有 session/SPI 中，路由不感知。
