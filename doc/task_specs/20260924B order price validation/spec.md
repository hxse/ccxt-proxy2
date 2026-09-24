# 委托价格精度与边界

## 任务边界

实现 Binance、Kraken 的 CCXT 下单价格处理，以及 CTP 限价处理；同步受影响的 current spec 和自动接口说明。维持现有路由、参数名称、sandbox/live 默认值、委托数量、订单有效期及原生市价行为。

普通限价报价支持自动步长对齐。CCXT 止盈止损触发阈值只增加精度及静态边界校验，不自动改变触发时点，不套用当前盘口的动态限价边界。

不修改 CFB、TQ、行情缓存，不修复与价格无关的订单有效期、平仓回执等问题。替换当前限价依赖 SDK 隐式四舍五入的行为；不得提供新旧两套报价链路。

停止线：价格必须经过唯一后端处理后才可提交；无可靠步长时停止提交；明确拒绝越界价格；返回实际价格与调整信息；覆盖真实 SDK 参数构造、CTP 查询和回调链路的离线测试。

## 任务规范

### 通用价格规则

使用十进制定点运算。输入须为有限正数；HTTP price/triggerPrice 在转换为 float 前拒绝布尔值，保留整数、小数和可解析数字字符串的既有写法。限价先还原极小浮点尾差，再按买入向下、卖出向上对齐合法价格网格。除下述尾差还原外，不突破用户最高买价或最低卖价。对齐后非正、超出已知有效上下界均返回 422，不自动截到边界。

仅普通限价的 float 输入适用尾差还原：原报价距离最近正数网格价不超过 `min(2 × ulp(报价), tickSize × 0.000001)` 时，使用该网格价。ulp 为该浮点数的最小表示间隔。超过此容差按原方向性规则处理；内部 Decimal/字符串精确输入及严格触发价不应用容差。容差不得放宽静态或动态价格边界，审计中的 requested_price 仍保留收到的原报价。

报价处理结果记录原价格、提交价格、步长及是否调整，以十进制字符串表达，避免审计字段再次引入浮点尾数。只在一次下单响应中返回，不为查询接口伪造历史原报价。

静态规则必须来自同一 provider/market/mode。无法取得有效步长、规则矛盾、参考行情无效时返回明确的价格资料不可用错误，禁止猜测或继续写操作。上游未公开统一边界的市场允许由上游最终判断，不把缺失边界解释为保证不限价。

价格处理必须位于现有客户端边界，覆盖 HTTP 路由及内部调用。Provider 扩展参数不得覆盖已经管理的委托价格；有独立语义的原生价格匹配、相对价格写法明确拒绝，避免静默绕过。保留不影响报价的扩展参数。

### CCXT

复用已加载的 markets 和现有请求锁、只读重试及单次写入规则。不得在请求时重建 exchange，不自动重发拒单或结果未知的写操作。

Binance 的价格步长和静态范围来自 PRICE_FILTER；合约动态边界按 PERCENT_PRICE 倍率与当前 mark price 判断，买单检查上界、卖单检查下界。现货按 PERCENT_PRICE / PERCENT_PRICE_BY_SIDE 的参考窗口及买卖倍率判断；参考窗口不一致时拒绝预检，不用错误参考价继续下单。不得把 pricePrecision 当成 tickSize，也不得写死全部品种的 5%。

Kraken 的步长来自已加载的合约/交易对资料，兼容 CCXT 的 tick-size 与 decimal-places 精度模式。合约限价若跨盘口，按官方当前的 mark price 20% price collar 检查；未跨盘口的挂单不机械套用该保护。现货不套用合约规则。上游依然作最终判断。

允许新增参数无变化的内部行情读取；读取失败发生在提交之前，必须保留明确的只读失败语义。触发价格只检查网格和静态边界，避免把未来触发单当作立即成交限价。

对 Binance/Kraken 已识别的价格拒单，保留安全的 provider 错误码及价格原因；不能返回完整异常、签名 URL、headers、账户凭证或任意未筛选文本。其他错误继续沿用现有分类。

### CTP

在现有 Trader API/Session/Callbacks 链路接入 ReqQryInstrument 和 ReqQryDepthMarketData。限价下单先取得精确匹配 exchange/instrument 的 PriceTick，再读取同一测试/实盘前置的当日 UpperLimitPrice/LowerLimitPrice。查询必须完整结束；缺失、矛盾、跨交易日数据或查询失败时不调用 ReqOrderInsert。

合约静态资料可在同一会话和交易日内复用；连接重建或重新登录后失效。涨跌停资料每次限价提交前读取，不读取 TQ 实盘资料代替模拟盘。查询继续使用已有流控和超时。

市价单不额外查询价格资料。CTP 上游拒单继续保留其错误消息、错误码及订单定位信息。

## 公开接口与用户写法

现有限价路由请求不变，例如：

```json
{
  "mode": "sandbox", "exchange_id": "DCE", "instrument_id": "m2701",
  "side": "buy", "offset": "open", "volume": 1,
  "price": 3574.2, "time_in_force": "IOC"
}
```

若官方步长为 1、有效范围覆盖 3574，提交价为 3574。CTP 响应新增顶层 `price_adjustment`；CCXT unified order 新增 `order.price_adjustment`：

```json
{
  "requested_price": "3574.2",
  "submitted_price": "3574",
  "tick_size": "1",
  "adjusted": true
}
```

CCXT `order.status` 保持必填，但允许 null：Kraken 现货受理回执没有状态时保留未知，避免已受理委托及其价格调整信息被响应校验丢弃。不伪造 open/closed，也不追加成交等待。其他已有字段与订单状态语义保持不变。未进行报价处理的市价/撤单响应中调整字段为 null 或不存在。

价格错误使用明确的 `INVALID_ORDER_PRICE`、`INVALID_PRICE_PRECISION`、`PRICE_OUT_OF_RANGE`；规则不可用使用 503 `PRICE_RULES_UNAVAILABLE`。详情可增加 `price_context`，记录报价、实际价格、步长、已知上下界和字段名。CTP 保留 mode；这些提交前错误没有新订单标识。

例如步长 1 的止损阈值 3574.2 返回精度错误，不自动变成 3574；上限 3500 时限价 3574.2 对齐到 3574 后仍返回范围错误，不改成 3500。

步长 0.1 的普通卖出限价 `100.10000000000001` 还原为 `100.1`；有意义的小数报价 `100.123` 仍向上对齐到 `100.2`。相同尾差若出现在 triggerPrice 中，仍拒绝；`price: true`、`triggerPrice: false` 均在 HTTP 参数阶段返回 422，不能转成 1 或 0。

## 测试、验证与阶段过渡

默认离线测试覆盖：方向性对齐、十进制边界、无效价格和规则、触发阈值不被移动；浮点尾差的双向还原、ULP/步长容差边界、精确十进制输入与严格触发价不被容差改变、上下界不被放宽、HTTP 布尔值在取客户端前拒绝；Binance 合约/现货动态规则、参考窗口和单侧边界；Kraken 跨盘口与远端挂单；真实 CCXT SDK 最终请求价格不被再次改变；扩展参数不能绕过价格处理；上游错误脱敏。

CTP 测试覆盖真实 session/callback 请求序列、查询完整性、精确合约身份、交易日、断线和缓存失效、限价回执、新增查询失败不下单以及原生市价不受影响。新增回调必须覆盖 native adapter。HTTP/OpenAPI 测试覆盖价格调整和错误字段。

使用正式入口：`CCXT_PROXY_CONFIG_PATH=Test/fixtures/config.toml just test-file`，运行新增价格测试、既有 CCXT 客户端/交易/边界/HTTP/router、domain error、CTP 客户端/native/HTTP/callback/correlation/status 和 repository contract 测试。必须全部通过，警告和异常跳过需说明。

源码变更后运行 `just lint`。不需要持久化新在线测试；本会话已授权模拟盘诊断，可用独立 debug 探针核对只读合约资料及有限 sandbox 委托，明确设置超时、不重试写操作、清理测试新增挂单和仓位。不得使用 live 下单。

本任务整体替换，无临时兼容入口，不调整 CFB 或其他任务历史。
