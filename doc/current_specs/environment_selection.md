# 公开环境选择

配置、HTTP 和 CLI 使用显式布尔值 is_live：true 为实盘 live，false 为模拟盘 sandbox。没有默认环境；未启用或不可用时直接报错，不回退另一环境。

## HTTP

所有 CCXT 路由、CTP 路由、CFB 业务与诊断路由，以及 GET /cache/summary，必须传 is_live。
CCXT 的三条 OHLCV 也没有默认环境。TQ、鉴权、公共取时及主应用健康接口不需要环境参数。

```http
GET /ccxt/fetch_ohlcv/latest-limit?exchange_name=binance&market=future&is_live=true&symbol=BTC/USDT:USDT&timeframe=1m&limit=10
GET /cfb/fetch_balance?is_live=false
GET /cfb/readyz?is_live=true
GET /ctp/fetch_orders?is_live=false&exchange_id=SHFE&instrument_id=rb2610
GET /cache/summary?is_live=true&provider=tq
```

GET 查询参数只接受小写 true/false；JSON 只接受真正的布尔值。以下为 POST /ccxt/create_limit_order 的请求体格式：

```json
{"exchange_name":"binance","market":"future","is_live":false,"symbol":"BTC/USDT:USDT","side":"buy","amount":0.001,"price":50000}
```

省略、null、0/1、yes/no、live/sandbox、JSON 字符串布尔值均返回 422。旧 mode/trading_env 即使与 is_live 一起传入也拒绝，不作为 CCXT 扩展参数发送到上游。CFB 参数错误保持 INVALID_ARGUMENTS 格式；其他模块保留现有校验错误格式。

所选 SDK 身份必须已在 service_whitelist 启用，否则返回 503 SERVICE_NOT_ENABLED。已启用但未就绪时沿用对应不可用错误。缓存概况只查询已有数据，不要求相应 SDK 启用。

## CLI

```bash
just cfb --is-live=false --status
just cfb --is-live=true --target=remote --logs
just debug-trade balance --is-live=false
just debug-balance --is-live=false
just debug-open-long 0.005 --is-live=false
just ctp-assessment --is-live=false --config=config.toml
```

参数必须带值，不能只写 --is-live；省略、旧 --mode 或非法值在访问服务之前以退出码 2 拒绝。Just 转发显式选择；截图文件名、容器名和持久化目录仍用 live/sandbox 区分。部署编排根据白名单为执行器生成 --is-live 参数，用户不需重复指定。

## 内部身份与既有数据

请求模型验证 is_live 后派生原有 mode，SDK、缓存序列、目录、容器标签和 CFB 账户命名空间继续使用 live/sandbox。
响应正文中的 mode、request_mode 及缓存 identity.mode 保留原格式；它们是结果身份，不是旧请求参数的兼容入口。
缓存清理请求 modes.live/sandbox 是两套保留数量，保持原契约。

CFB 指纹将已验证的 is_live 转为原持久化 mode 字段再计算哈希。相同账户、键及业务参数可重放既有 Journal 回执；未知提交保留，不删除记录或自动重发。公开请求和执行器 Operation 均不接受旧字段。

配置加载、HTTP 模型、CLI 和执行器分别校验自身边界；OpenAPI 声明 is_live 为必填 boolean。离线测试验证参数、路由分发、旧输入退出和历史指纹；CFB 来源契约样本保留原貌，仅对批准的环境输入变化作显式等价变换。
