# 统一公开环境选择

## 边界与唯一入口

白名单、HTTP 和 CLI 的环境选择统一为显式布尔值 is_live：true=live，false=sandbox。
CCXT（包括三条 OHLCV）、CTP、CFB 八条业务和四条诊断路由，以及缓存概况查询，都必须明确选择环境；取消原来的默认环境。
TQ、鉴权、公共取时、主应用健康接口没有环境选择，不增加无意义参数。
缓存清理 modes 中的 live/sandbox 是两套保留数量，继续沿用，不属于单环境选择器。

环境改名不改变业务算法、响应正文和错误格式；响应中的 mode、request_mode、缓存 identity 仍描述原有内部身份。
CCXT 扩展参数继续支持，但 mode/trading_env 不得混入扩展参数透传上游。
旧环境参数直接拒绝，无输入别名、默认值或跨环境回退。

## 输入与分发

- JSON 中 is_live 必须是真正的布尔值；字符串、数字和 null 拒绝。
- GET 查询串和 CLI 只接受小写 true/false；省略、空值、0/1、yes/no、live/sandbox 均拒绝。
- 请求模型统一规范化为布尔值，然后派生现有 live/sandbox 内部身份；不另造服务注册表。
- 所选身份未启用时返回 503 SERVICE_NOT_ENABLED；已启用但未就绪继续返回现有不可用错误。
- 参数错误在访问 SDK、执行器或缓存前拒绝，HTTP 422；CFB 保留自己的 INVALID_ARGUMENTS 格式。CLI 参数错误退出码 2，在访问容器、配置或交易连接前拒绝。
- 缓存概况 is_live 必填，其余过滤条件仍可选；过滤转换后交现有缓存 API。TQ 缓存只在 true 下匹配，不修改数据库字段。

## CLI 与已有数据

CFB 运维、执行器守护进程、交易调试、CTP 联调统一用 --is-live=true|false；不保留 --mode。
Just 保持现有命令名，转发用户显式参数。旧调试快捷入口也必须接受并转发选择，不再偷偷补 sandbox。
容器编排从白名单身份产生执行器布尔参数；容器名、标签、数据目录、IPC 身份摘要不因参数名称变化而改变。

CFB Journal 在计算指纹时，将已验证的 is_live 恢复为原始持久化格式的 mode 字段。
这是固定的存储规范化，不接受旧 HTTP/IPC 请求，不重写历史记录、不清空幂等数据。
相同账户、键及业务参数仍重放已有结果；相同键但不同业务参数仍冲突；未知提交不得重发。
IPC Operation 采用新输入模型，执行器再次验证 is_live 与启动环境一致。

## 冻结示例

```http
GET /ccxt/fetch_balance?exchange_name=binance&market=future&is_live=true
GET /cfb/fetch_balance?is_live=false
GET /cfb/readyz?is_live=true
GET /ctp/fetch_orders?is_live=false&exchange_id=SHFE&instrument_id=rb2610
GET /cache/summary?is_live=true&provider=tq
```

```json
{"exchange_name":"binance","market":"future","is_live":false,"symbol":"BTC/USDT:USDT","side":"buy","amount":0.001}
```

上述 JSON 为 POST /ccxt/create_market_order 的参数格式；示例不授权真实下单。
传 {"mode":"sandbox"}、{"is_live":"false"} 或同时传 is_live 和 mode 均为 422。

```bash
just cfb --is-live=false --status
just cfb --is-live=true --target=remote --logs
just debug-trade balance --is-live=false
just debug-open-long 0.005 --is-live=false
just ctp-assessment --is-live=false --config=config.toml
```

## 验证与过渡

修改现有契约测试和 Bruno 请求，不改来源 OpenAPI 样本；等价测试仅对批准的环境字段变化做显式变换，其余输入、结果和能力保持等价。
离线验证全部路由 OpenAPI 的必填布尔声明、GET/POST 双环境分发、缺失及非法值、旧字段及混传拒绝、未启用身份不回退、CLI 在副作用前拒绝、Just 透传和执行器参数一致。
用迁移前格式的真实哈希和离线 SQLite 记录验证历史幂等重放，不以新实现自己产生的指纹作为唯一预期。
通过 just test、just lint、just check；本地 just deploy --target=local --build 验证配套镜像，just test-cfb-native 验证断网空账户执行器 CLI 和双实例隔离。
此为一次替换，主程序与 CFB 执行镜像一起构建；不发布兼容旧参数的平行链路。不触发真实交易、远端上传或在线测试。
