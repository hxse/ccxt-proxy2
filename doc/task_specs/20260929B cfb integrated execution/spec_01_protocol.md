# 通信协议与 HTTP 等价

## Unix socket 边界

主服务访问 data/cfb/<mode>/run/bridge.sock；执行容器的数据根固定 /data，监听 /data/run/bridge.sock。容器之间通过项目数据目录映射共享 socket，业务数据库仍由执行器独占。进程权限必须允许受管客户端访问，不开放任意用户或公网端口。

一连接一请求一响应；UTF-8 JSON 加换行分帧，固定 version=1。请求最多 64 KiB，响应最多 8 MiB；读写和等待均有期限，畸形、超限、未知版本/命令在执行前拒绝。禁止 pickle、任意命令执行、动态模块名和通用 RPC 框架。

请求带 request_id、kind、可选 Operation 和幂等键；execute 和 ready 必须带已加载 CFB 配置的身份摘要，执行器使用同一门禁再次比对。不匹配返回 503 SERVICE_NOT_READY，先于业务准入或就绪读取拒绝，避免更新失败后误用旧账户或误报就绪。kind 只允许 execute、ready、status、health、pause、resume、clean、screenshot；execute 的 action 仅原八种。ready 读取原状态，不排队、不触发登录；/cfb/readyz 仅在配置身份匹配且 trading_ready=true 时返回 200。status 和 health 保留查看旧执行器及进程存活的诊断能力，不作为当前配置就绪证明。服务端复用正式模型二次验证及能力校验，不能因主服务验证过就绕过业务前提。

响应携带原 Reply 的 status/body，以及白名单响应头。CFB 请求编号、幂等重放编号、Cache-Control、X-Content-Type-Options、Retry-After 保持原语义。截图使用明确的 base64 字段及 image/png 类型；大小与解码校验失败报错，不返回截断图片。

主 HTTP 入口为每次 CFB 请求生成唯一 cfb- 编号，路由直接复用；客户端传入的 X-Request-ID 不用作 Journal 主键。普通成功、参数错误和执行错误的正文、响应头与 HTTP 完成日志使用同一编号。幂等重放保留已保存的正文和原编号；HTTP 完成日志的 request_id 使用响应原编号，另以 http_request_id 记录本次 HTTP 编号，保留两次请求的关联。其他路由沿用现有请求编号行为。

离线验证须穿过真实 HTTP 请求中间件和 CFB 路由，断言成功、参数错误、通信失败及重放的编号关联；使用真实 Unix socket 验证配置不一致时就绪和业务均拒绝、状态诊断仍可读，且不触发执行队列。

## 执行权和取消

每个模式服务端唯一 Dispatcher 承担准入、FIFO、Journal 查询和写入。收到 EOF/取消后设置同一取消 Event，并尝试取消未开始 Job；已经开始的操作继续由 worker 核对、收尾和记录。关闭连接不能取消持有 GUI 的 Future，更不能释放执行权。

已保存幂等响应优先于终端就绪判断。相同键/相同原始参数重放原响应；修改参数或仍在执行依原规则报 409。socket 失败不自动重试；发送后的未知结果不能转成未执行或自动补发。已取得的订单身份和提交事实始终保留。

状态和截图不排入交易 FIFO，沿原 BridgeService 诊断行为；pause/resume 继续遵守原执行权检查。socket 服务并发等候多个请求，但不增加业务并行。

## HTTP 归属

只有 ccxt-proxy2 的 FastAPI 包含业务 APIRouter；CFB 守护进程不导入/运行独立 FastAPI 应用或 Uvicorn。CFB 请求模型、错误模型和文档直接生成 OpenAPI，旧上游快照与同步命令退出。

CFB 范围的参数错误、BridgeError 和非预期失败由局部 HTTP 适配处理，不能改变其他模块的错误格式。保证失败后的 submission_status/order_id/identity/execution/verification 不被全局 500 覆盖；鉴权仍先于业务执行。

八条业务路由的 is_live、有效期、价格与数量规则沿用迁入的 cfb_api/cfb_market_orders 等规范。诊断路径迁移：原 /v1/status→/cfb/status，原 /readyz→/cfb/readyz，原 /healthz→/cfb/healthz，原 /v1/desktop/screenshot→/cfb/desktop/screenshot；都使用主项目鉴权，原单独端口退出。

路由先按统一模型解析必填 is_live 并派生内部 mode，再检查该服务身份并取得对应客户端；不得从另一个位置重复猜测模式。执行器仍验证请求 is_live 与启动绑定一致。两个模式共用协议定义，各自独立连接、排队、Journal 和恢复；没有账户动态切换或跨模式补发。
