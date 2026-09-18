# 启动白名单的 Bruno 测试

本目录全部是 GET 检查，不执行交易。`ready.bru` 可检查任意已启动的服务；其余 `*_disabled.bru` 要求使用空白名单配置。

1. 在项目根目录用独立配置启动：

   ```bash
   CCXT_PROXY_CONFIG_PATH='bruno/SERVICE LIFECYCLE/disabled.example.toml' uv run --no-sync uvicorn src.main:app --host 127.0.0.1 --port 5123
   ```

2. 该文件只有明确标记的本地测试占位凭证。Bruno 环境中使用其中的 bruno 用户及测试密码，继续继承集合自动获取 Bearer token 的流程。
3. 依次执行本目录请求：ready 应返回空 initialized 数组，三个禁用服务应返回 503 SERVICE_NOT_ENABLED，不建立外部连接。
4. 修改该配置文件不会改变当前进程。重启后才应用新配置；测试自动加载真实服务时，请另复制 config.example.toml，填写对应账号并添加 service_whitelist 条目。

也可先按第 1 步启动，再在另一个终端运行 `just bru-service-disabled`。此 recipe 读取同一测试配置取得本地登录凭证，不自动启动或停止服务。
