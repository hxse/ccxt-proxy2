# 来源保留与验证迁移

源项目 cn-futures-bridge 的当前工作区作为复制来源，原仓不切换、不写文件、不改配置、不删除容器和卷。原 Python、native、container、tests、doc 及配置摘要在迁移前后核对；原文任务材料位于 doc/archive/cfb_source，明确不覆盖本仓当前规范。

## 文档去向

| 来源主题 | 本仓现行规范 |
| --- | --- |
| project_principles | cfb_project_principles：CSV 优先级、真实证据与禁止失败后换路径重发 |
| api | cfb_api：八条业务输入、响应、错误、幂等及查询解释 |
| terminal_execution | cfb_terminal_execution：唯一执行权、会话、GUI、重连与防重发 |
| market_orders | cfb_market_orders：真实涨跌停限价＋IOC、表单核对与完整引用关联 |
| trading_status | cfb_trading_status：固定版本原生状态、会话与未知值规则 |
| bootstrap | cfb_bootstrap：独立镜像、数据、配置、协议、VNC 及文件治理 |
| terminal_capabilities | cfb_terminal_capabilities：版本限定的能力与历史验证边界 |

历史性能样本及未验证事项必须保留版本、口径和限制，不把迁移后的代码通过视为重新完成历史在线验证。原任务编号保留在来源归档，不覆盖本仓同号任务。新的主任务及扩展完整定义迁移设计；现行文档不得依赖源仓绝对路径或聊天。

## 不允许变化的核心行为

1. 无弹窗 CSV 导入/回读/发送/回报链及已选定的 IOC 下单板路径；失败不回退其他交易方式。
2. 原生模块和终端版本哈希、GUI 线程归属、控件身份、唯一会话及 generation 校验。
3. 提交、柜台接受、成交分别判断；完整 OrderRef 关联，不根据参数差集或持仓变化认领订单。
4. 原始价格参与幂等指纹，未知结果不自动过期或重发，已持久化事实恢复后仍保留。
5. 自动重连只处理原来允许的连接故障，旧 owner 未退出不创建新 owner，人工接管必须 pause 成功。
6. CSV、PNG、SQLite、日志的容量治理和活动证据保护，不删除未知提交依据换取磁盘空间。

## 验证组织

纯 Python 原 CFB 测试迁入 Test/cfb；只调整模块路径、配置归属和新 HTTP/IPC 夹具，不弱化断言。原生 C 及截图能力在 Test/cfb_native、独立 CFB 验证镜像内运行，保持无网络和无真实账户。

路由等价使用相同输入/终端样本比较状态码、正文、响应头、实际副作用次数和 journal；新增 IPC 断连/超时/畸形帧测试。源旧 HTTP 专用、部署专用测试由本仓替换测试承接，不要求保留失效入口。

所有真实在线请求保持只读；模拟 GUI 发送用离线样本，不能把实际交易执行两遍作为等价验证。镜像和原生代理验证使用本地受控端点及隔离数据。
