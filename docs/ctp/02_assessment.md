# 期货公司穿透式采集联调

入口：[script/ctp_assessment.py](../../script/ctp_assessment.py)。独立命令，不需要启动 HTTP 服务；复用项目的 TOML 加载器和 `vnpy_ctp` 交易扩展。

## 当前 VeighNa 的方式

核对日期：2026-09-19。

- 早期教程使用独立的 `CTPTEST/vnpy_ctptest`。当前项目所用 `vnpy_ctp 6.7.11.4` 已包含生产、评测合并的 CTP 6.7.11 库，参见 [VeighNa 官方源码说明](https://github.com/vnpy/vnpy_ctp/blob/main/README.md)。
- VeighNa 社区在 **2026-05-29** 的答复仍是使用 CTP 接口，柜台环境选“测试”，参见 [当前评测方式](https://www.vnpy.com/forum/topic/34699-sha-shi-hou-you-zhi-chi-6-7-13ban-ben-apide-ctptestmo-kuai-ni?page=1)。其[网关源码](https://github.com/vnpy/vnpy_ctp/blob/main/vnpy_ctp/gateway/ctp_gateway.py)将“测试”映射为 `production_mode=false`。
- **SimNow 使用生产密钥模式 `production_mode=true`**，不是上述评测模式。VeighNa 在 **2026-02-27** 对此有[明确答复](https://www.vnpy.com/forum/topic/34535-vn-py-zai-lian-jie-simnowshi-yu-dao-wen-ti)。`--mode sandbox` 只表示读取 `[ctp.test]`，不会自动改变密钥模式。
- 采集由 CTP 原生 SDK 内部完成，上层沿用连接、`ReqAuthenticate`、`ReqUserLogin` 的流程，不需要手工填写硬件标识。参见 [VeighNa FAQ 的采集说明](https://www.vnpy.com/forum/topic/250-ti-wen-qian-qing-xian-kan-%3Avn-pyshi-yong-faq)。本脚本按直连交易前置方式登录；采集字段是否齐全、是否收到和认可，由期货公司后台核对。

2026 年的测试案例还涉及报撤单统计、风控阈值和错误回报展示，参见 [VeighNa 新版测试讨论](https://www.vnpy.com/forum/topic/34508-qiu-zhu-%3Axin-de-chuan-tou-ce-shi-yao-qiu)。这些是完整交易系统的测试项目，不能由“登录成功”代替。华安当前的具体测试清单、支持的 API 版本和环境须由其提供；此处没有把其他期货公司的做法当作华安的要求。

## 配置和运行

向华安取得**评测交易前置、BrokerID、评测账号/密码、登记的 AppID、AuthCode**，确认其要求的 SDK 版本、密钥模式和测试时间。只做采集联调时使用交易前置即可，不需要行情前置；VeighNa 对没有行情服务器的测试有[相应说明](https://www.vnpy.com/forum/topic/34606-lao-men-chuan-tou-shi-ce-shi-shi-bai-zen-yao-ban)。

直接复用项目根目录的 `config.toml`；如果后台通过 `CCXT_PROXY_CONFIG_PATH` 选择配置，脚本也沿用同一个入口。默认读取其中的 `[ctp.test]`，传 `--mode live` 则读取 `[ctp.live]`。

华安评测时，将其提供的信息填入 `config.toml` 中已有的 `[ctp.test]` 分组。下面仅示例该分组，不要重复追加同名分组；`production_mode` 按华安评测环境的要求填写：

```toml
[ctp.test]
trader_front = "tcp://assessment-front.example:12345"
broker_id = "BROKER"
investor_id = "ACCOUNT"
password = "PASSWORD"
app_id = "REGISTERED_APP_ID"
auth_code = "AUTH_CODE"
production_mode = false
```

`user_id` 仍为可选项，省略时使用 `investor_id`。本脚本只对手动选择的账号执行测试，不启动白名单中的其他服务，也不要求把评测账号加入后台白名单。配置仅在脚本启动时读取一次。实际 `config.toml` 已被 Git 和 Docker 构建上下文忽略。

```bash
uv sync --locked --extra ctp

# 查看参数，不读取配置、不连接交易前置。
uv run --no-sync python script/ctp_assessment.py --help

# 直接使用默认配置中的 ctp.test。
just ctp-assessment

# 联调时可延长回报等待和登录后的保持时间。
just ctp-assessment --timeout 60 --hold-seconds 60
```

参数：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `--config` | `CCXT_PROXY_CONFIG_PATH` 或项目根目录 `config.toml` | 可选覆盖路径，通常省略，直接复用后台配置；显式参数的相对路径按当前目录解析 |
| `--mode` | `sandbox` | `sandbox` 读取 `ctp.test`；`live` 读取 `ctp.live`；不覆盖 `production_mode` |
| `--timeout` | TOML 中的连接/请求超时 | 覆盖每个阶段的回报等待时间，1～300 秒；不重试密码登录 |
| `--hold-seconds` | `30` | 登录后保持连接观察断线，0～3600 秒；完成后释放 API |
| `--output-dir` | 项目 `data/ctp_assessment` | 每次创建独立目录保存报告和 SDK flow 文件，不与后台连接共用 flow |

脚本仅连接、认证、登录和保持连接，不确认结算、查询账户或发送报撤单。`Ctrl+C` 可结束测试。回报等待有超时，原生 SDK 的同步调用和释放仍取决于 SDK 本身。

## 看报告

每次生成 `data/ctp_assessment/<运行时间_随机后缀>/report.jsonl`，每行一条 JSON；目录权限 `0700`，报告权限 `0600`。

报告包括：所选配置模式、密钥模式、前置地址、BrokerID、AppID、脱敏账号、Python/操作系统/架构、`vnpy_ctp` 和 CTP API 版本，以及各阶段返回码、`ErrorID/ErrorMsg`、断线原因、`TradingDay/LoginTime/FrontID/SessionID`。`time` 是本机 UTC 时间，`elapsed_ms` 使用单调时钟；柜台的 `TradingDay/LoginTime` 保留原值。

结束示例：

```json
{"event":"result","status":"login_ok","login_ok":true,"collection_verified":null,"message":"连接、认证和登录成功；采集是否合格请期货公司确认"}
```

`collection_verified` 始终为 `null`：登录回报没有“华安采集验收通过”这个结论。登录成功后如果观察期断线，`login_ok` 仍为 `true`，但整体 `status` 为 `failed`。退出码 `0` 表示流程完成；`1` 表示连接/认证/登录/观察失败；`2` 表示配置或本地准备失败；`130` 表示手动中断。

与华安核对时提供本次报告中的环境、版本、测试时间和会话信息，再请其确认收到的采集字段及验收结果。报告不会保存完整账号、密码、AuthCode 或原始硬件采集数据；SDK 自身的控制台输出和 flow 文件不属于这份脱敏报告。

## 在 Podman 容器中运行

镜像包含该脚本。使用将来实际部署的 VPS、容器网络和权限运行测试，才能让华安检查那个运行环境产生的采集记录。本地测试成功不能代表 VPS 容器也已验收。

例如在项目根目录运行（`your-image:tag` 换成构建了本次代码的镜像）：

```bash
mkdir -p data/ctp_assessment
podman run --rm \
  -v "$PWD/config.toml:/app/config.toml:ro" \
  -v "$PWD/data/ctp_assessment:/app/data/ctp_assessment" \
  --entrypoint /app/.venv/bin/python \
  your-image:tag \
  /app/script/ctp_assessment.py --hold-seconds 60
```

同一程序和配置会在当前容器中重新建立连接，由 SDK 采集该环境可见的信息；能否满足华安要求需要这次实际测试确认。该命令不额外授予特权、不挂载宿主机硬件目录；如有字段缺失，先让华安明确缺失项及其支持的部署方式。
