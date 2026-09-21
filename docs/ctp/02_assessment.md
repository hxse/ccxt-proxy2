# 使用完整 VeighNa 与期货公司联调

入口：[script/ctp_assessment.py](../../script/ctp_assessment.py)。在有图形桌面的主机上运行：

```bash
just ctp-assessment
```

该命令通过 uv 在独立环境中准备 Python 3.13、完整 `vnpy==4.4.0`、官方 `vnpy_ctp==6.7.11.4` 和 `vnpy_riskmanager==2.0.0`，随后打开 VeighNa Trader。首次会下载并可能编译依赖，后续复用 uv 的包缓存；不会把完整框架写入项目的 `pyproject.toml`、`uv.lock` 或后台 `.venv`。

核心命令封装在 [justfile](../../justfile) 中：

```bash
uv run --no-project --no-config --isolated --python 3.13 \
  --with vnpy==4.4.0 --with vnpy_ctp==6.7.11.4 \
  --with vnpy_riskmanager==2.0.0 --with 'pydantic>=2,<3' \
  python script/ctp_assessment.py
```

`--with` 是 uv 的临时依赖方式，参见 [uv 脚本说明](https://docs.astral.sh/uv/guides/scripts/#running-a-script-with-dependencies)。`--no-project --no-config --isolated` 让此命令独立于后台的依赖、裁剪包来源和 `exclude-dependencies` 设置。

## 使用方法

1. 在现有 `config.toml` 的 `[ctp.test]` 填入期货公司提供的前置、账号和认证信息。默认也沿用 `CCXT_PROXY_CONFIG_PATH`，不需要另建账号配置文件。
2. 运行 `just ctp-assessment`，点击“系统 → 连接CTP”。脚本直接使用启动时读取的 TOML 配置，不另存带密码的 `connect_ctp.json`。
3. 在标准界面中查看委托、成交、持仓、资金、日志及合约；在“功能”菜单打开风控模块，按期货公司的测试清单设置规则和执行操作。
4. 手动退出窗口结束测试。窗口打开本身不会连接前置或报单；点击连接后，官方网关会认证、登录、自动确认结算、查询合约及轮询资金/持仓。下单和撤单由你的界面操作触发。

这里使用完整的 `EventEngine → MainEngine → CtpGateway → MainWindow` 启动方式，参见 [VeighNa CTP 官方示例](https://github.com/vnpy/vnpy_ctp/blob/main/README.md)；风控通过 `RiskManagerApp` 加载，参见 [官方风控模块](https://github.com/vnpy/vnpy_riskmanager)。

## 配置与参数

原有 TOML 字段继续使用，不改变后台配置格式：

| TOML 字段 | VeighNa 网关字段 |
| --- | --- |
| `investor_id` | 用户名 |
| `password` | 密码 |
| `broker_id` | 经纪商代码 |
| `trader_front` | 交易服务器 |
| `app_id` | 产品名称 |
| `auth_code` | 授权编码 |
| `production_mode=true/false` | 柜台环境：实盘/测试 |

官方网关把同一个用户名用于 UserID 和 InvestorID，因此可选 `user_id` 必须省略或等于 `investor_id`；两者不同会明确报错。

**`sandbox/live` 选择账号分组，`production_mode` 选择 SDK 密钥环境，两者独立。** SimNow 使用 `production_mode=true`；期货公司穿透式评测按其要求通常使用 `false`。不要因为配置放在 `ctp.test` 就把 SimNow 改为评测密钥。

```bash
# 查看参数。
just ctp-assessment --help

# 使用默认 config.toml 的 ctp.live。
just ctp-assessment --mode live

# 期货公司同时提供行情前置时，显式传入；替换为实际地址。
just ctp-assessment --md-front tcp://md-front.example:12345
```

`--config` 可覆盖配置文件路径，但通常省略；`--output-dir` 可指定界面设置、风控配置、日志及 flow 的父目录，默认 `data/ctp_assessment`。每种模式独立使用 `<父目录>/<sandbox或live>/vnpy/.vntrader`，不会复用后台 flow 或其他 VeighNa 程序的数据目录。账号在启动时读取一次，修改后重启该脚本。

没有 `--md-front` 时，通过官方网关的交易 API 连接，仅跳过空行情前置；提供后使用标准网关同时连接交易和行情。行情地址不猜测、不写进现有 TOML。GUI 中没有行情不等于交易连接失败。

原来的 `--timeout`、`--hold-seconds` 和 `report.jsonl` 登录探测报告已由完整交互界面替代。原生回报和风控结果通过 VeighNa 的日志和各监控窗口查看，界面数据可能含账户信息。

## NixOS 与运行环境

完整 GUI 需要桌面会话和 Qt 系统库。首次从源码安装 CTP/风控扩展需要 C++ 编译器。NixOS 的 just 入口会通过 `nix build --no-link` 按需准备 Qt 所需运行库、GB18030 locale 和 `dmidecode`，仅设置本次命令的环境变量，不修改 NixOS 系统配置或生成项目 Nix 文件。非 NixOS 主机需自行提供相应系统依赖。

`dmidecode` 可执行文件存在不代表它有权限读取全部 DMI 数据；此入口不提升权限。采集完整性仍需期货公司在后台确认。

后台 Podman 镜像继续用于 HTTP 服务，不会安装完整 GUI 环境。这个桌面联调入口需要在具备 uv、just 和图形环境的主机上运行；若要放到容器中，需要另外准备图形会话和系统库。

## 测试结果的范围

完整界面便于执行期货公司的测试项目、查看风控结果和截图，是否通过仍由其验收。临时环境使用官方完整网关，不使用后台的 `+ccxtproxy.2` 裁剪补丁；其 SDK 行为及系统要求遵循官方版本。

GUI 的风控模块只对这个联调进程生效，不会自动加入 HTTP 后台。应向期货公司说明最终使用的交易程序和部署环境，不能把这个独立 GUI 的测试结果直接视为后台服务已经通过验收。
