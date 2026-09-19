# VeighNa CTP 交易模块

上游为 [vnpy/vnpy_ctp](https://github.com/vnpy/vnpy_ctp)，使用 [VeighNa 的 PyPI 发布包](https://pypi.org/project/vnpy_ctp/)。当前上游版本、下载地址和 SHA-256 记录在 [upstream.json](upstream.json)；本地版本附加 `+ccxtproxy.2`，产物校验值见 [SHA256SUMS](SHA256SUMS)。MIT 许可证保留在源码包内。

应用只通过 `src/tools/ctp_native.py` 加载底层 `TdApi`。uv 排除 `vnpy` 框架依赖；不导入上游包入口中的 `CtpGateway`，不伪造 `vnpy` 模块。构建仅产出交易扩展及其原生交易库，不编译或安装行情扩展、行情库；不需要 Qt、图形界面或 VeighNa 引擎。包内其余 Python 文件保留上游原样，但不会加载。

[trading-api.patch](trading-api.patch) 和[构建脚本](../../scripts/build_vnpy_ctp_source.py)记录全部源码调整：

- `exit` 在等待工作线程时释放 GIL，避免回调等待 GIL 导致死锁。
- 关闭标志使用原子变量，终止回调队列时持有队列锁，避免线程竞争和丢失唤醒。
- 回调数据和错误信息由 `shared_ptr` 管理。终止队列时丢弃积压任务，拒绝迟到入队；已取出的任务在处理结束后释放，避免泄漏或重复释放。构建脚本对上游生成代码统一替换分配、指针读取和手动删除，并核对各类替换数量；结构变化时停止构建。
- 在持有 GIL 的回报转换阶段，用 Python 的 GB18030 解码器替代系统 locale。无需在 NixOS 或精简容器中额外安装 GB18030 语言包；无效编码替换为 Unicode 替代字符。
- 只编译和安装交易 API。

另外仅调整发行版本元数据和补丁涉及文件的文本换行。**上期技术提供的原生 CTP 二进制不做修改**；采集功能由交易 SDK 提供。运行仍需要信任上期技术的原生库及 VeighNa 的分发渠道。

## 更新

在项目根目录执行：

```sh
just update-ctp                 # 官方最新稳定版
just update-ctp 6.7.11.4         # 指定官方版本
```

升级先从官方 PyPI 元数据取得源码地址和哈希，再校验源码、严格应用补丁，并检查现有详细响应模型是否覆盖所有回报字段。字段或补丁上下文发生变化时停止，适配后再继续。随后更新 `uv.lock`、同步 CTP extra 并运行离线回归。不会读取交易账户配置或连接真实交易前置；原生回归只使用无前置连接或本机假 TCP 前置。

升级不需要手工搬运二进制。源码包和锁文件应一起提交；本地和容器均安装锁定版本，通过回归后再重建部署镜像。普通启动不会追踪上游最新版本。

## 重建与核验

将 `upstream.json` 中对应的官方源码包下载到本地后执行：

```sh
just verify-ctp-source /path/to/vnpy_ctp-6.7.11.4.tar.gz
```

也可执行 `uv run --no-sync python scripts/build_vnpy_ctp_source.py --check`，按清单下载并校验。重建需要 Python 3.13+ 和 `patch`，固定文件顺序、属主、时间及 gzip header；应得到逐字节一致的源码包。编译安装需要 C++ 编译器，构建依赖由 uv 在隔离环境安装，运行镜像不包含开发依赖。
