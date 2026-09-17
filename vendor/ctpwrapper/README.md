# ctpwrapper 本地补丁版本

版本：`6.7.13+ccxtproxy.1`。基于 PyPI 的 `ctpwrapper==6.7.13`，仅修改交易 API 的释放实现及版本元数据。

上游 `TraderApiWrapper.Release()` 持有 Python GIL 等待原生线程退出；该线程上的回调又需要 GIL，会在断线/关闭并发时死锁。本补丁先在 GIL 内清空实例指针，保证重复释放安全，再在 `with nogil` 内注销 SPI 并释放 API，最后销毁已结束回调的 SPI。

- [release-gil.patch](release-gil.patch)：完整可审查的源码改动。
- `ctpwrapper-6.7.13+ccxtproxy.1.tar.gz`：固定源码包，保留上游原生库、源码和 LICENSE，可在支持的平台上编译。
- [SHA256SUMS](SHA256SUMS)：补丁源码包校验值。

根目录 `pyproject.toml` 的 `tool.uv.sources` 将 CTP extra 指向此源码包；本地及 Docker 均通过 `uv sync --locked --extra ctp` 安装。源码包与补丁一起纳入版本管理。无需修改已安装环境；未启用 CTP extra 时不安装它。

## 重建与核验

在项目根目录执行；重建只需要 Python 3.13+ 和 `patch`。安装源码包另需 C++ 编译器。

```bash
curl --fail --location 'https://files.pythonhosted.org/packages/b7/d7/ba4b0482f6182c5326089652773a4efaa41a09f5cdde4c815ac4f0871c42/ctpwrapper-6.7.13.tar.gz' --output /tmp/ctpwrapper-6.7.13.tar.gz
uv run --no-sync python scripts/build_ctpwrapper_source.py --source /tmp/ctpwrapper-6.7.13.tar.gz --check
```

上游源码 SHA-256：`94be58b8360e26f6c3b57f5a991a6c5bdac0f7707234e41ceb69c42c006e778e`。脚本先校验来源，再应用补丁；固定 tar/gzip 时间与文件顺序，`--check` 验证重建产物逐字节一致。去掉 `--check` 可重新生成源码包和校验文件。

上游来源：[ctpwrapper v6.7.13](https://github.com/nooperpudd/ctpwrapper/tree/v6.7.13)。封装代码许可证为 LGPL-3.0-or-later，完整 LICENSE 包含在源码包内。本地补丁尚未由上游维护者确认或合并。
