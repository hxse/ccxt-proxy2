"""只在应用镜像内部执行的配置预检；不初始化 Provider。"""

from pathlib import PurePosixPath

from src.tools.config_loader import load_config
from src.tools.market_data_config import (
    has_work,
    load_market_data_plan,
    validate_client,
)

try:
    config = load_config()
    plan = load_market_data_plan()
    validate_client(plan, config)
    if has_work(plan, config):
        assert config.market_data_client.base_url == "http://127.0.0.1:5123"
    paths = [config.ohlcv_cache.database_path]
    if config.ctp is not None:
        paths.append(config.ctp.flow_path)
    for raw in paths:
        path = PurePosixPath(raw)
        assert ".." not in path.parts
        assert (
            path.is_relative_to("/app/data")
            if path.is_absolute()
            else path.is_relative_to("data")
        )
except Exception:
    raise SystemExit(
        "容器配置无效：请检查 TOML、后台用户/5123 地址、data 持久化路径；值已隐藏"
    ) from None
