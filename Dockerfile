# 固定基础镜像；升级时显式更新摘要。源码变更不使依赖层失效。
FROM docker.io/library/python:3.13-slim-bookworm@sha256:3e2de9c40ca4e3d73240059f9d48baff27908f10293e985a2f382a0378e6df4a AS base

# 依赖阶段：编译器、锁文件和 CTP 源码单独缓存。
FROM base AS dependencies
ARG TARGETARCH
LABEL io.ccxt-proxy2.project="ccxt-proxy2" io.ccxt-proxy2.kind="build-cache"
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir uv==0.12.17
COPY pyproject.toml uv.lock ./
COPY vendor/vnpy_ctp/ ./vendor/vnpy_ctp/
# uv 下载与已编译 wheel 缓存跨依赖层更新复用；运行镜像只复制 .venv。
ENV UV_LINK_MODE=copy UV_CONCURRENT_BUILDS=1
# CTP 使用 Meson/Ninja；单独限制其编译任务，适配小内存 VPS。
RUN --mount=type=cache,id=ccxt-proxy2-uv-${TARGETARCH},target=/root/.cache/uv uv sync --locked --extra ctp --no-dev \
    --config-settings-package=vnpy-ctp:compile-args=-j1

# 运行阶段：复用已构建的依赖，不重复编译。
FROM base AS app
LABEL io.ccxt-proxy2.project="ccxt-proxy2" io.ccxt-proxy2.kind="build-cache"
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 && rm -rf /var/lib/apt/lists/*
# 运行镜像只保留锁定后的虚拟环境，不携带构建源码和开发工具。
COPY --from=dependencies /app/.venv /app/.venv
COPY pyproject.toml uv.lock ./
# 拷贝 src 目录
COPY ./src/ ./src/
COPY ./scripts/collect_market_data.py ./scripts/prune_market_data.py ./scripts/market_data_pipeline.py ./scripts/
ENV PYTHONUNBUFFERED=1
# 发布地址由管理脚本固定为宿主 127.0.0.1；容器内部需接收端口映射。
EXPOSE 5123
CMD ["/app/.venv/bin/python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "5123"]
# 只给最终交付层标记用途，避免将中间构建层识别成旧运行镜像。
LABEL io.ccxt-proxy2.project="ccxt-proxy2" io.ccxt-proxy2.kind="runtime" io.ccxt-proxy2.release="true"
