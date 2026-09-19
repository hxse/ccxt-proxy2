# Stage 1: dependencies (依赖阶段)
FROM docker.io/library/python:3.13-slim AS dependencies
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
RUN pip install uv
COPY pyproject.toml uv.lock ./
COPY vendor/vnpy_ctp/ ./vendor/vnpy_ctp/
RUN uv sync --locked --extra ctp --no-dev

# Stage 2: app (应用阶段)
FROM docker.io/library/python:3.13-slim AS app
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 && rm -rf /var/lib/apt/lists/*
# 运行镜像只保留锁定后的虚拟环境，不携带构建源码和开发工具。
COPY --from=dependencies /app/.venv /app/.venv
COPY pyproject.toml uv.lock ./
# 拷贝 src 目录
COPY ./src/ ./src/
COPY ./script/ctp_assessment.py ./script/ctp_assessment.py
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["/app/.venv/bin/python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
