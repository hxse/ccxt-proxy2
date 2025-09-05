# Stage 1: dependencies (依赖阶段)
FROM python:3.13-slim AS dependencies
WORKDIR /app
RUN pip install uv
COPY pyproject.toml uv.lock ./
RUN uv sync

# Stage 2: app (应用阶段)
FROM python:3.13-slim AS app
WORKDIR /app
# 从 dependencies 阶段拷贝所有依赖文件和 uv 可执行文件
COPY --from=dependencies /usr/local/bin/uv /usr/local/bin/uv
COPY --from=dependencies /app /app
# 拷贝 src 目录
COPY ./src/ ./src/
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uv", "run", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

