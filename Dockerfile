FROM python:3.13-slim
WORKDIR /app
RUN pip install uv
COPY pyproject.toml uv.lock ./
RUN uv sync
COPY src/ ./src/
RUN mkdir -p /app/cache
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
ENV API_TOKEN=default_token
ENV CCXT_TOKEN=default_ccxt_token
CMD ["uv", "run", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
