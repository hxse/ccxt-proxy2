# ccxt-proxy2

## docker local dev
  * `docker-compose down`
  * `docker image prune -a`
  * `docker build --no-cache-filter app -t ccxt_proxy2 . ; docker-compose up`
## local dev
  * `uv run uvicorn src.main:app --host 127.0.0.1 --port 5123 --reload`
    * on windows: `localhost` with --reload is slowly , use `127.0.0.1` to avoid slowly
## docker run on linux

  ```
  docker run -d -p 5123:8000 \
  -v ~/ccxt-proxy2:/app/data \
  -e PYTHONUNBUFFERED=1 \
  --name ccxt-proxy2 \
  --restart=always \
  hxse/ccxt-proxy2:latest
  ```
