# ccxt-proxy2

## docker local test
  * `docker-compose down`
  * `docker image prune -a`
  * `docker-compose up --build`
    * or use
    * `docker build -t ccxt_proxy2 .`
    * `docker-compose up`

## docker run on linux

  ```
  docker run -d -p 5123:8000 \
  -e API_TOKEN=default_token \
  -e CCXT_TOKEN=default_ccxt_token \
  -v ~/ccxt-proxy2/cache:/app/cache \
  -v ~/ccxt-proxy2/static:/app/static \
  -e PYTHONUNBUFFERED=1 \
  --name ccxt-proxy2 \
  --restart=always \
  hxse/ccxt-proxy2:latest
  ```
