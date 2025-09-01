# ccxt-proxy2

## docker local test
  * `docker-compose down`
  * `docker image prune -a`
  * `docker-compose up --build`
    * or use
    * `docker build --no-cache -t ccxt_proxy2 .`
    * `docker-compose up`

## docker run on linux

  ```
  docker run -d -p 5123:8000 \
  -v ~/ccxt-proxy2:/app/data \
  -e PYTHONUNBUFFERED=1 \
  --name ccxt-proxy2 \
  --restart=always \
  hxse/ccxt-proxy2:latest
  ```
