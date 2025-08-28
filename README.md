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
  docker run -d -p 8000:8000 \
  -e API_TOKEN=mytoken \
  -e CCXT_TOKEN=your_token \
  -v $(pwd)/cache:/app/cache \
  -v $(pwd)/static:/app/static \
  -e PYTHONUNBUFFERED=1 \
  --name ccxt-proxy2 \
  --restart=always \
  hxse/ccxt-proxy2:latest
  ```

## docker run on windows
  ```
  docker run -d -p 8000:8000 `
  -e API_TOKEN=mytoken `
  -e CCXT_TOKEN=your_token `
  -v ${PWD}/cache:/app/cache `
  -v ${PWD}/static:/app/static `
  -e PYTHONUNBUFFERED=1 `
  --name ccxt-proxy2 `
  --restart=always `
  hxse/ccxt-proxy2:latest
  ```
