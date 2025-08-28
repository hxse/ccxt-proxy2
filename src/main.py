import os
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import ccxt
from pydantic import BaseModel

app = FastAPI()

# 环境变量：API Token 和 CCXT Token
API_TOKEN = os.getenv("API_TOKEN", "default_token")
CCXT_TOKEN = os.getenv("CCXT_TOKEN")  # 假设这是 API key，实际根据交易所调整

# 缓存目录和文件暴露目录（Docker 卷映射）
CACHE_DIR = "/app/cache"
STATIC_DIR = "/app/static"

# Bearer Token 鉴权
security = HTTPBearer()


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return credentials.credentials


# 初始化 ccxt 交易所（示例用 Binance，实际替换）
exchange = ccxt.binance(
    {
        "apiKey": CCXT_TOKEN,  # 假设 token 是 API key，需添加 secret 如果需要
        # 'secret': os.getenv("CCXT_SECRET"),  # 如果需要 secret，添加环境变量
    }
)


@app.get("/", response_class=HTMLResponse)
def root():
    return """
    <html>
    <head><title>主页</title></head>
    <body>
        <h1>API 测试主页</h1>
        <a href="/test_data">测试数据 API</a><br>
        <a href="/test_ccxt">测试 CCXT API</a>
    </body>
    </html>
    """


# 路由1: GET 数据（示例返回缓存数据）
@app.get("/get_data")
def get_data(token: str = Depends(verify_token)):
    cache_file = os.path.join(CACHE_DIR, "data.txt")
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            data = f.read()
        return {"data": data}
    else:
        with open(cache_file, "w") as f:
            f.write("Sample cached data")
        return {"data": "Sample cached data (newly created)"}


# 路由2: 文件服务器（暴露静态文件，可以 fetch 下载）
app.mount("/files", StaticFiles(directory=STATIC_DIR, html=False), name="files")


# 路由3-4: 执行交易，对应 ccxt API（示例：获取余额和下单）
class Order(BaseModel):
    symbol: str
    side: str  # buy/sell
    amount: float


@app.get("/ccxt/balance")
def get_balance(token: str = Depends(verify_token)):
    try:
        balance = exchange.fetch_balance()
        return {"balance": balance}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ccxt/order")
def create_order(order: Order, token: str = Depends(verify_token)):
    try:
        result = exchange.create_order(order.symbol, "market", order.side, order.amount)
        return {"order": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/list_files")
def list_files(token: str = Depends(verify_token)):
    try:
        files = [
            f
            for f in os.listdir(STATIC_DIR)
            if os.path.isfile(os.path.join(STATIC_DIR, f))
        ]
        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 路由：简单前端按钮测试 API
@app.get("/test_data", response_class=HTMLResponse)
def test_data():
    default_token = "default_token"
    html_content = f"""
    <html>
        <head><title>测试数据 API</title></head>
        <body>
            <h1>测试数据下载</h1>
            <div>
                <label for="tokenInput">输入 Token:</label>
                <input type="text" id="tokenInput" value="{default_token}">
            </div>

            <button onclick="fetchData()">获取缓存数据</button>
            <button onclick="listFiles()">列出并下载文件</button>

            <div id="result"></div>
            <ul id="fileList"></ul>

            <script>
                function getToken() {{
                    return document.getElementById('tokenInput').value;
                }}

                function fetchData() {{
                    const token = getToken();
                    fetch('/get_data', {{
                        headers: {{ 'Authorization': 'Bearer ' + token }}
                    }})
                    .then(response => {{
                        if (!response.ok) throw new Error('Network response was not ok');
                        return response.json();
                    }})
                    .then(data => {{
                        document.getElementById('result').innerText = JSON.stringify(data, null, 2);
                    }})
                    .catch(error => {{
                        document.getElementById('result').innerText = 'Error: ' + error.message;
                    }});
                }}

                function listFiles() {{
                    const token = getToken();
                    fetch('/list_files', {{
                        headers: {{ 'Authorization': 'Bearer ' + token }}
                    }})
                    .then(response => {{
                        if (!response.ok) throw new Error('Network response was not ok');
                        return response.json();
                    }})
                    .then(data => {{
                        const fileList = document.getElementById('fileList');
                        fileList.innerHTML = ''; // 清空旧列表
                        data.files.forEach(fileName => {{
                            const li = document.createElement('li');
                            const a = document.createElement('a');
                            a.href = `/files/${{fileName}}`;
                            a.textContent = fileName;
                            a.download = fileName; // 确保下载而不是打开
                            li.appendChild(a);
                            fileList.appendChild(li);
                        }});
                    }})
                    .catch(error => {{
                        document.getElementById('result').innerText = 'Error: ' + error.message;
                    }});
                }}
            </script>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)


# 路由：简单前端按钮测试 ccxt API
@app.get("/test_ccxt", response_class=HTMLResponse)
def test_ccxt():
    your_ccxt_token_here = "your_ccxt_token_here"
    html_content = f"""
    <html>
        <head><title>测试 CCXT API</title></head>
        <body>
            <h1>测试 CCXT 交易</h1>
            <div>
                <label for="tokenInput">输入 Token:</label>
                <input type="text" id="tokenInput" value="{your_ccxt_token_here}">
            </div>
            <button onclick="getBalance()">获取余额</button>
            <button onclick="placeOrder()">下单 (示例: BUY BTC/USDT 0.001)</button>
            <div id="result"></div>
            <script>
                function getToken() {{
                    return document.getElementById('tokenInput').value;
                }}

                function getBalance() {{
                    const token = getToken();
                    fetch('/ccxt/balance', {{
                        headers: {{ 'Authorization': 'Bearer ' + token }}
                    }})
                    .then(response => response.json())
                    .then(data => document.getElementById('result').innerText = JSON.stringify(data));
                }}

                function placeOrder() {{
                    const token = getToken();
                    fetch('/ccxt/order', {{
                        method: 'POST',
                        headers: {{
                            'Content-Type': 'application/json',
                            'Authorization': 'Bearer ' + token
                        }},
                        body: JSON.stringify({{ symbol: 'BTC/USDT', side: 'buy', amount: 0.001 }})
                    }})
                    .then(response => response.json())
                    .then(data => document.getElementById('result').innerText = JSON.stringify(data));
                }}
            </script>
        </body>
    </html>
    """
    return html_content
