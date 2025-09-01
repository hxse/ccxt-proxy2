demo_data_html = """
    <html>
        <head><title>测试数据 API</title></head>
        <body>
            <h1>测试数据下载</h1>
            <div>
                <label for="tokenInput">输入 Token:</label>
                <input type="text" id="tokenInput" value="{}">
            </div>

            <button onclick="listFiles()">列出并下载文件</button>

            <div id="result"></div>
            <ul id="fileList"></ul>

            <script>
                function getToken() {{
                    return document.getElementById('tokenInput').value;
                }}

                function listFiles() {{
                    const token = getToken();
                    fetch('/download/list_files', {{
                        headers: {{ 'Authorization': 'Bearer ' + token }}
                    }})
                    .then(response => {{
                        if (!response.ok) throw new Error('Network response was not ok');
                        return response.json();
                    }})
                    .then(data => {{
                        const fileList = document.getElementById('fileList');
                        fileList.innerHTML = ''; // 清空旧列表
                        if (data.files && data.files.length > 0) {{
                            document.getElementById('result').innerText = '文件列表：';
                            data.files.forEach(fileName => {{
                                const li = document.createElement('li');
                                const a = document.createElement('a');
                                // 注意：这里使用 "/download/files/" 前缀来访问静态文件服务
                                a.href = `/download/files/${{fileName}}`;
                                a.textContent = fileName;
                                a.download = fileName; // 确保下载而不是打开
                                li.appendChild(a);
                                fileList.appendChild(li);
                            }});
                        }} else {{
                            document.getElementById('result').innerText = '目录下没有文件。';
                        }}
                    }})
                    .catch(error => {{
                        document.getElementById('result').innerText = 'Error: ' + error.message;
                    }});
                }}
            </script>
        </body>
    </html>
    """

demo_ccxt_html = """
    <html>
        <head><title>测试 CCXT API</title></head>
        <body>
            <h1>测试 CCXT 交易</h1>
            <div>
                <label for="tokenInput">输入 Token:</label>
                <input type="text" id="tokenInput" value="{}">
            </div>
            <div>
                <label for="exchangeSelect">选择交易所:</label>
                <select id="exchangeSelect">
                    <option value="binance">Binance</option>
                    <option value="kraken">Kraken</option>
                </select>
            </div>
            <button onclick="getBalance()">获取余额</button>
            <hr/>
            <div>
                <h2>创建订单</h2>
                <div>
                    <label for="symbolInput">交易对 (Symbol):</label>
                    <input type="text" id="symbolInput" value="BTC/USDT">
                </div>
                <div>
                    <label for="sideInput">方向 (Side):</label>
                    <select id="sideInput">
                        <option value="buy">buy</option>
                        <option value="sell">sell</option>
                    </select>
                </div>
                <div>
                    <label for="amountInput">数量 (Amount):</label>
                    <input type="number" id="amountInput" value="0.001" step="0.0001">
                </div>
                <button onclick="createOrder()">下单</button>
            </div>
            <hr/>
            <div>
                <h2>获取 OHLCV 数据</h2>
                <div>
                    <label for="ohlcvSymbolInput">交易对 (Symbol):</label>
                    <input type="text" id="ohlcvSymbolInput" value="BTC/USDT">
                </div>
                <div>
                    <label for="periodInput">周期 (Period):</label>
                    <input type="text" id="periodInput" value="15m">
                </div>
                <div>
                    <label for="startTimeInput">起始时间 (毫秒时间戳, 可选):</label>
                    <input type="number" id="startTimeInput">
                </div>
                <div>
                    <label for="countInput">数量 (Count, 可选):</label>
                    <input type="number" id="countInput" value="100">
                </div>
                <div>
                    <label for="enableCacheInput">启用缓存 (enable_cache, 默认 true):</label>
                    <input type="checkbox" id="enableCacheInput" checked>
                </div>
                <div>
                    <label for="enableTestInput">启用测试 (enable_test, 默认 false):</label>
                    <input type="checkbox" id="enableTestInput">
                </div>
                <div>
                    <label for="fileTypeSelect">文件类型 (file_type, 默认 .parquet):</label>
                    <select id="fileTypeSelect">
                        <option value=".parquet">.parquet</option>
                        <option value=".csv">.csv</option>
                    </select>
                </div>
                <div>
                    <label for="cacheSizeInput">缓存大小 (cache_size, 默认 1000):</label>
                    <input type="number" id="cacheSizeInput" value="1000">
                </div>
                <div>
                    <label for="pageSizeInput">页面大小 (page_size, 默认 1500):</label>
                    <input type="number" id="pageSizeInput" value="1500">
                </div>
                <button onclick="getOhlcv()">获取 OHLCV 数据</button>
            </div>
            <div id="result"></div>
            <script>
                function getToken() {{
                    return document.getElementById('tokenInput').value;
                }}

                function getExchange() {{
                    return document.getElementById('exchangeSelect').value;
                }}

                function getSymbol() {{
                    return document.getElementById('symbolInput').value;
                }}

                function getSide() {{
                    return document.getElementById('sideInput').value;
                }}

                function getAmount() {{
                    return document.getElementById('amountInput').value;
                }}

                // 统一处理 API 响应的函数
                async function handleResponse(response) {{
                    let errorData = await response.text();
                    try {{
                        // 尝试将响应体解析为 JSON
                        errorData = JSON.parse(errorData);
                    }} catch (e) {{
                        // 如果解析失败，则保留原始文本
                        console.error("Failed to parse error as JSON:", e);
                    }}

                    if (!response.ok) {{
                        // 如果响应状态码不是 200 OK，抛出包含详细信息的错误
                        let errorMessage = `API request failed with status ${{response.status}}: ${{JSON.stringify(errorData, null, 2)}}`;
                        throw new Error(errorMessage);
                    }}

                    return errorData;
                }}

                function getBalance() {{
                    const token = getToken();
                    const exchange_name = getExchange();
                    fetch(`/ccxt/balance?exchange_name=$1{{exchange_name}}`, {{
                        headers: {{ 'Authorization': 'Bearer ' + token }}
                    }})
                    .then(handleResponse)
                    .then(data => {{
                        document.getElementById('result').innerText = JSON.stringify(data, null, 2);
                    }})
                    .catch(error => {{
                        document.getElementById('result').innerText = `Error: $1{{error.message}}`;
                    }});
                }}

                function createOrder() {{
                    const token = getToken();
                    const exchange_name = getExchange();
                    const symbol = getSymbol();
                    const side = getSide();
                    const amount = parseFloat(getAmount());

                    fetch('/ccxt/order', {{
                        method: 'POST',
                        headers: {{
                            'Content-Type': 'application/json',
                            'Authorization': 'Bearer ' + token
                        }},
                        body: JSON.stringify({{
                            symbol: symbol,
                            side: side,
                            amount: amount,
                            exchange_name: exchange_name
                        }})
                    }})
                    .then(handleResponse)
                    .then(data => {{
                        document.getElementById('result').innerText = JSON.stringify(data, null, 2);
                    }})
                    .catch(error => {{
                        document.getElementById('result').innerText = `Error: $1{{error.message}}`;
                    }});
                }}

                // 新增的 OHLCV 测试函数
                function getOhlcv() {{
                    const token = getToken();
                    const exchange_name = getExchange();
                    const symbol = document.getElementById('ohlcvSymbolInput').value;
                    const period = document.getElementById('periodInput').value;
                    const startTime = document.getElementById('startTimeInput').value;
                    const count = document.getElementById('countInput').value;
                    const enableCache = document.getElementById('enableCacheInput').checked;
                    const enableTest = document.getElementById('enableTestInput').checked;
                    const fileType = document.getElementById('fileTypeSelect').value;
                    const cacheSize = document.getElementById('cacheSizeInput').value;
                    const pageSize = document.getElementById('pageSizeInput').value;

                    // 构建查询字符串
                    let queryString = `exchange_name=$1{{exchange_name}}&symbol=$1{{symbol}}&period=$1{{period}}`;
                    if (startTime) {{
                        queryString += `&start_time=$1{{startTime}}`;
                    }}
                    if (count) {{
                        queryString += `&count=$1{{count}}`;
                    }}
                    // 新参数
                    queryString += `&enable_cache=$1{{enableCache}}`; // 明确添加，即使是默认值
                    queryString += `&enable_test=$1{{enableTest}}`; // 明确添加，即使是默认值
                    if (fileType !== '.parquet') {{ // 只有当值与默认值不同时才添加
                        queryString += `&file_type=$1{{fileType}}`;
                    }}
                    if (cacheSize !== '1000') {{ // 只有当值与默认值不同时才添加
                        queryString += `&cache_size=$1{{cacheSize}}`;
                    }}
                    if (pageSize !== '1500') {{ // 只有当值与默认值不同时才添加
                        queryString += `&page_size=$1{{pageSize}}`;
                    }}

                    fetch(`/ccxt/ohlcv?$1{{queryString}}`, {{
                        headers: {{ 'Authorization': 'Bearer ' + token }}
                    }})
                    .then(handleResponse)
                    .then(data => {{
                        document.getElementById('result').innerText = JSON.stringify(data, null, 2);
                    }})
                    .catch(error => {{
                        document.getElementById('result').innerText = `Error: $1{{error.message}}`;
                    }});
                }}

            </script>
        </body>
    </html>
    """
