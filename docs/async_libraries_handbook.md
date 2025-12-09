# Sổ Tay Thực Chiến: Async IO & Thư Viện Cốt Lõi (Web3, Aiohttp, Uvloop)

Tài liệu này là hướng dẫn chuyên sâu ("Deep Dive") về các công nghệ Async IO cốt lõi được sử dụng trong dự án Crypto Market Analysis System. Mục tiêu là giúp đội ngũ phát triển hiểu rõ **bản chất**, **cách sử dụng chuẩn (best practices)** và tránh các **cạm bẫy (pitfalls)**.

---

## 1. Nền tảng: AsyncIO & uvloop

### 1.1. Bản chất vấn đề
Trong mô hình lập trình đồng bộ (Synchronous/Blocking):
*   Khi code chạy đến dòng `response = requests.get(url)`, toàn bộ chương trình (hoặc thread hiện tại) sẽ **DỪNG LẠI** (blocked).
*   CPU ngồi chơi xơi nước chờ server bên kia trả lời.
*   Để chạy song song, ta phải đẻ thêm Thread. Nhưng Thread tốn RAM và chi phí chuyển đổi (Context Switching).

**AsyncIO** giải quyết bằng **Event Loop (Vòng lặp sự kiện)**:
*   Chỉ có **1 Thread duy nhất** (thường là vậy).
*   Khi gặp lệnh chờ (I/O bound) như gọi API, nó không dừng lại mà "đánh dấu" task đó, rồi chuyển sang làm việc khác ngay lập tức.
*   Khi có kết quả trả về, nó quay lại xử lý tiếp.

### 1.2. uvloop là gì? (`uvloop>=0.19.0`)
*   **Định nghĩa:** Là một bản thay thế (drop-in replacement) cho Event Loop mặc định của Python (`asyncio`).
*   **Cấu tạo:** Được viết bằng **Cython** trên nền tảng **libuv** (thư viện C cực mạnh đứng sau Node.js).
*   **Hiệu năng:** Nhanh gấp **2-4 lần** loop chuẩn của Python. Giúp Python đạt tốc độ ngang ngửa Go hoặc Node.js trong các tác vụ I/O.
*   **Cách dùng:** Cực kỳ đơn giản, chỉ cần 2 dòng code ở điểm khởi đầu chương trình (Entry point).

```python
# Trong file run.py hoặc main.py
import asyncio
import uvloop # Chỉ cần import

def main():
    # Kích hoạt uvloop
    uvloop.install() 
    asyncio.run(app_main())
```

---

## 2. Aiohttp: HTTP Client Siêu Tốc (`aiohttp>=3.9.0`)

Nếu `requests` là súng lục, thì `aiohttp` là súng máy 6 nòng.

### 2.1. ClientSession: Khái niệm quan trọng nhất
*   Trong `requests`, mỗi lần gọi `requests.get()` là một lần mở kết nối TCP mới (tốn kém handshake).
*   Trong `aiohttp`, ta dùng `ClientSession`. Nó giữ một **Connection Pool** (bể kết nối).
    *   Các request gửi đến cùng một host (ví dụ `api.coingecko.com`) sẽ **tái sử dụng** kết nối cũ (Keep-Alive).
    *   Đây là chìa khóa để đạt tốc độ cao.

### 2.2. Mẫu Code Chuẩn (Best Practice)

**❌ SAI (Đừng làm thế này):**
```python
# Tạo session mới cho mỗi request -> Chết vì hết socket (Port exhaustion)
async def get_price(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            return await resp.json()
```

**✅ ĐÚNG (Tái sử dụng Session):**
```python
import aiohttp
import asyncio

async def fetch_url(session: aiohttp.ClientSession, url: str):
    # Context manager 'async with' đảm bảo resource được giải phóng
    async with session.get(url) as response:
        response.raise_for_status() # Bắn lỗi nếu HTTP != 200
        return await response.json()

async def main():
    # Tạo session 1 lần duy nhất ở cấp cao nhất
    timeout = aiohttp.ClientTimeout(total=10) # Timeout 10s
    connector = aiohttp.TCPConnector(limit=100) # Giới hạn 100 connect song song
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Gửi 100 request song song
        tasks = [fetch_url(session, f"https://api.com/items/{i}") for i in range(100)]
        results = await asyncio.gather(*tasks)
```

### 2.3. Cạm bẫy thường gặp
*   **Quên `await`:** `resp.json()` là một coroutine, phải `await resp.json()`. Nếu không bạn chỉ nhận được object coroutine chứ không phải data.
*   **Không đóng Session:** Luôn dùng `async with` để session tự đóng. Nếu không sẽ bị leak memory.

---

## 3. AsyncWeb3 (`web3>=6.0.0`)

Thư viện chuẩn để giao tiếp với Ethereum Node, phiên bản v6+ hỗ trợ Async native.

### 3.1. Sự khác biệt cốt lõi
*   **Web3 (Sync):** Dùng `HTTPProvider` (dựa trên `requests`). Gọi `w3.eth.get_block()` sẽ block thread.
*   **AsyncWeb3 (Async):** Dùng `AsyncHTTPProvider` (dựa trên `aiohttp`). Gọi `await w3.eth.get_block()` không block thread.

### 3.2. Cách cài đặt & Sử dụng

**Khởi tạo:**
```python
from web3 import AsyncWeb3, AsyncHTTPProvider

# Khởi tạo provider Async
# Lưu ý: cache_allowed_requests=True giúp cache lại các request tĩnh (như chainId)
w3 = AsyncWeb3(AsyncHTTPProvider(
    endpoint_uri="https://eth-mainnet.g.alchemy.com/v2/YOUR-KEY",
    request_kwargs={'timeout': 60} # Truyền tham số cho aiohttp bên dưới
))

async def check_connection():
    is_connected = await w3.is_connected()
    print(f"Connected: {is_connected}")
```

**Lấy dữ liệu (Ví dụ lấy Block):**
```python
async def get_block_data(block_number):
    # Lệnh này non-blocking!
    block = await w3.eth.get_block(block_number, full_transactions=True)
    return block
```

### 3.3. Batch Request trong AsyncWeb3
Alchemy và Infura hỗ trợ JSON-RPC Batching (gửi 1 HTTP request chứa 100 lệnh RPC con). Điều này giảm chi phí mạng cực lớn.

Tuy nhiên, `AsyncWeb3` hiện tại hỗ trợ batching chưa tốt bằng bản Sync.
**Giải pháp thay thế (Workaround) hiệu quả:** Dùng `asyncio.gather` để gửi song song 100 request đơn lẻ. Do dùng `aiohttp` với Keep-Alive connection, hiệu năng thực tế **tương đương hoặc nhanh hơn** Batching truyền thống (vì Server có thể xử lý song song thay vì tuần tự trong 1 batch).

```python
# Thay vì gửi 1 Batch request chứa 10 block
# Ta gửi 10 request song song
tasks = [w3.eth.get_block(n) for n in range(1000, 1010)]
blocks = await asyncio.gather(*tasks)
```

---

## 4. Tổng kết chiến lược Refactor

Khi áp dụng vào dự án `crypto-market-analysis-system`:

1.  **Thay `requests` bằng `aiohttp`:** Trong các module lấy giá (`web2_api`).
2.  **Thay `Web3` bằng `AsyncWeb3`:** Trong `EthStreamerAdapter`.
3.  **Tận dụng `asyncio.gather`:** Để thay thế `ThreadPoolExecutor` khi lấy nhiều block.
4.  **Bật `uvloop`:** Để tăng tốc toàn bộ hệ thống.

---
*Tài liệu này được soạn thảo bởi AI Assistant để phục vụ quá trình Refactor codebase.*
