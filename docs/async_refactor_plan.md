# Kế Hoạch Refactor Hệ Thống: Chuyển đổi sang Async I/O (Asyncio)

Tài liệu này phác thảo lộ trình chi tiết để chuyển đổi kiến trúc hệ thống từ **Multithreading (Synchronous I/O)** sang **Asynchronous I/O (Asyncio)**. Đây là bước đi chiến lược ("Think Harder") nhằm tối ưu hóa hiệu năng cho tầng Ingestion, đặc biệt khi giao tiếp với Ethereum Full Node RPC (như Alchemy, Infura) và Web2 APIs.

## Tại sao cần Async I/O? (Why)

1.  **Hiệu năng vượt trội cho I/O Bound:**
    *   Mô hình hiện tại (`BatchWorkExecutor`) dùng Thread Pool. Khi chờ mạng (RPC call ~200-500ms), Thread bị block hoàn toàn.
    *   Asyncio dùng **Event Loop** đơn luồng. Khi chờ mạng, nó "nhả" CPU để xử lý request khác. Một process có thể xử lý hàng nghìn request đồng thời mà không tốn RAM cho hàng nghìn Thread.
2.  **Giảm Overhead:**
    *   Loại bỏ chi phí chuyển đổi ngữ cảnh (Context Switching) của OS giữa các Thread.
    *   Tránh tranh chấp GIL (Global Interpreter Lock) của Python khi scale số lượng worker lớn.
3.  **Real-time Readiness:**
    *   Phù hợp hoàn hảo cho "Speed Layer" (Streaming), giảm độ trễ (latency) từ lúc Block sinh ra đến khi vào Kafka.

---

## Lộ trình Thực hiện (Step-by-Step)

### Giai đoạn 1: Xây dựng Nền tảng (Infrastructure Foundation)

Mục tiêu: Tạo các tiện ích Async dùng chung mà không ảnh hưởng đến code đang chạy.

#### Bước 1.1: Cài đặt thư viện
Thêm các thư viện Async vào `requirements.txt`:
```text
aiohttp>=3.9.0
web3>=6.0.0  # Hỗ trợ AsyncWeb3
uvloop>=0.19.0 # Event loop siêu tốc (thay thế loop mặc định của Python)
# Note: Giữ nguyên confluent-kafka vì hiệu năng C++ (librdkafka) tốt hơn aiokafka và hỗ trợ Schema Registry mạnh mẽ.
# Hàm produce() của confluent-kafka là bất đồng bộ (non-blocking) nên vẫn dùng tốt trong asyncio.
```

#### Bước 1.2: Tạo Module `utils/async_utils.py`
Xây dựng các hàm helper cơ bản:
*   `create_async_session()`: Tạo `aiohttp.ClientSession` với cấu hình tối ưu (Connection Pooling, Keep-Alive).
*   `async_retry()`: Decorator để retry khi gọi API thất bại (Backoff exponential), tương tự `execute_with_retries` nhưng dùng `await asyncio.sleep()`.

#### Bước 1.3: Cấu hình `AsyncWeb3` Provider
Trong `ingestion/ethereumetl/providers/auto.py`:
*   Thêm logic để khởi tạo `AsyncWeb3` với `AsyncHTTPProvider`.
*   Điều này cho phép chúng ta gọi `await w3.eth.get_block(...)`.

---



### Giai đoạn 3: Refactor Core Ingestion (Quan trọng nhất)

Mục tiêu: Chuyển đổi trái tim của hệ thống - luồng lấy Block/Transaction.

#### Bước 3.1: Xây dựng `AsyncBatchWorkExecutor`
*   Thay thế class `BatchWorkExecutor` (dùng ThreadPool) bằng phiên bản Async.
*   Sử dụng `asyncio.gather()` hoặc `asyncio.TaskGroup` (Python 3.11+) để chạy song song một batch các RPC calls.
*   Sử dụng `asyncio.Semaphore` để giới hạn số lượng request đồng thời (Rate Limiting) thay vì `max_workers`.

#### Bước 3.2: Viết lại `EthStreamerAdapter`
*   File: `ingestion/ethereumetl/streaming/eth_streamer_adapter.py`.
*   Chuyển đổi method `open()`, `get_current_block_number()`, `export_blocks_and_transactions()` sang `async def`.
*   Logic gọi RPC:
    *   Cũ: `self.batch_web3_provider.make_batch_request(...)` (Blocking).
    *   Mới: `await asyncio.gather(*[w3.eth.get_block(n) for n in batch])`.

#### Bước 3.3: Refactor `Streamer` Loop
*   File: `ingestion/blockchainetl/streaming/streamer.py`.
*   Chuyển vòng lặp chính `stream()` sang async.
*   Thay `time.sleep()` bằng `await asyncio.sleep()`.

---

### Giai đoạn 4: Tối ưu hóa & Scale (Advanced)

#### Bước 4.1: Sử dụng `uvloop`
*   Trong `run.py`, cấu hình sử dụng `uvloop` làm Event Loop Policy mặc định. `uvloop` được viết bằng Cython (trên libuv), nhanh gấp 2-4 lần loop chuẩn của Python.

#### Bước 4.2: Pipelining (Producer-Consumer Pattern)
Thay vì xử lý tuần tự (Lấy Batch -> Xử lý -> Gửi Kafka -> Lặp lại), hãy tách luồng:
*   **Task 1 (Producer):** `fetch_blocks()` liên tục đẩy dữ liệu thô vào `asyncio.Queue`.
*   **Task 2 (Consumer):** `process_and_export()` liên tục lấy từ Queue, map dữ liệu và gửi Kafka.
*   Giúp tận dụng triệt để thời gian CPU lúc chờ mạng.

---

## Ví dụ Code Minh Họa (Comparison)

### Code cũ (Blocking)
```python
# BatchWorkExecutor
def execute(self, items):
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(self._fetch_item, items) # Blocked here
    return list(results)
```

### Code mới (Async)
```python
# AsyncBatchWorkExecutor
async def execute(self, items):
    async with aiohttp.ClientSession() as session:
        tasks = [self._fetch_item(session, item) for item in items]
        # Chạy tất cả song song, "nhả" CPU khi chờ mạng
        results = await asyncio.gather(*tasks) 
    return results
```
