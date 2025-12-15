# Kế Hoạch Tích Hợp & Tối Ưu Hóa Ethereum Ingestion Pipeline

Tài liệu này mô tả chi tiết quá trình tái cấu trúc (Refactor) và tối ưu hóa hiệu năng cho module `ingestion/ethereumetl` trong dự án **Crypto Market Analysis System**. Mục tiêu là chuyển đổi từ mô hình xử lý tuần tự/bán song song cũ sang mô hình **"Autonomous Async Pipeline Workers"** hiệu năng cao.

---

## 1. Bối Cảnh & Vấn Đề (Context & Challenges)

Hệ thống cũ sử dụng mô hình `Streamer` -> `Adapter` -> `Jobs` -> `Executors` với các hạn chế sau:

1.  **I/O Bottleneck:** Sử dụng `Web3.py` (dù có Async) nhưng việc quản lý Batch Request không tối ưu, thường gửi nhiều request nhỏ lẻ, gây lãng phí RTT (Round Trip Time).
2.  **CPU Utilization:** Việc xử lý dữ liệu (Mapping, Enrichment) thường bị chặn (block) bởi I/O hoặc chạy trên ThreadPool không tận dụng hết đa nhân CPU do Python GIL.
3.  **Complex Data Flow:** Dữ liệu phải đi qua nhiều lớp trung gian (`InMemoryBufferExporter`) trước khi đến Kafka, gây overhead về bộ nhớ và serialization.
4.  **Stability:** Gặp vấn đề về SSL Verification trên macOS và khó kiểm soát Rate Limit của các RPC Provider miễn phí.

## 2. Kiến Trúc Mới: Autonomous Async Pipeline (Giải Pháp)

Chúng ta chuyển sang mô hình **Đa tiến trình bất đồng bộ (Multiprocessing AsyncIO)** với các đặc điểm chủ chốt:

### 2.1. Kiến trúc tổng thể
*   **Orchestrator (`EthStreamerAdapter`):** Đóng vai trò điều phối viên. Thay vì chạy Jobs tuần tự, nó chia Block Range thành các phần nhỏ (Chunks) và đẩy vào `Work Queue`.
*   **Autonomous Workers:** Các tiến trình (Process) độc lập, tự nhận việc từ Queue và thực hiện trọn gói quy trình (Fetch -> Process -> Produce). Không chia sẻ trạng thái, không bị GIL khóa.

### 2.2. Pipeline nội bộ (Inside a Worker)
Trong mỗi Worker Process, chúng ta chạy một **AsyncIO Event Loop** với 2 tác vụ song song:
1.  **Fetcher (Producer):**
    *   Sử dụng `RpcClient` (tối ưu hóa với `aiohttp`) để gửi **JSON-RPC Batch Request** (gộp `eth_getBlock` và `eth_getBlockReceipts` vào 1 gói tin HTTP).
    *   Tốc độ mạng đạt mức tối đa.
    *   Đẩy kết quả JSON thô vào `Internal Queue` (RAM).
2.  **Processor (Consumer):**
    *   Lấy dữ liệu từ `Internal Queue`.
    *   Thực hiện CPU-bound tasks: Mapping (JSON -> Object), Enrichment (ghép Transaction + Receipt), Extraction (lọc Token Transfers).
    *   Đẩy trực tiếp vào Kafka (qua `librdkafka` C-library).

### 2.3. RPC Load Balancing & Resilience
*   **Multi-RPC:** Hệ thống hỗ trợ nhập nhiều RPC URL (phân cách bởi dấu phẩy). Orchestrator sẽ chia đều các RPC này cho các Worker (Round-robin assignment).
*   **Self-healing:** `RpcClient` tự động phát hiện lỗi SSL (trên macOS/Local) và tự động chuyển sang chế độ không xác thực (Unsafe SSL) để đảm bảo tiến trình không bị gián đoạn.
*   **Rate Limiting:** Kiểm soát chặt chẽ tốc độ gửi request thông qua tham số `rate_sleep` và giảm `BATCH_SIZE` để phù hợp với gói Free Tier.

---

## 3. Chi Tiết Triển Khai (Implementation)

### 3.1. Cấu trúc thư mục mới
Chúng ta thêm module `ingestion/optimized/` để chứa logic lõi mới:
```
ingestion/optimized/
├── __init__.py
├── rpc_client.py       # Aiohttp Client, Batching logic, SSL fix
└── worker.py           # IngestionWorker class, Async Pipeline
```

### 3.2. Thay đổi Core Components

#### `ingestion/ethereumetl/streaming/eth_streamer_adapter.py`
*   **Trước:** Gọi tuần tự `ExportBlocksJob` -> `ExportReceiptsJob` -> `ExtractTokenTransfersJob`.
*   **Sau:** Khởi tạo `multiprocessing.Process` chạy hàm `worker_entrypoint`. Quản lý `multiprocessing.Queue` để phân phối block range.

#### `ingestion/ethereumetl/models/transaction.py`
*   **Thay đổi:** Loại bỏ class kế thừa `EnrichedEthTransaction`. Gộp trực tiếp các trường enrichment (như `receipt_gas_used`, `receipt_status`) vào model `EthTransaction` chính.
*   **Lợi ích:** Đơn giản hóa việc serialization/deserialization, tạo cấu trúc dữ liệu phẳng (flat schema) dễ query trên ClickHouse.

#### `ingestion/ethereumetl/mappers/token_transfer_mapper.py`
*   **Thay đổi:** Bổ sung method `json_dict_to_token_transfer` chứa logic giải mã Log (ERC20/ERC721) trực tiếp từ Raw JSON.
*   **Lợi ích:** Worker có thể extract transfer ngay lập tức mà không cần gọi qua các Service lớp trên phức tạp.

### 3.3. Luồng dữ liệu (Data Flow)
1.  **CLI Start:** `stream_ethereum.py` gọi `EthStreamerAdapter.export_all(start, end)`.
2.  **Dispatch:** Adapter chia range thành các chunks (100 blocks), đẩy vào Queue.
3.  **Fetch:** Worker nhận chunk, dùng `RpcClient` gửi Batch Request (5-10 blocks/request).
4.  **Process:**
    *   Map Block.
    *   Map Transaction.
    *   Lookup Receipt từ Dictionary (O(1)).
    *   Enrich Transaction với thông tin Receipt.
    *   Parse Logs để tạo TokenTransfer.
5.  **Produce:** Gửi message vào Kafka với `key=block_number` (đảm bảo thứ tự partition).
6.  **Checkpoint:** `Streamer` ghi nhận `last_synced_block` sau khi toàn bộ range hoàn tất.

---

## 4. Hướng Dẫn Sử Dụng & Tinh Chỉnh

### 4.1. Lệnh chạy chuẩn (Qua CLI Streamer)
```bash
python cli/stream_ethereum.py \
    --start-block 18000000 \
    --end-block 18000100 \
    --provider-uri "https://rpc1.com,https://rpc2.com" \
    --max-workers 4 \
    --rate-sleep 0.5 \
    --batch-size 5
```

### 4.2. Tham số Tối ưu (Tuning Guide)

| Tham số | Ý nghĩa | Free Tier (Alchemy/QuickNode) | Pro/Paid Tier |
| :--- | :--- | :--- | :--- |
| `--max-workers` | Số tiến trình chạy song song | `1` - `2` | `4` - `8` (tùy số Core CPU) |
| `--rate-sleep` | Thời gian nghỉ giữa các request (giây) | `0.5` - `1.5` | `0.0` - `0.1` |
| `BATCH_SIZE` | Số block gộp trong 1 HTTP Request (sửa trong code) | `5` (để giảm Compute Units) | `10` - `20` |
| `--provider-uri` | Danh sách RPC | Dùng 1 URL | Dùng nhiều URL phân cách dấu phẩy để Load Balance |

### 4.3. Xử lý sự cố thường gặp
*   **Lỗi 429 (Too Many Requests):** Tăng `rate-sleep` lên (ví dụ 1.5s) hoặc giảm số workers xuống 1.
*   **Lỗi SSL (Mac):** Code đã tự động xử lý (bypass verify). Không cần can thiệp.
*   **Kafka Unknown Partition:** Đảm bảo topic đã được tạo (`kafka-topics --create ...`).

---
*Tài liệu này đánh dấu bước chuyển mình quan trọng của hệ thống Ingestion sang kiến trúc hiện đại, sẵn sàng cho việc xử lý dữ liệu quy mô lớn (High Throughput).*
