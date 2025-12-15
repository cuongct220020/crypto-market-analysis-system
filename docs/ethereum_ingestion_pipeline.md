# Ethereum Data Ingestion Pipeline

## 1. Architecture Overview

Hệ thống được thiết kế theo kiến trúc **High-Performance Asynchronous Streaming**, tối ưu hóa cho việc ingestion lượng lớn dữ liệu blockchain trong thời gian thực.

**Key Technologies:**
*   **Python Multiprocessing:** Vượt qua giới hạn GIL để tận dụng đa nhân CPU.
*   **AsyncIO & uvloop:** Tăng tốc độ I/O (Network requests) lên gấp nhiều lần so với luồng đồng bộ.
*   **Kafka & Avro:** Streaming data backbone với schema validation chặt chẽ.

### Data Flow Diagram

```mermaid
graph TD
    CLI[CLI Command] -->|Start| Streamer[Streamer (Main Process)]
    Streamer -->|Orchestrate| Adapter[EthStreamerAdapter]
    Adapter -->|1. Generate Chunks| JobQueue[Job Queue (Multiprocessing)]
    
    subgraph "Worker Layer (Parallel Processes)"
        W1[Worker 1]
        W2[Worker 2]
        WN[Worker N]
    end
    
    JobQueue --> W1
    JobQueue --> W2
    JobQueue --> WN
    
    subgraph "Network Layer (AsyncIO)"
        RPC[JSON-RPC Providers]
    end
    
    W1 <-->|Batch RPC Requests| RPC
    W2 <-->|Batch RPC Requests| RPC
    
    subgraph "Processing Layer"
        Enricher[EthDataEnricher]
        Analyzer[EthContractAnalyzer]
        Mapper[Mappers]
    end
    
    W1 --> Enricher
    Enricher --> Mapper
    Enricher --> Analyzer
    
    subgraph "Sink Layer"
        Kafka[Kafka Broker]
        Schema[Schema Registry]
    end
    
    Mapper -->|Avro Serialize| Schema
    Mapper -->|Publish| Kafka
```

## 2. Pipeline Stages

### 2.1. Orchestration (Main Process)
*   **Component:** `Streamer` & `EthStreamerAdapter`
*   **Role:**
    *   Quản lý `last_synced_block`.
    *   Chia nhỏ dải block (ví dụ 1M blocks) thành các `chunks` nhỏ (ví dụ 10 blocks) để cân bằng tải.
    *   Khởi tạo pool các `IngestionWorker`.

### 2.2. Ingestion (Worker Process)
Mỗi Worker hoạt động độc lập với một Event Loop riêng (`uvloop`), thực hiện chu trình:

1.  **Fetching (Network Bound):**
    *   Sử dụng `RpcClient` với `aiohttp` Session bền vững (persistent connection).
    *   Gửi **Batch Requests** để giảm RTT (Round Trip Time).
2.  **Enrichment (CPU Bound):**
    *   Parse JSON response.
    *   Kết hợp dữ liệu từ `Block` và `Receipts` để làm giàu thông tin Transaction (Gas used, Status, Logs).
    *   Trích xuất `TokenTransfers` từ Event Logs.
3.  **Smart Contract Processing:**
    *   Tự động phát hiện địa chỉ Contract mới từ `Receipt` (trường `contractAddress`).
    *   Phân tích Bytecode để xác định chuẩn (ERC20, ERC721, Proxy Pattern).
    *   Fetch metadata (Name, Symbol, Supply) sử dụng `eth_call`.
4.  **Exporting:**
    *   Serialize dữ liệu thành Avro binary.
    *   Đẩy xuống Kafka topic tương ứng.

## 3. JSON-RPC Strategy

Để tối ưu hóa chi phí (Compute Units) và tốc độ, hệ thống sử dụng tập hợp các phương thức RPC chọn lọc:

### Core Methods

| Method | Mục đích | Tại sao dùng? |
| :--- | :--- | :--- |
| **`eth_getBlockByNumber`** | Lấy thông tin Header của Block và danh sách Transactions đầy đủ. | Tham số `full_transactions=True` giúp lấy toàn bộ Tx trong 1 call, tránh gọi N lần `eth_getTransactionByHash`. |
| **`eth_getBlockReceipts`** | Lấy toàn bộ Receipts của một Block. | **QUAN TRỌNG:** Đây là phương thức mở rộng (non-standard nhưng phổ biến ở Alchemy/QuickNode/Infura). Nó thay thế cho việc gọi hàng nghìn `eth_getTransactionReceipt`. Giúp giảm request từ `O(N)` xuống `O(1)`. |
| **`eth_getCode`** | Lấy Bytecode của Smart Contract. | Để phân tích xem contract đó làm gì (ERC20/721/Proxy). |
| **`eth_call`** | Gọi hàm `view` của contract (name, symbol...). | Dùng để lấy metadata on-chain mà không tốn gas. |

### Optimization Techniques
*   **Batching:** Gộp nhiều call (`eth_getBlockByNumber` cho block 1000, 1001, 1002...) vào một HTTP Request duy nhất.
*   **Failover:** Tự động chuyển sang Provider khác nếu một node bị lỗi hoặc Rate Limit (429).

## 4. Provider Comparison & Selection

Việc chọn Provider ảnh hưởng trực tiếp đến chiến lược Ingestion (đặc biệt là hỗ trợ `eth_getBlockReceipts`).

| Tiêu chí | Infura (Free) | QuickNode (Free) | Alchemy (Free) | Kiến nghị |
| :--- | :--- | :--- | :--- | :--- |
| **Hỗ trợ `eth_getBlockReceipts`** | Không (Standard only) | Có (Add-on) | **Có (Native)** | Alchemy thắng tuyệt đối ở khoản này cho Ingestion. |
| **Giới hạn** | 100k reqs/day | 10M Credits | 300M CUs/month | Alchemy hào phóng nhất cho dev. |
| **Rate Limit** | Thấp (~3 RPS) | Trung bình | Cao (~30 RPS) | |
| **Batching Support** | Tốt | Tốt | Tốt | |

**Kết luận:** Với pipeline hiện tại, **Alchemy** là lựa chọn tối ưu nhất nhờ hỗ trợ native `eth_getBlockReceipts` và hạn ngạch miễn phí lớn.
