# Cấu Trúc Dự Án Crypto Market Analysis System

Tài liệu này mô tả chi tiết cấu trúc thư mục và mục đích của các tệp tin trong dự án **Crypto Market Analysis System**.

## Tổng Quan
Dự án được tổ chức theo mô hình modular, tách biệt các thành phần:
- **Ingestion**: Thu thập dữ liệu từ Blockchain và Web2 APIs.
- **Processing**: Xử lý dữ liệu (Batch & Streaming) sử dụng Spark.
- **Storage**: Lưu trữ dữ liệu vào ClickHouse và Elasticsearch.
- **Orchestration**: Quản lý luồng công việc với Airflow.
- **Infrastructure**: Cấu hình Docker cho toàn bộ hệ thống.

## Cấu Trúc Chi Tiết

### Thư mục gốc `/`
*   `.env.example`: Mẫu file cấu hình biến môi trường.
*   `.gitignore`: Các file/thư mục bị Git bỏ qua.
*   `docker-compose.yml`: File cấu hình Docker Compose chính để khởi chạy toàn bộ services ở mức ứng dụng.
*   `LICENSE`: Giấy phép sử dụng mã nguồn.
*   `README.md`: Tài liệu hướng dẫn chung của dự án.
*   `requirements.txt`: Danh sách các thư viện Python phụ thuộc.
*   `run.py`: Entry point của ứng dụng, sử dụng để chạy các lệnh CLI.

### `abi/`
Chứa Application Binary Interface (ABI) của các smart contracts, dùng để giải mã dữ liệu giao dịch.
*   `__init__.py`: Đánh dấu thư mục là package.
*   `erc20_abi.py`: Định nghĩa ABI chuẩn cho ERC20 tokens.

### `airflow/`
Cấu hình và mã nguồn cho Apache Airflow.
*   `config/`: Các file cấu hình bổ sung cho Airflow.
*   `dags/`: Chứa các Directed Acyclic Graphs (DAGs) định nghĩa luồng công việc.
    *   `first_dag.py`: Ví dụ DAG đầu tiên.
*   `logs/`: Thư mục chứa logs của Airflow.
*   `plugins/`: Các plugins mở rộng cho Airflow.

### `cli/`
Giao diện dòng lệnh (Command Line Interface) của ứng dụng.
*   `__init__.py`: Khởi tạo nhóm lệnh CLI chính (sử dụng `click`).
*   `streaming.py`: Định nghĩa lệnh `streaming` để chạy các tác vụ xử lý dữ liệu thời gian thực.

### `cluster-infra/`
Chứa cấu hình hạ tầng (Docker Compose) cho các cụm dịch vụ riêng biệt.
*   `airflow-cluster/`: Hạ tầng cho Airflow (Webserver, Scheduler, Worker, Redis, Postgres).
*   `clickhouse-cluster/`: Hạ tầng cho ClickHouse (Embedded Keeper Mode).
*   `elastic-cluster/`: Hạ tầng cho Elasticsearch và Kibana.
*   `kafka-cluster/`: Hạ tầng cho Apache Kafka (Kraft Mode).
*   `spark-cluster/`: Hạ tầng cho Apache Spark (Master và Workers).

### `config/`
Quản lý cấu hình tập trung của ứng dụng Python.
*   `constants.py`: Các hằng số dùng chung.
*   `settings.py`: Quản lý các biến môi trường và cài đặt cấu hình (DB URL, API Keys...).

### `constants/`
Chứa các dữ liệu hằng số đặc thù của domain Ethereum.
*   `mainnet_daofork_state_changes.py`: Cấu hình trạng thái liên quan đến DAO fork.
*   `mainnet_genesis_alloc.py`: Cấp phát token tại khối Genesis của Mainnet.

### `docs/`
Tài liệu dự án.
*   `project-structure.md`: File bạn đang đọc, mô tả cấu trúc dự án.

### `ingestion/`
Module quan trọng nhất, chịu trách nhiệm trích xuất (Extract) và tải (Load) dữ liệu.

#### `ingestion/blockchainetl/`
Framework lõi để xây dựng các quy trình ETL blockchain (thiết kế generic, không phụ thuộc cụ thể Ethereum).
*   `converters/`: Chuyển đổi dữ liệu.
    *   `composite_item_converter.py`: Xử lý chuyển đổi cho nhiều loại item khác nhau trong cùng một luồng.
*   `exporters/`: Xuất dữ liệu ra các đích đến (Sink).
    *   `console_item_exporter.py`: Xuất ra màn hình console (debug).
    *   `kafka_exporter.py`: Đẩy dữ liệu vào Kafka topic.
    *   `in_memory_item_exporter.py`: Lưu vào bộ nhớ (testing).
    *   `composite_item_exporter.py`: Kết hợp nhiều exporter.
*   `jobs/`: Quản lý công việc ETL.
    *   `base_job.py`: Lớp trừu tượng (Abstract Base Class) cho tất cả các job ETL.
*   `streaming/`: Logic nền tảng cho việc streaming.
    *   `streamer.py`: Quản lý vòng lặp streaming, xử lý block và đẩy dữ liệu.
    *   `streamer_adapter_stub.py`: Interface adapter mẫu.

#### `ingestion/ethereumetl/`
Triển khai cụ thể của `blockchainetl` dành cho Ethereum Blockchain.

*   **`domain/`**: Định nghĩa các Object Model (POJO) đại diện cho dữ liệu Ethereum.
    *   `block.py`, `transaction.py`: Đại diện Block và Transaction.
    *   `receipt.py`, `receipt_log.py`: Transaction Receipt và Log events.
    *   `token.py`, `token_transfer.py`: Thông tin Token và sự kiện chuyển Token.
    *   `trace.py`: Internal transaction (trace).
    *   `contract.py`: Thông tin Smart Contract.

*   **`enumeration/`**: Các định nghĩa Enum.
    *   `entity_type.py`: Liệt kê các loại thực thể (block, transaction, log, token_transfer...).

*   **`executors/`**: Quản lý việc thực thi đa luồng/đa tiến trình.
    *   `batch_work_executor.py`: Quản lý thực thi một lô công việc.
    *   `bounded_executor.py`: Giới hạn số lượng worker concurrent.
    *   `fail_safe_executor.py`: Đảm bảo lỗi ở một task không làm sập toàn bộ quy trình.

*   **`jobs/`**: Các Job cụ thể để trích xuất từng loại dữ liệu (thường dùng cho batch export).
    *   `export_blocks_job.py`: Lấy thông tin blocks và transactions.
    *   `export_receipts_job.py`: Lấy transaction receipts và logs.
    *   `extract_token_transfers_job.py`: Trích xuất token transfers từ logs.
    *   `extract_contracts_job.py`, `extract_tokens_job.py`: Trích xuất thông tin contract và token.

*   **`mappers/`**: Chuyển đổi dữ liệu thô (Dictionary từ RPC) sang Domain Object.
    *   `block_mapper.py`, `transaction_mapper.py`: Map dữ liệu block/tx.
    *   `receipt_mapper.py`, `receipt_log_mapper.py`: Map receipt/log.
    *   `token_transfer_mapper.py`: Map sự kiện transfer.

*   **`providers/`**: Kết nối tới mạng lưới Ethereum.
    *   `rpc.py`: Provider sử dụng giao thức JSON-RPC (HTTP/IPC).
    *   `auto.py`: Tự động phát hiện và cấu hình provider dựa trên URI.

*   **`service/`**: Chứa logic nghiệp vụ phức tạp.
    *   `eth_contract_service.py`: Xác định bytecode có phải là contract không.
    *   `token_metadata_service.py`: Các logic liên quan đến token (ví dụ: lấy decimals, symbol).
    *   `token_transfer_service.py`: Logic trích xuất sự kiện transfer từ log (giải mã topic).
    *   `trace_id_service.py`: Tính toán ID cho trace.
    *   `trace_status_service.py`: Tính toán status cho trace.
    *   `block_timestamp_service.py`: Chuyển đổi giữa block và timestamp (tích hợp thuật toán tìm kiếm trên đồ thị).

*   **`streaming/`**: Logic streaming chuyên biệt cho Ethereum.
    *   `eth_streamer_adapter.py`: Adapter kết nối `ethereumetl` với `blockchainetl.streamer`. Chịu trách nhiệm gọi RPC lấy block, gọi mapper, và trả về items.
    *   `enrich.py`: Bổ sung thông tin cho dữ liệu (join dữ liệu).
    *   `item_exporter_creator.py`: Factory tạo exporter dựa trên cấu hình.

*   `json_rpc_requests.py`: Helper để tạo các payload JSON-RPC (như `eth_getBlockByNumber`).

#### `ingestion/web2_api/`
Module thu thập dữ liệu thị trường off-chain từ các API Web2 (HTTP REST APIs).
*   `chain_link.py`: Lấy dữ liệu Oracle từ Chainlink.
*   `coin_gecko.py`: Client gọi API CoinGecko (giá, market cap, metadata).
*   `coin_market_cap.py`: Client gọi API CoinMarketCap.
*   `defillama.py`: Client gọi API DefiLlama (TVL, protocols).

### `notebooks/`
Jupyter Notebooks dùng để kiểm thử nhanh (POC) và nghiên cứu dữ liệu.
*   `test_spark_batch_processing.ipynb`: Test luồng xử lý batch với Spark.
*   `test_spark_streaming_processing.ipynb`: Test luồng xử lý streaming với Spark.

### `processing/`
Logic xử lý và chuyển đổi dữ liệu (ETL/ELT pipeline).
*   `batch/`: Xử lý lô (Batch Processing).
    *   `raw_to_silver.py`: Job Spark đọc dữ liệu thô, làm sạch, deduplicate và lưu bảng Silver.
*   `streaming/`: Xử lý thời gian thực (Streaming Processing).
    *   `inges_market_prices.py`: Job Spark Streaming ingest giá thị trường liên tục.

### `storage/`
Lớp giao tiếp (Data Access Layer) với các hệ thống lưu trữ.
*   `clickhouse/`: Client và schema cho ClickHouse.
    *   `client.py`: Wrapper cho ClickHouse driver.
    *   `schemas/`: Định nghĩa câu lệnh SQL `CREATE TABLE`.
*   `elasticsearch/`: Client và mapping cho Elasticsearch.
    *   `mappings/`: Định nghĩa JSON mapping cho index.

### `tests/`
*   `integration/`: Test tích hợp (cần môi trường thực hoặc docker).
*   `unit/`: Test đơn vị (mock dependencies).

### `utils/`
Các thư viện tiện ích dùng chung.
*   `atomic_counter.py`: Bộ đếm an toàn trong môi trường đa luồng.
*   `exceptions.py`: Định nghĩa các lỗi tùy chỉnh của ứng dụng.
*   `file_utils.py`: Đọc/ghi file, xử lý đường dẫn thông minh (hỗ trợ smart open).
*   `logger_utils.py`: Cấu hình logging chuẩn cho toàn bộ project.
*   `rpc_helpers.py`: Batching RPC requests.
*   `web3_utils.py`: Các hàm tiện ích khi làm việc với địa chỉ, hash của Web3.