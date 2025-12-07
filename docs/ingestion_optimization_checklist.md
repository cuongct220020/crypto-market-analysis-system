# Checklist Tối Ưu Hóa & Cải Thiện Hệ Thống Ingestion

Tài liệu này theo dõi tiến độ cải thiện, tối ưu hóa và làm sạch source code cho module Ingestion (lấy dữ liệu on-chain vào Kafka), dựa trên kiến trúc Lambda và codebase `ethereum-etl`.

## Giai đoạn 1: Code Hygiene & Modernization (Làm sạch và Hiện đại hóa Code)

Mục tiêu: Giúp code dễ đọc, dễ debug và giảm thiểu lỗi tiềm ẩn do kiểu dữ liệu.

- [ ] **1. Áp dụng Type Hinting (Quan trọng)**
    - *Hiện trạng:* Code thiếu type checking (ví dụ: `def convert_item(self, item):`).
    - *Hành động:* Thêm type hints cho toàn bộ các hàm và method. Sử dụng thư viện `typing` (`List`, `Dict`, `Optional`, `Any`).
    - *Lợi ích:* IDE sẽ hỗ trợ tốt hơn, giảm lỗi runtime.
    - *Tool:* Cài đặt `mypy` để kiểm tra static type.

- [ ] **2. Chuyển đổi Data Models sang Pydantic hoặc Dataclasses**
    - *Hiện trạng:* Các class như `EthBlock`, `EthTransaction` (trong `ingestion/ethereumetl/domain/`) đang là các class Python thuần túy với nhiều thuộc tính `None`. Mapping thủ công trong `mappers/` rất dài dòng.
    - *Hành động:* Chuyển các class domain này thành **Pydantic Models** (v2) hoặc Python **Dataclasses**.
    - *Lợi ích:* Tự động validate dữ liệu, serialization/deserialization nhanh hơn (đặc biệt Pydantic v2 viết bằng Rust), code gọn hơn.

- [ ] **3. Chuẩn hóa Logging**
    - *Hiện trạng:* Một số file vẫn dùng `print()`. `KafkaItemExporter` dùng `logging` nhưng mix `print`.
    - *Hành động:* Thay thế toàn bộ `print()` bằng `logger` cấu hình từ `utils.logger_utils`. Xem xét sử dụng *structured logging* (ví dụ thư viện `structlog`).

- [ ] **4. Sử dụng Black & Ruff**
    - *Hành động:* Thiết lập `pre-commit hook` chạy `ruff` (linter) và `black` (formatter) để đảm bảo style code nhất quán.

## Giai đoạn 2: Performance Optimization (Tối ưu hiệu năng)

Mục tiêu: Tăng tốc độ ingestion và xử lý dữ liệu, giảm độ trễ cho luồng Real-time.

- [ ] **5. Tối ưu Serialization (JSON)**
    - *Hiện trạng:* `KafkaItemExporter` và các exporter khác đang dùng thư viện `json` chuẩn của Python.
    - *Hành động:* Thay thế bằng **`orjson`** trong `ingestion/blockchainetl/jobs/kafka_exporter.py`.
    - *Lợi ích:* `orjson` nhanh hơn `json` chuẩn từ 10-20 lần và trả về `bytes` trực tiếp, giảm overhead CPU.

- [ ] **6. Cấu hình Kafka Producer**
    - *Hiện trạng:* Các cấu hình `linger.ms`, `batch.size` đang hardcode trong code.
    - *Hành động:*
        - Đưa cấu hình ra file config (`config/settings.py` hoặc `.env`).
        - Tăng `queue.buffering.max.messages` và `queue.buffering.max.kbytes` nếu RAM cho phép.
        - Tối ưu callback `on_delivery`: Chỉ log lỗi, tránh log level `DEBUG` cho từng message.

- [ ] **7. Web3 Provider & Batching**
    - *Hiện trạng:* Đang dùng `BatchHTTPProvider` tự implement.
    - *Hành động:* Kiểm tra `web3.py` v6+ hỗ trợ `AsyncHTTPProvider` hoặc batching native. Chuyển sang **AsyncIO** cho các tác vụ I/O bound nếu có thể để giảm overhead context switching so với Multithreading.

## Giai đoạn 3: Architecture & Reliability (Kiến trúc & Độ tin cậy)

Mục tiêu: Hệ thống ổn định, dễ cấu hình và mở rộng.

- [ ] **8. Configuration Management**
    - *Hiện trạng:* Các tham số nằm rải rác trong CLI options và code logic.
    - *Hành động:* Sử dụng **`pydantic-settings`** để tạo class `Settings` load từ biến môi trường (`.env`).

- [ ] **9. Dependency Injection**
    - *Hiện trạng:* Code khởi tạo trực tiếp dependency bên trong `__init__`.
    - *Hành động:* Áp dụng Dependency Injection. Ví dụ: `EthStreamerAdapter` nhận `item_exporter` từ bên ngoài thay vì tự tạo.

- [ ] **10. Error Handling & Retries**
    - *Hiện trạng:* Đã có `RetriableValueError` và retry cơ bản.
    - *Hành động:*
        - Bổ sung **Dead Letter Queue (DLQ)** cho Kafka.
        - Cải thiện `BatchWorkExecutor`: Xem xét circuit breaker pattern khi gặp lỗi rate limit từ RPC.

## Giai đoạn 4: Mở rộng Logic cho Crypto Analysis

- [ ] **11. Triển khai Web2 API Connectors**
    - *Hành động:* Hoàn thiện `ingestion/web2_api/` (CoinGecko, DefiLlama).
    - *Lưu ý:* Implement **Rate Limiting** (dùng `tenacity` hoặc `ratelimit`) và **Caching**.

- [ ] **12. Streaming Enrichment (Làm giàu dữ liệu)**
    - *Hiện trạng:* Join in-memory trong Python (`EthStreamerAdapter`) có nguy cơ OOM.
    - *Hành động:* Chuyển phần join nặng sang **Spark Structured Streaming** hoặc **ClickHouse**. Python Ingestion chỉ làm nhiệm vụ "Move data".
