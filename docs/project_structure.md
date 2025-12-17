# Cấu Trúc Dự Án Crypto Market Analysis System

Tài liệu này mô tả chi tiết cấu trúc thư mục và mục đích của các thành phần trong dự án **Crypto Market Analysis System**.

## Tổng Quan
Dự án được tổ chức theo mô hình modular, tách biệt các thành phần:
- **Ingestion**: Thu thập dữ liệu từ Blockchain và Web2 APIs.
- **Processing**: Xử lý dữ liệu (Batch & Streaming) sử dụng Spark.
- **Storage**: Lưu trữ dữ liệu vào ClickHouse và Elasticsearch.
- **Orchestration**: Quản lý luồng công việc với Airflow.
- **Infrastructure**: Cấu hình Docker cho toàn bộ hệ thống.

## Cấu Trúc Chi Tiết

### Thư mục gốc `/`
Chứa các tệp cấu hình môi trường, quản lý phụ thuộc (dependencies), tài liệu hướng dẫn chung và điểm nhập (entry point) của ứng dụng.

### `abi/`
Lưu trữ Application Binary Interface (ABI) của các smart contracts, phục vụ việc giải mã dữ liệu giao dịch và tương tác với blockchain.

### `airflow/`
Chứa toàn bộ mã nguồn và cấu hình liên quan đến Apache Airflow.
*   `config/`: Các tệp cấu hình bổ trợ.
*   `dags/`: Định nghĩa các quy trình xử lý dữ liệu (Directed Acyclic Graphs).
*   `logs/`: Lưu trữ nhật ký hoạt động hệ thống.
*   `plugins/`: Các tiện ích mở rộng tuỳ chỉnh cho Airflow.

### `cli/`
Cung cấp giao diện dòng lệnh (Command Line Interface) để tương tác với hệ thống, bao gồm các lệnh khởi chạy pipeline streaming và các công cụ tiện ích khác.

### `infrastructure/`
Quản lý cấu hình hạ tầng dưới dạng mã (Infrastructure as Code) sử dụng Docker Compose, chia thành các cụm dịch vụ riêng biệt.
*   `airflow-cluster/`: Môi trường vận hành Airflow.
*   `clickhouse-cluster/`: Cụm cơ sở dữ liệu ClickHouse (OLAP).
*   `elastic-cluster/`: Hệ thống tìm kiếm và phân tích Elasticsearch/Kibana.
*   `kafka-cluster/`: Hệ thống message broker Apache Kafka.
*   `spark-cluster/`: Cụm xử lý dữ liệu phân tán Apache Spark.

### `config/`
Tập trung quản lý các cấu hình tĩnh và động của toàn bộ ứng dụng Python.

### `constants/`
Lưu trữ các giá trị hằng số, địa chỉ hợp đồng quan trọng và các tham số cố định đặc thù của mạng lưới Ethereum.

### `docs/`
Thư mục chứa tài liệu kỹ thuật, hướng dẫn phát triển và biểu đồ kiến trúc hệ thống.

### `ingestion/`
Module nòng cốt chịu trách nhiệm trích xuất (Extract) và tải (Load) dữ liệu từ các nguồn bên ngoài.

#### `ingestion/blockchainetl/`
Framework nền tảng cho quy trình ETL blockchain, được thiết kế generic để có thể mở rộng cho nhiều chuỗi khác nhau.
*   `converters/`: Chuyển đổi dữ liệu thô sang định dạng chuẩn.
*   `exporters/`: Xuất dữ liệu đã xử lý ra các hệ thống đích (Console, Kafka, File...).
*   `jobs/`: Quản lý logic thực thi các tác vụ ETL.
*   `streaming/`: Cung cấp các thành phần cốt lõi để duy trì luồng dữ liệu thời gian thực.

#### `ingestion/ethereumetl/`
Triển khai cụ thể các logic của framework `blockchainetl` dành riêng cho Ethereum.
*   `enums/`: Định nghĩa các kiểu liệt kê (Enumerations) dùng trong models.
*   `mappers/`: Chuyển đổi dữ liệu JSON-RPC thô thành các Domain Models.
*   `models/`: Định nghĩa các cấu trúc dữ liệu (Pydantic Models) đại diện cho thực thể blockchain (Block, Transaction, Log...).
*   `service/`: Chứa logic nghiệp vụ phức tạp để xử lý và làm giàu dữ liệu Ethereum.
*   `streaming/`: Tích hợp logic streaming đặc thù của Ethereum, kết nối Adapter với Framework.

#### `ingestion/schemas/`
Chứa các định nghĩa Avro Schema dùng để serialize dữ liệu trước khi đẩy vào Kafka.

#### `ingestion/web2/`
Module thu thập dữ liệu thị trường off-chain (giá, TVL, metadata) từ các API REST Web2 (CoinGecko, DefiLlama, Chainlink...).

### `notebooks/`
Không gian làm việc cho Jupyter Notebooks, phục vụ mục đích nghiên cứu dữ liệu, kiểm thử nhanh (POC) và phân tích thăm dò.

### `processing/`
Module xử lý dữ liệu trung tâm, thực hiện các phép biến đổi (Transform) phức tạp.
*   `batch/`: Các job xử lý dữ liệu định kỳ (Batch Processing).
*   `streaming/`: Các ứng dụng xử lý dữ liệu thời gian thực (Streaming Processing).

### `storage/`
Lớp truy cập dữ liệu (Data Access Layer), quản lý kết nối và thao tác với các hệ thống lưu trữ.
*   `clickhouse/`: Client, schemas và logic tương tác với ClickHouse.
*   `elasticsearch/`: Client và cấu hình mapping cho Elasticsearch.

### `tests/`
Chứa các bài kiểm thử tự động (Unit Tests) để đảm bảo chất lượng mã nguồn.

### `utils/`
Bộ thư viện tiện ích dùng chung cho toàn bộ dự án (Logging, File I/O, Error Handling, Validation...).