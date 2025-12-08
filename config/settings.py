from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- App Settings ---
    app_name: str = "Crypto Market Analysis System"
    debug: bool = False
    log_level: str = "INFO"

    # --- Ethereum Node Settings ---
    # URL của Full Node (Alchemy, Infura, or Local node)
    # Dùng default là chuỗi rỗng hoặc None nếu muốn bắt buộc phải set trong .env
    # Ở đây ta để Optional để tránh crash khi chạy test/CI không có .env thực
    provider_uri: str = Field(
        default="https://eth-mainnet.g.alchemy.com/v2/demo", description="Ethereum Node JSON-RPC URL"
    )

    # Batch size cho RPC requests (cho ExportBlocksJob, ExportReceiptsJob...)
    batch_size: int = Field(default=100, gt=0)

    # Số lượng worker threads tối đa
    max_workers: int = Field(default=5, gt=0)

    # Timeout cho RPC calls (giây)
    rpc_timeout: int = Field(default=60, gt=0)

    # --- Kafka Settings ---
    # Địa chỉ Kafka Broker (ví dụ: localhost:9092)
    kafka_output: Optional[str] = Field(
        default=None, description="Kafka output config string, e.g. kafka/localhost:9092"
    )

    # Prefix cho các topic (để tránh trùng lặp trên cluster chung)
    kafka_topic_prefix: str = "crypto_analysis_"

    # Kafka Producer specific settings (from ingestion_optimization_checklist.md)
    kafka_producer_linger_ms: int = Field(default=100, ge=0)
    kafka_producer_batch_size_bytes: int = Field(default=65536, gt=0)  # 64KB
    kafka_producer_compression_type: str = Field(default="lz4")  # none, gzip, snappy, lz4, zstd
    kafka_producer_queue_buffering_max_messages: int = Field(default=100000, gt=0)
    kafka_producer_flush_timeout_seconds: int = Field(default=10, gt=0)

    # --- Streamer Settings ---
    streamer_period_seconds: int = Field(default=10, gt=0)
    streamer_block_batch_size: int = Field(default=10, gt=0)
    streamer_retry_errors: bool = Field(default=True)

    # --- Storage Settings (Optional for now) ---
    clickhouse_url: Optional[str] = None
    postgres_url: Optional[str] = None

    # Cấu hình load từ file .env
    # extra='ignore' để bỏ qua các biến thừa trong .env mà không báo lỗi
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


# Khởi tạo settings instance (Singleton)
# Khi import settings từ file này, nó sẽ tự động load và validate config
settings = Settings()
