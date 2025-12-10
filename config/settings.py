from typing import Optional

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseModel):
    """General application settings."""

    name: str = Field("Crypto Market Analysis System", validation_alias="APP_NAME")
    debug: bool = Field(False, validation_alias="DEBUG")
    log_level: str = Field("INFO", validation_alias="LOG_LEVEL")


class EthereumSettings(BaseModel):
    """Settings related to Ethereum Node connection and RPC."""

    provider_uri: str = Field(
        default="https://eth-mainnet.g.alchemy.com/v2/demo",
        validation_alias="PROVIDER_URI",
        description="Ethereum Node JSON-RPC URL",
    )
    # Batch size for RPC requests (e.g., for ExportBlocksJob)
    batch_size: int = Field(default=100, gt=0, validation_alias="BATCH_SIZE")
    # Max worker threads for RPC calls
    max_workers: int = Field(default=5, gt=0, validation_alias="MAX_WORKERS")
    # Timeout for RPC calls (seconds)
    rpc_timeout: int = Field(default=60, gt=0, validation_alias="RPC_TIMEOUT")


class KafkaSettings(BaseModel):
    """Settings for Apache Kafka Producer and connection."""

    output: Optional[str] = Field(
        default=None,
        validation_alias="KAFKA_OUTPUT",
        description="Kafka output config string, e.g. kafka/localhost:9092",
    )
    topic_prefix: str = Field("crypto_analysis_", validation_alias="KAFKA_TOPIC_PREFIX")

    # Producer specific settings
    producer_linger_ms: int = Field(default=100, ge=0, validation_alias="KAFKA_PRODUCER_LINGER_MS")
    producer_batch_size_bytes: int = Field(default=65536, gt=0, validation_alias="KAFKA_PRODUCER_BATCH_SIZE_BYTES")
    producer_compression_type: str = Field(default="lz4", validation_alias="KAFKA_PRODUCER_COMPRESSION_TYPE")
    producer_queue_buffering_max_messages: int = Field(
        default=100000, gt=0, validation_alias="KAFKA_PRODUCER_QUEUE_BUFFERING_MAX_MESSAGES"
    )
    producer_flush_timeout_seconds: int = Field(
        default=10, gt=0, validation_alias="KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS"
    )

    # Schema Registry
    schema_registry_url: str = Field(
        default="http://localhost:8081",
        validation_alias="SCHEMA_REGISTRY_URL",
        description="URL of the Schema Registry",
    )


class StreamerSettings(BaseModel):
    """Settings specifically for the Streaming CLI/Service."""

    period_seconds: int = Field(default=10, gt=0, validation_alias="STREAMER_PERIOD_SECONDS")
    block_batch_size: int = Field(default=10, gt=0, validation_alias="STREAMER_BLOCK_BATCH_SIZE")
    retry_errors: bool = Field(default=True, validation_alias="STREAMER_RETRY_ERRORS")


class StorageSettings(BaseModel):
    """Settings for downstream storage (ClickHouse, Postgres, etc.)."""

    clickhouse_url: Optional[str] = Field(None, validation_alias="CLICKHOUSE_URL")


class Settings(BaseSettings):
    """
    Main Settings class that composes all sub-settings.
    Uses validation_alias in sub-models to map legacy flat env vars to nested structure.
    """

    app: AppSettings = Field(default_factory=AppSettings)
    ethereum: EthereumSettings = Field(default_factory=EthereumSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    streamer: StreamerSettings = Field(default_factory=StreamerSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)

    # Config to load from .env file
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


# Singleton instance
settings = Settings()
