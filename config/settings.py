import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def get_env(key: str, default: any = None, cast_type: type = str):
    value = os.getenv(key)
    if value is None:
        return default

    if cast_type == bool:
        return value.lower() in ("true", "1", "t", "yes", "on")
    try:
        return cast_type(value)
    except (ValueError, TypeError):
        return default


class AppSettings:
    def __init__(self):
        self.name = get_env("APP_NAME", "Crypto Market Analysis System")
        self.debug = get_env("DEBUG", False, bool)
        self.log_level = get_env("LOG_LEVEL", "INFO")


class EthereumSettings:
    def __init__(self):
        self.provider_uri = get_env(
            "PROVIDER_URI", "https://eth-mainnet.g.alchemy.com/v2/demo"
        )
        self.batch_size = get_env("BATCH_SIZE", 100, int)
        self.max_workers = get_env("MAX_WORKERS", 5, int)
        self.max_concurrent_requests = get_env("ASYNC_RPC_MAX_CONCURRENCY", 5, int)
        self.rpc_timeout = get_env("RPC_TIMEOUT", 60, int)


class KafkaSettings:
    def __init__(self):
        self.output = get_env("KAFKA_OUTPUT")
        self.topic_prefix = get_env("KAFKA_TOPIC_PREFIX", "crypto_analysis_")
        self.producer_linger_ms = get_env("KAFKA_PRODUCER_LINGER_MS", 100, int)
        self.producer_batch_size_bytes = get_env(
            "KAFKA_PRODUCER_BATCH_SIZE_BYTES", 65536, int
        )
        self.producer_compression_type = get_env(
            "KAFKA_PRODUCER_COMPRESSION_TYPE", "lz4"
        )
        self.producer_queue_buffering_max_messages = get_env(
            "KAFKA_PRODUCER_QUEUE_BUFFERING_MAX_MESSAGES", 100000, int
        )
        self.producer_flush_timeout_seconds = get_env(
            "KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS", 10, int
        )
        self.schema_registry_url = get_env(
            "SCHEMA_REGISTRY_URL", "http://localhost:8881"
        )


class StreamerSettings:
    def __init__(self):
        self.period_seconds = get_env("STREAMER_PERIOD_SECONDS", 10, int)
        self.block_batch_size = get_env("STREAMER_BLOCK_BATCH_SIZE", 10, int)
        self.retry_errors = get_env("STREAMER_RETRY_ERRORS", True, bool)


class StorageSettings:
    def __init__(self):
        self.clickhouse_url = get_env("CLICKHOUSE_URL")


class Settings:
    def __init__(self):
        self.app = AppSettings()
        self.ethereum = EthereumSettings()
        self.kafka = KafkaSettings()
        self.streamer = StreamerSettings()
        self.storage = StorageSettings()


# Singleton instance
settings = Settings()