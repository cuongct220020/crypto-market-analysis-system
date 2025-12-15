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


class AppConfigs:
    def __init__(self):
        self.name = get_env("APP_NAME", "Crypto Market Analysis System")
        self.debug = get_env("DEBUG", False, bool)
        self.log_level = get_env("LOG_LEVEL", "INFO")


class EthereumStreamingConfigs:
    def __init__(self):
        self.rpc_provider_uris = get_env(
            "RPC_PROVIDER_URIS", "https://eth-mainnet.g.alchemy.com/v2/demo"
        )
        self.rpc_batch_request_size = get_env("RPC_BATCH_REQUEST_SIZE", 100, int)
        self.rpc_request_rate_sleep = get_env("RPC_REQUEST_RATE_SLEEP", 1.5, float)
        self.streamer_chunk_size = get_env("STREAMER_CHUNK_SIZE", 1000, int)
        self.streamer_queue_size = get_env("STREAMER_QUEUE_SIZE", 5, int)
        self.streamer_num_worker_process = get_env("STREAMER_NUM_WORKER_PROCESS", 5, int)
        self.streamer_period_seconds = get_env("STREAMER_PERIOD_SECONDS", 10, int)
        self.streamer_block_batch_size = get_env("STREAMER_BLOCK_BATCH_SIZE", 10, int)
        self.streamer_retry_errors = get_env("STREAMER_RETRY_ERRORS", True, bool)
        self.streamer_entity_types = get_env("STREAMER_ENTITY_TYPES", "block,transaction", list)


class KafkaConfigs:
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


class SystemConfigs:
    def __init__(self):
        self.app = AppConfigs()
        self.ethereum = EthereumStreamingConfigs()
        self.kafka = KafkaConfigs()

# Singleton instance
configs = SystemConfigs()