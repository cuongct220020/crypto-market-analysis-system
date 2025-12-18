import orjson
import os
from typing import Any, Dict, Optional, Union

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import BaseModel

from config.configs import configs
from utils.logger_utils import get_logger

logger = get_logger("Kafka Producer")


class KafkaProducerWrapper:
    """
    A generic wrapper around confluent_kafka.Producer that handles:
    1. Multi-Broker Connection (tránh Leader Skew)
    2. Schema Registry integration (Avro serialization)
    3. Automatic schema loading
    4. Fallback to JSON serialization
    """

    def __init__(
            self,
            kafka_broker_url: str,
            schema_registry_url: Optional[str] = None
    ):
        # Parse multi-broker URLs
        self.kafka_broker_url = self._parse_broker_urls(kafka_broker_url)
        self.schema_registry_url = schema_registry_url or configs.kafka.schema_registry_url

        # 1. Config Schema Registry Client
        self.schema_registry_client = None
        if self.schema_registry_url:
            self.schema_registry_client = SchemaRegistryClient({"url": self.schema_registry_url})
            logger.info(f"Connected to Schema Registry at: {self.schema_registry_url}")
        else:
            logger.warning("Schema Registry URL not provided. Avro serialization will be disabled.")

        # 2. Load Schemas and Create Serializers
        self.serializers: Dict[str, AvroSerializer] = {}
        self._load_all_schemas()

        # 3. Kafka Producer Config với Multi-Broker
        conf = {
            # QUAN TRỌNG: Liệt kê tất cả brokers để Producer tự cân bằng
            "bootstrap.servers": self.kafka_broker_url,
            "client.id": configs.app.name.replace(" ", "-").lower() + "-kafka-producer",

            # Performance Tuning
            "linger.ms": configs.kafka.producer_linger_ms,
            "batch.size": configs.kafka.producer_batch_size_bytes,
            "compression.type": configs.kafka.producer_compression_type,
            "queue.buffering.max.messages": configs.kafka.producer_queue_buffering_max_messages,

            # Reliability & Idempotence
            "acks": configs.kafka.producer_acks,
            "enable.idempotence": configs.kafka.producer_enable_idempotence,  # Tránh duplicate messages
            "max.in.flight.requests.per.connection": configs.kafka.producer_max_request_in_flight,

            # Retry & Timeout
            "retries": configs.kafka.producer_retries,
            "retry.backoff.ms": configs.kafka.producer_retry_errors_backoff,
            "request.timeout.ms": configs.kafka.producer_request_timeout,

            # Partitioner: Sử dụng murmur2 (mặc định) để phân tán đều
            "partitioner": configs.kafka.topic_partitioner
        }

        self.producer = Producer(conf)
        logger.info(f"Initialized Confluent Kafka Producer connected to: {self.kafka_broker_url}")

    @staticmethod
    def _parse_broker_urls(broker_url: str) -> str:
        """
        Parse broker URLs và validate format.
        Input: "localhost:9095,localhost:9096,localhost:9097"
        Output: "localhost:9095,localhost:9096,localhost:9097"
        """
        # Remove 'kafka/' prefix if exists
        if broker_url.startswith("kafka/"):
            broker_url = broker_url[6:]

        brokers = [b.strip() for b in broker_url.split(",")]
        logger.info(f"Connecting to {len(brokers)} Kafka brokers: {brokers}")

        return ",".join(brokers)

    def _load_all_schemas(self, schema_dir: str = "ingestion/schemas") -> None:
        """
        Scans the schema directory and loads all .avsc files.
        """
        base_path = os.getcwd()
        full_schema_dir = os.path.join(base_path, schema_dir)

        if not os.path.exists(full_schema_dir):
            logger.warning(f"Schema directory not found: {full_schema_dir}")
            return

        if not self.schema_registry_client:
            return

        for filename in os.listdir(full_schema_dir):
            if filename.endswith(".avsc"):
                schema_key = filename[:-5]  # remove .avsc
                schema_path = os.path.join(full_schema_dir, filename)
                try:
                    with open(schema_path, "r") as f:
                        schema_str = f.read()

                    serializer = AvroSerializer(
                        self.schema_registry_client,
                        schema_str,
                        conf={"auto.register.schemas": True},
                    )
                    self.serializers[schema_key] = serializer
                    logger.debug(f"Loaded Avro serializer for schema '{schema_key}'")
                except Exception as e:
                    logger.error(f"Failed to load schema from {filename}: {e}")

    @staticmethod
    def _delivery_report(err: Any, msg: Any = None) -> None:
        """Callback for message delivery success/failure."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")

    def _produce_with_backpressure(self, topic: str, value: Any, key: Optional[Union[str, bytes]]) -> None:
        """
        Helper to produce with retry logic on BufferError (Queue Full).
        """
        while True:
            try:
                self.producer.produce(topic, value=value, key=key, on_delivery=self._delivery_report)
                # Poll immediately để xử lý callbacks và giải phóng queue
                self.producer.poll(0)
                break
            except BufferError:
                # Queue đầy, đợi để queue drain
                logger.warning(f"Local Kafka queue full. Waiting...")
                self.producer.poll(0.5)
            except Exception as e:
                logger.error(f"Kafka produce error: {e}")
                break

    def produce(
            self,
            topic: str,
            value: Union[Dict[str, Any], BaseModel],
            schema_key: Optional[str] = None,
            key: Optional[Union[str, bytes]] = None,
    ) -> None:
        """
        Publishes a message to Kafka với partitioning strategy.

        Args:
            topic: Target Kafka topic
            value: Data to send (Dict or Pydantic Model)
            schema_key: Key to lookup Avro serializer (e.g., 'block', 'transaction')
            key: Message key (IMPORTANT: use for partitioning)
        """
        # Convert Pydantic model to dict
        item_dict: Dict[str, Any]
        if isinstance(value, BaseModel):
            item_dict = value.model_dump(by_alias=True, exclude_none=True)
        else:
            item_dict = value

        # Try Avro Serialization first
        if schema_key and schema_key in self.serializers:
            try:
                serializer = self.serializers[schema_key]
                serialized_value = serializer(item_dict, SerializationContext(topic, MessageField.VALUE))
                self._produce_with_backpressure(topic, serialized_value, key)
                return
            except Exception as e:
                logger.error(f"Avro serialization error for schema '{schema_key}': {e}. Falling back to JSON.")

        # Fallback to JSON
        try:
            json_value = orjson.dumps(item_dict)
            self._produce_with_backpressure(topic, json_value, key)
        except Exception as e:
            logger.error(f"JSON serialization error: {e}. Data sample: {str(item_dict)[:100]}")

    def flush(self, timeout: float = 10.0) -> None:
        """Flush all buffered messages."""
        logger.info(f"Flushing Kafka producer (timeout={timeout}s)...")
        remaining = self.producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning(f"{remaining} messages were not delivered within {timeout}s")
        else:
            logger.info("All messages successfully delivered")

    def close(self) -> None:
        """Close the Kafka producer connection."""
        logger.info("Closing Kafka producer...")
        self.producer.flush()
        logger.info("Kafka producer closed.")