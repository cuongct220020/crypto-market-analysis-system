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
    1. Schema Registry integration (Avro serialization).
    2. Automatic schema loading from a specified directory.
    3. Fallback to JSON serialization.
    4. Singleton-like behavior (optional, but good practice if managed centrally).
    """

    def __init__(self, kafka_broker_url: str, schema_registry_url: Optional[str] = None):
        self.kafka_broker_url = kafka_broker_url
        self.schema_registry_url = schema_registry_url or configs.kafka.schema_registry_url

        # 1. Config Schema Registry Client
        self.schema_registry_client = None
        if self.schema_registry_url:
            self.schema_registry_client = SchemaRegistryClient({"url": self.schema_registry_url})
            logger.info(f"Connected to Schema Registry at: {self.schema_registry_url}")
        else:
            logger.warning("Schema Registry URL not provided. Avro serialization will be disabled.")

        # 2. Load Schemas and Create Serializers
        # Dictionary mapping schema_name (e.g., 'block', 'market_data') -> AvroSerializer
        self.serializers: Dict[str, AvroSerializer] = {}
        # We load schemas lazily or upfront. Let's load standard schemas upfront if possible,
        # or we can allow users to register schemas.
        # For this refactor, let's keep the logic of loading from a directory but make it a method.
        self._load_all_schemas()

        # 3. Kafka Producer Config
        conf = {
            "bootstrap.servers": self.kafka_broker_url,
            "client.id": configs.app.name.replace(" ", "-").lower() + "-kafka-producer",
            "linger.ms": configs.kafka.producer_linger_ms,
            "batch.size": configs.kafka.producer_batch_size_bytes,
            "compression.type": configs.kafka.producer_compression_type,
            "queue.buffering.max.messages": configs.kafka.producer_queue_buffering_max_messages,
        }

        self.producer = Producer(conf)
        logger.info(f"Initialized Confluent Kafka Producer connected to: {self.kafka_broker_url}")

    def _load_all_schemas(self, schema_dir: str = "ingestion/schemas") -> None:
        """
        Scans the schema directory and loads all .avsc files.
        The filename (without extension) is used as the schema key.
        e.g., 'block.avsc' -> key 'block'
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
        """
        Callback for message delivery success/failure.
        """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

    def produce(
        self,
        topic: str,
        value: Union[Dict[str, Any], BaseModel],
        schema_key: Optional[str] = None,
        key: Optional[str] = None,
    ) -> None:
        """
        Publishes a message to Kafka.

        Args:
            topic: The target Kafka topic.
            value: The data to send (Dict or Pydantic Model).
            schema_key: The key to look up the Avro serializer (e.g., 'block', 'transaction').
                        If None or not found, falls back to JSON.
            key: Optional message key (for partitioning).
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
                self.producer.produce(topic, value=serialized_value, key=key, on_delivery=self._delivery_report)
                logger.debug(f"Produced message to {topic} using Avro schema '{schema_key}'")
                return
            except Exception as e:
                logger.error(f"Avro serialization error for schema '{schema_key}': {e}. Falling back to JSON. Data sample: {str(item_dict)[:100]}...")

        # Fallback to JSON
        try:
            json_value = orjson.dumps(item_dict)
            self.producer.produce(topic, value=json_value, key=key, on_delivery=self._delivery_report)
            if schema_key:
                logger.warning(f"Produced message to {topic} using JSON fallback (Schema '{schema_key}' missing or failed).")
        except Exception as e:
            logger.error(f"JSON serialization error: {e}. Data: {item_dict}")

    def flush(self, timeout: float = 10.0) -> None:
        self.producer.flush(timeout=timeout)
