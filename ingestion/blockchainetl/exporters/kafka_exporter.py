import json
import os
from typing import Any, Dict, List, Union

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from pydantic import BaseModel

from config.settings import settings
from utils.logger_utils import get_logger

logger = get_logger(__name__)


class KafkaItemExporter:

    def __init__(self, kafka_broker_url: str, item_type_to_topic_mapping: Dict[str, str]):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.kafka_broker_url = kafka_broker_url

        # 1. Config Schema Registry Client
        schema_registry_conf = {"url": settings.kafka.schema_registry_url}
        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        # 2. Load Schemas and Create Serializers
        self.serializers = {}
        self._load_serializers()

        # 3. Kafka Producer Config
        conf = {
            "bootstrap.servers": self.kafka_broker_url,
            "client.id": settings.app.name.replace(" ", "-").lower() + "-kafka-producer",
            "linger.ms": settings.kafka.producer_linger_ms,
            "batch.size": settings.kafka.producer_batch_size_bytes,
            "compression.type": settings.kafka.producer_compression_type,
            "queue.buffering.max.messages": settings.kafka.producer_queue_buffering_max_messages,
        }

        self.producer = Producer(conf)
        logger.info(f"Initialized Confluent Kafka Producer connected to: {self.kafka_broker_url}")

    def _load_serializers(self) -> None:
        """
        Loads .avsc files from 'ingestion/schemas/' and creates AvroSerializer instances.
        """
        # Mapping from item "type" field to schema filename
        schema_mapping = {
            "block": "block.avsc",
            "transaction": "transaction.avsc",
            "log": "log.avsc",
            "token_transfer": "token_transfer.avsc",
            "trace": "trace.avsc",
            "contract": "contract.avsc",
            "token": "token.avsc",
        }

        # Path to schemas directory relative to project root
        base_path = os.getcwd()  # Assumes running from project root
        schema_dir = os.path.join(base_path, "ingestion", "schemas")

        for item_type, schema_file in schema_mapping.items():
            schema_path = os.path.join(schema_dir, schema_file)
            if os.path.exists(schema_path):
                try:
                    with open(schema_path, "r") as f:
                        schema_str = f.read()

                    serializer = AvroSerializer(
                        self.schema_registry_client,
                        schema_str,
                        conf={"auto.register.schemas": True},
                    )
                    self.serializers[item_type] = serializer
                    logger.info(f"Loaded Avro serializer for type '{item_type}' from {schema_path}")
                except Exception as e:
                    logger.error(f"Failed to load schema for {item_type}: {e}")
            else:
                logger.warning(f"Schema file not found for type '{item_type}' at {schema_path}. Proceeding without Avro serialization for this type.")

    def open(self) -> None:
        pass

    @staticmethod
    def _delivery_report(err: Any, msg: Any = None) -> None:
        """
        Callback for message delivery success/failure.
        """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            # logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
            pass

    def export_items(self, items: List[Union[Dict[str, Any], BaseModel]]) -> None:
        for item in items:
            self.export_item(item)
        self.producer.poll(0)

    def export_item(self, item: Union[Dict[str, Any], BaseModel]) -> None:
        # Convert Pydantic model to dict if necessary
        item_dict: Dict[str, Any]
        if isinstance(item, BaseModel):
            item_dict = item.model_dump(by_alias=True, exclude_none=True) # Use by_alias to match Avro schema names if models use Field(alias=...)
        else:
            item_dict = item

        item_type = item_dict.get("type")
        
        if item_type is None:
            logger.error(f"Cannot export item: 'type' field is missing. Item: {item_dict}")
            return

        if item_type in self.item_type_to_topic_mapping:
            topic = self.item_type_to_topic_mapping[item_type]

            # 1. Try Avro Serialization
            serializer = self.serializers.get(item_type)

            if serializer:
                try:
                    # Serialize to Avro (returns bytes)
                    value = serializer(item_dict, SerializationContext(topic, MessageField.VALUE))

                    self.producer.produce(topic, value=value, on_delivery=self._delivery_report)
                    # logger.info(f"Produced {topic} with key {item_dict.get('hash') or item_dict.get('id') or 'N/A'} successfully!")
                    return  # Success
                except Exception as e:
                    logger.error(f"Avro serialization error for item type '{item_type}': {e}. Item: {item_dict}. Falling back to JSON.")
                    # Fallback to JSON if Avro serialization fails
            else:
                 logger.warning(f"No Avro schema found for item type '{item_type}'. Falling back to JSON.")


            # 2. Fallback to JSON (if no schema or Avro failed)
            try:
                value = json.dumps(item_dict).encode("utf-8")
                self.producer.produce(topic, value=value, on_delivery=self._delivery_report)
            except Exception as e:
                logger.error(f"JSON serialization error for item {item_dict}: {e}")

        else:
            logger.warning(f'Topic for item type "{item_type}" is not configured in item_type_to_topic_mapping. Item: {item_dict}')

    def close(self) -> None:
        logger.info("Flushing Kafka producer...")
        self.producer.flush(timeout=settings.kafka.producer_flush_timeout_seconds)
        logger.info("Kafka producer closed.")