from typing import Any, Dict, List, Union

from pydantic import BaseModel

from config.configs import configs
from ingestion.kafka_producer import KafkaProducerWrapper
from utils.logger_utils import get_logger

logger = get_logger("Kafka Item Exporter")


class KafkaItemExporter:

    def __init__(self, kafka_broker_url: str, item_type_to_topic_mapping: Dict[str, str]):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping

        # Initialize the shared Kafka Producer Wrapper
        self.producer_wrapper = KafkaProducerWrapper(
            kafka_broker_url=kafka_broker_url, schema_registry_url=configs.kafka.schema_registry_url
        )

    def open(self) -> None:
        pass

    def export_items(self, items: List[Union[Dict[str, Any], BaseModel]]) -> None:
        for item in items:
            self.export_item(item)
        # Poll is handled internally by the wrapper's underlying producer usually,
        # but wrapper.producer is exposed or we can add a poll method.
        # For now, we can access the underlying producer for polling if needed,
        # or rely on produce's async nature and final flush.
        self.producer_wrapper.producer.poll(0)

    def export_item(self, item: Union[Dict[str, Any], BaseModel]) -> None:
        # Determine item type to find topic and schema
        if isinstance(item, BaseModel):
            item_type = getattr(item, "type", None)  # Pydantic model should have this field
            # If not in attribute, try to dump
            if not item_type:
                item_dict = item.model_dump()
                item_type = item_dict.get("type")
        else:
            item_type = item.get("type")

        if item_type is None:
            logger.error(f"Cannot export item: 'type' field is missing. Item: {item}")
            return

        if item_type in self.item_type_to_topic_mapping:
            topic = self.item_type_to_topic_mapping[item_type]
            
            # Use the wrapper to produce
            # The schema_key is typically the item_type (e.g. 'block', 'transaction')
            logger.debug(f"Exporting item of type '{item_type}' to topic '{topic}'")
            self.producer_wrapper.produce(topic=topic, value=item, schema_key=item_type)
        else:
            logger.warning(f'Topic for item type "{item_type}" is not configured in item_type_to_topic_mapping.')

    def close(self) -> None:
        logger.info("Flushing Kafka producer...")
        self.producer_wrapper.flush(timeout=configs.kafka.producer_flush_timeout_seconds)
        logger.info("Kafka producer closed.")
