import collections
import json
from typing import Any, Dict, List

from confluent_kafka import Producer

from config.settings import settings  # NEW: Import settings
from ingestion.blockchainetl.converters.composite_item_converter import CompositeItemConverter
from utils.logger_utils import get_logger  # Use project's logger

logger = get_logger(__name__)


class KafkaItemExporter:

    def __init__(self, kafka_broker_url: str, item_type_to_topic_mapping: Dict[str, str], converters=()):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.converter = CompositeItemConverter(converters)
        self.kafka_broker_url = kafka_broker_url  # Use direct broker URL

        # Optimize config for high throughput, using settings
        conf = {
            "bootstrap.servers": self.kafka_broker_url,
            "client.id": settings.app_name.replace(" ", "-").lower() + "-kafka-producer",  # Use app name for client ID
            # Tối ưu batching: Đợi tối đa X ms hoặc gom đủ Y KB dữ liệu mới gửi 1 lần
            "linger.ms": settings.kafka_producer_linger_ms,
            "batch.size": settings.kafka_producer_batch_size_bytes,
            # Nén dữ liệu
            "compression.type": settings.kafka_producer_compression_type,
            # Số lượng tin nhắn tối đa trong hàng đợi cục bộ
            "queue.buffering.max.messages": settings.kafka_producer_queue_buffering_max_messages,
        }

        self.producer = Producer(conf)
        logger.info(f"Initialized Confluent Kafka Producer connected to: {self.kafka_broker_url}")

    # REMOVED: get_connection_url is no longer needed

    def open(self):
        pass

    def _delivery_report(self, err, msg):
        """
        Callback được gọi khi tin nhắn gửi thành công hoặc thất bại.
        Chạy trên thread phụ của librdkafka.
        """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            # Uncomment dòng dưới nếu muốn debug từng tin (sẽ rất spam log)
            # logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
            pass

    def export_items(self, items: List[Dict[str, Any]]):
        for item in items:
            self.export_item(item)

        # Gọi poll(0) để kích hoạt các callbacks (delivery reports) đang chờ
        # Giúp giải phóng bộ nhớ của queue cục bộ
        self.producer.poll(0)

    def export_item(self, item: Dict[str, Any]):
        item_type = item.get("type")
        if item_type is not None and item_type in self.item_type_to_topic_mapping:
            topic = self.item_type_to_topic_mapping[item_type]

            # Serialize JSON
            try:
                # Note: Có thể dùng orjson để nhanh hơn nữa nếu cần: orjson.dumps(item)
                value = json.dumps(item).encode("utf-8")
            except TypeError as e:
                logger.error(f"Serialization error for item {item}: {e}")
                return

            # Gửi bất đồng bộ (Asynchronous)
            try:
                self.producer.produce(topic, value=value, on_delivery=self._delivery_report)
            except BufferError:
                logger.warning("Local producer queue is full (BufferError). Waiting...")
                self.producer.poll(1)  # Wait 1 second
                self.producer.produce(topic, value=value, on_delivery=self._delivery_report)  # Retry produce
        else:
            logger.warning(f'Topic for item type "{item_type}" is not configured.')

    def convert_items(self, items: List[Dict[str, Any]]):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
        # Flush đảm bảo tất cả tin nhắn còn trong bộ nhớ đệm được đẩy đi trước khi tắt app
        logger.info("Flushing Kafka producer...")
        self.producer.flush(timeout=settings.kafka_producer_flush_timeout_seconds)  # Use setting
        logger.info("Kafka producer closed.")


def group_by_item_type(items: List[Dict[str, Any]]):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get("type")].append(item)
    return result
