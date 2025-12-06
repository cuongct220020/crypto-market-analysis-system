import collections
import json
import logging

from confluent_kafka import Producer
from collectors.base.converters.composite_item_converter import CompositeItemConverter


class KafkaItemExporter:

    def __init__(self, output, item_type_to_topic_mapping, converters=()):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.converter = CompositeItemConverter(converters)
        self.connection_url = self.get_connection_url(output)

        # Optimize config for high throughput
        conf = {
            'bootstrap.servers': self.connection_url,
            'client.id': 'ethereum-etl-producer',

            # Tối ưu batching: Đợi tối đa 100ms hoặc gom đủ 64KB dữ liệu mới gửi 1 lần
            # Giúp giảm số lượng request mạng, tăng tốc độ cực nhanh.
            'linger.ms': 100,
            'batch.size': 65536,

            # Nén dữ liệu: Blockchain data text nén rất tốt (giảm 70-80% băng thông)
            'compression.type': 'lz4',

            # Số lượng tin nhắn tối đa trong hàng đợi cục bộ
            'queue.buffering.max.messages': 100000,
        }

        self.producer = Producer(conf)
        print(f"Initialized Confluent Kafka Producer connected to: {self.connection_url}")

    def get_connection_url(self, output):
        try:
            return output.split('/')[1]
        except (IndexError, AttributeError):
            # Fallback hoặc raise lỗi rõ ràng hơn
            raise Exception('Invalid kafka output param. Format required: "kafka/127.0.0.1:9092"')

    def open(self):
        pass

    def _delivery_report(self, err, msg):
        """
        Callback được gọi khi tin nhắn gửi thành công hoặc thất bại.
        Chạy trên thread phụ của librdkafka.
        """
        if err is not None:
            logging.error(f'Message delivery failed: {err}')
        else:
            # Uncomment dòng dưới nếu muốn debug từng tin (sẽ rất spam log)
            # logging.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
            pass

    def export_items(self, items):
        for item in items:
            self.export_item(item)

        # Gọi poll(0) để kích hoạt các callbacks (delivery reports) đang chờ
        # Giúp giải phóng bộ nhớ của queue cục bộ
        self.producer.poll(0)

    def export_item(self, item):
        item_type = item.get('type')
        if item_type is not None and item_type in self.item_type_to_topic_mapping:
            topic = self.item_type_to_topic_mapping[item_type]

            # Serialize JSON
            # Note: Có thể dùng orjson để nhanh hơn nữa nếu cần: orjson.dumps(item)
            try:
                value = json.dumps(item).encode('utf-8')
            except TypeError as e:
                logging.error(f"Serialization error for item {item}: {e}")
                return

            # Gửi bất đồng bộ (Asynchronous)
            try:
                self.producer.produce(
                    topic,
                    value=value,
                    on_delivery=self._delivery_report
                )
            except BufferError:
                # Nếu hàng đợi đầy (Kafka chậm/đứt mạng), chờ 1 giây để giải phóng bớt rồi thử lại
                logging.warning("Local producer queue is full (BufferError). Waiting...")
                self.producer.poll(1)
                self.producer.produce(
                    topic,
                    value=value,
                    on_delivery=self._delivery_report
                )
        else:
            logging.warning(f'Topic for item type "{item_type}" is not configured.')

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
        # CỰC KỲ QUAN TRỌNG:
        # Flush đảm bảo tất cả tin nhắn còn trong bộ nhớ đệm được đẩy đi trước khi tắt app
        logging.info("Flushing Kafka producer...")
        self.producer.flush(timeout=10)
        logging.info("Kafka producer closed.")


def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get('type')].append(item)
    return result