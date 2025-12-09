#  MIT License
#
#  Copyright (c) 2020 Evgeny Medvedev, evge.medvedev@gmail.com
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this copyright notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
#
#  Modified by: Dang Tien Cuong, 2025
#  Description of modifications: remove some item exporter type

from config.settings import settings
from ingestion.blockchainetl.exporters.console_exporter import ConsoleItemExporter
from ingestion.blockchainetl.exporters.multi_item_exporter import MultiItemExporter


def create_item_exporters(outputs):
    split_outputs = [output.strip() for output in outputs.split(",")] if outputs else ["console"]

    item_exporters = [create_item_exporter(output) for output in split_outputs]
    return MultiItemExporter(item_exporters)


def create_item_exporter(output):
    item_exporter_type = _determine_item_exporter_type(output)

    if item_exporter_type == ItemExporterType.CONSOLE:
        item_exporter = ConsoleItemExporter()
    elif item_exporter_type == ItemExporterType.KAFKA:
        from ingestion.blockchainetl.exporters.kafka_exporter import KafkaItemExporter

        # Lấy Kafka broker URL từ settings.kafka.output
        # settings.kafka.output có dạng "kafka/host:port"
        kafka_broker_url = None
        if settings.kafka.output and settings.kafka.output.startswith("kafka/"):
            kafka_broker_url = settings.kafka.output.split("/", 1)[1]

        if not kafka_broker_url:
            raise ValueError("Kafka broker URL is not configured in settings.kafka.output.")

        # Tạo mapping topic với prefix từ settings
        topic_prefix = settings.kafka.topic_prefix
        item_type_to_topic_mapping = {
            "block": f"{topic_prefix}blocks",
            "transaction": f"{topic_prefix}transactions",
            "log": f"{topic_prefix}logs",
            "token_transfer": f"{topic_prefix}token_transfers",
            "trace": f"{topic_prefix}traces",
            "contract": f"{topic_prefix}contracts",
            "token": f"{topic_prefix}tokens",
        }

        item_exporter = KafkaItemExporter(
            kafka_broker_url=kafka_broker_url,
            item_type_to_topic_mapping=item_type_to_topic_mapping,
        )

    else:
        raise ValueError("Unable to determine item exporter type for output " + output)

    return item_exporter


def _determine_item_exporter_type(output):
    if output is not None and output.startswith("kafka"):
        return ItemExporterType.KAFKA
    elif output is None or output == "console":
        return ItemExporterType.CONSOLE
    else:
        return ItemExporterType.UNKNOWN


class ItemExporterType:
    CONSOLE = "console"
    KAFKA = "kafka"
    UNKNOWN = "unknown"
