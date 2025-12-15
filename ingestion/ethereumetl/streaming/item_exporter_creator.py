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


from config.configs import configs
from ingestion.blockchainetl.exporters.console_item_exporter import ConsoleItemExporter
from ingestion.blockchainetl.exporters.multi_item_exporter import MultiItemExporter
from ingestion.blockchainetl.exporters.kafka_item_exporter import KafkaItemExporter
from ingestion.ethereumetl.enums.item_exporter_type import ItemExporterType


def create_topic_mapping(topic_prefix=None):
    # Convention: {prefix}{item_type}.v0
    # Example: crypto.raw.eth.blocks.v0
    prefix = topic_prefix if topic_prefix else configs.kafka.topic_prefix

    # Ensure prefix ends with a dot if not empty
    if prefix and not prefix.endswith("."):
        prefix += "."

    # Standardize keys to match EntityType or singular form used in code
    return {
        "block": f"{prefix}blocks.v0",
        "transaction": f"{prefix}transactions.v0",
        "receipt": f"{prefix}receipts.v0",
        "log": f"{prefix}logs.v0",
        "token_transfer": f"{prefix}token_transfers.v0",
        "contract": f"{prefix}contracts.v0"
    }

def create_item_exporter(output, entity_types=None, topic_prefix=None):
    if output is not None and output.startswith("kafka"):
        item_exporter_type = ItemExporterType.KAFKA
    elif output is not None and output != "console":
        # Relaxed check: If not console, assume it's a Kafka Broker URL (e.g., localhost:9092)
        item_exporter_type = ItemExporterType.KAFKA
    elif output is None or output == "console":
        item_exporter_type = ItemExporterType.CONSOLE
    else:
        item_exporter_type = ItemExporterType.UNKNOWN

    if item_exporter_type == ItemExporterType.CONSOLE:
        # Only pass entity types to ConsoleItemExporter for filtering
        console_entity_types = [et.value for et in entity_types] if entity_types else None
        item_exporter = ConsoleItemExporter(entity_types=console_entity_types)
    elif item_exporter_type == ItemExporterType.KAFKA:

        # Extract broker URL
        kafka_broker_url = output
        if output.startswith("kafka/"):
            kafka_broker_url = output.split("/", 1)[1]
        
        # Fallback to settings if empty (though logic above handles explicit output)
        if not kafka_broker_url and configs.kafka.output:
             temp_out = configs.kafka.output
             if temp_out.startswith("kafka/"):
                 kafka_broker_url = temp_out.split("/", 1)[1]
             else:
                 kafka_broker_url = temp_out

        if not kafka_broker_url:
            raise ValueError(f"Kafka broker URL could not be determined from output: {output}")

        # Map topics with prefix from settings
        item_type_to_topic_mapping = create_topic_mapping(topic_prefix)

        item_exporter = KafkaItemExporter(
            kafka_broker_url=kafka_broker_url,
            item_type_to_topic_mapping=item_type_to_topic_mapping,
        )

    else:
        raise ValueError("Unable to determine item exporter type for output " + output)

    return item_exporter

def create_item_exporters(
    outputs,
    entity_types=None,
    topic_prefix=None
):
    split_outputs = [output.strip() for output in outputs.split(",")] if outputs else ["console"]

    item_exporters = [create_item_exporter(output, entity_types, topic_prefix) for output in split_outputs]
    return MultiItemExporter(item_exporters)