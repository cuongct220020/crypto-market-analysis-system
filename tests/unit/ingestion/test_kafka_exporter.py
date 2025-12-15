import pytest
from unittest.mock import MagicMock, patch
from ingestion.blockchainetl.exporters.kafka_item_exporter import KafkaItemExporter
from ingestion.ethereumetl.models.block import EthBlock

# Sample data with nulls
SAMPLE_BLOCK_WITH_NULLS = EthBlock(
    number=123,
    hash="0xabc",
    # parent_hash is None (default)
    timestamp=100000
)

@pytest.fixture
def mock_producer_wrapper():
    with patch("ingestion.blockchainetl.exporters.kafka_exporter.KafkaProducerWrapper") as MockWrapper:
        mock_instance = MockWrapper.return_value
        yield mock_instance

def test_export_item_pydantic_model(mock_producer_wrapper):
    # Setup
    exporter = KafkaItemExporter(
        kafka_broker_url="localhost:9092",
        item_type_to_topic_mapping={"block": "blocks_topic"}
    )
    
    # Execution
    exporter.export_item(SAMPLE_BLOCK_WITH_NULLS)
    
    # Assertion
    # Verify produce was called with correct topic and value
    mock_producer_wrapper.produce.assert_called_once()
    call_args = mock_producer_wrapper.produce.call_args
    assert call_args.kwargs['topic'] == "blocks_topic"
    assert call_args.kwargs['value'] == SAMPLE_BLOCK_WITH_NULLS
    assert call_args.kwargs['schema_key'] == "block"

def test_export_item_missing_type(mock_producer_wrapper):
    exporter = KafkaItemExporter(kafka_broker_url="localhost:9092", item_type_to_topic_mapping={})
    # Item without type
    item = {"id": 1} 
    
    exporter.export_item(item)
    
    # Should not produce anything
    mock_producer_wrapper.produce.assert_not_called()

def test_export_item_unconfigured_topic(mock_producer_wrapper):
    exporter = KafkaItemExporter(
        kafka_broker_url="localhost:9092",
        item_type_to_topic_mapping={"block": "blocks_topic"}
    )
    # Item with type but no mapping
    item = {"type": "unknown_entity", "id": 1}
    
    exporter.export_item(item)
    
    # Should not produce
    mock_producer_wrapper.produce.assert_not_called()
