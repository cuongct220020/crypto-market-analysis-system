import pytest
from unittest.mock import MagicMock, patch, ANY
from ingestion.kafka_producer import KafkaProducerWrapper
from ingestion.ethereumetl.models.block import EthBlock
import orjson

@pytest.fixture
def mock_confluent_producer():
    with patch("ingestion.kafka_producer.Producer") as MockProducer:
        yield MockProducer.return_value

@pytest.fixture
def producer_wrapper(mock_confluent_producer):
    # Mock SchemaRegistryClient to avoid real connection
    with patch("ingestion.kafka_producer.SchemaRegistryClient"):
        wrapper = KafkaProducerWrapper(kafka_broker_url="localhost:9095", schema_registry_url="http://mock:8881")
        # Mock serializers list to simulate loaded schemas
        wrapper.serializers = {} 
        return wrapper

def test_produce_fallback_to_json_when_no_schema(producer_wrapper, mock_confluent_producer):
    item = {"type": "test", "val": None} # None value
    
    # Produce with a schema key that doesn't exist in wrapper.serializers
    producer_wrapper.produce(topic="test_topic", value=item, schema_key="unknown_schema")
    
    # Should call confluent producer with JSON dumped bytes
    mock_confluent_producer.produce.assert_called_once()
    call_args = mock_confluent_producer.produce.call_args
    
    # Check if value sent is valid JSON bytes
    sent_value = call_args.kwargs['value']
    assert isinstance(sent_value, bytes)
    assert orjson.loads(sent_value) == item

def test_produce_pydantic_with_nulls_to_json(producer_wrapper, mock_confluent_producer):
    # Create model with null fields
    block = EthBlock(number=100, type="block", hash="0x123") # hash explicitly set to a value

    producer_wrapper.produce(topic="blocks", value=block, schema_key="block_schema_missing")

    mock_confluent_producer.produce.assert_called_once()
    sent_value = mock_confluent_producer.produce.call_args.kwargs['value']

    # Deserialize and check
    data = orjson.loads(sent_value)
    assert data['number'] == 100
    assert data['hash'] == "0x123"
