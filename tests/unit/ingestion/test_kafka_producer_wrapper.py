import pytest
from unittest.mock import MagicMock, patch, call
from ingestion.kafka_producer_wrapper import KafkaProducerWrapper
from ingestion.ethereumetl.models.block import EthBlock
import orjson

@pytest.fixture
def mock_confluent_producer():
    with patch("ingestion.kafka_producer_wrapper.Producer") as MockProducer:
        producer_instance = MockProducer.return_value
        # Mock poll to do nothing by default
        producer_instance.poll.return_value = 0
        yield producer_instance

@pytest.fixture
def producer_wrapper(mock_confluent_producer):
    # Mock SchemaRegistryClient
    with patch("ingestion.kafka_producer_wrapper.SchemaRegistryClient"):
        wrapper = KafkaProducerWrapper(kafka_broker_url="localhost:9095")
        wrapper.serializers = {} 
        return wrapper

def test_produce_success(producer_wrapper, mock_confluent_producer):
    item = {"id": 1}
    producer_wrapper.produce("test_topic", item)
    
    # Should call produce once and poll(0) once
    mock_confluent_producer.produce.assert_called_once()
    mock_confluent_producer.poll.assert_called_with(0)

def test_produce_backpressure_buffer_error(producer_wrapper, mock_confluent_producer):
    item = {"id": 1}
    
    # Side effect: First call raises BufferError (Queue Full), Second call succeeds
    mock_confluent_producer.produce.side_effect = [BufferError("Local queue full"), None]
    
    producer_wrapper.produce("test_topic", item)
    
    # Should attempt produce twice
    assert mock_confluent_producer.produce.call_count == 2
    
    # Should call poll(0.5) when BufferError occurs to drain queue
    mock_confluent_producer.poll.assert_any_call(0.5)
    
    # Should finally call poll(0) after success
    mock_confluent_producer.poll.assert_any_call(0)

def test_produce_fallback_to_json(producer_wrapper, mock_confluent_producer):
    item = {"id": 1}
    # Simulate valid schema key but missing serializer (or serializer failure)
    # Here we just test basic json fallback path which happens when schema_key is None or not in serializers
    
    producer_wrapper.produce("test_topic", item, schema_key="unknown_schema")
    
    call_args = mock_confluent_producer.produce.call_args
    assert isinstance(call_args.kwargs['value'], bytes)
    assert orjson.loads(call_args.kwargs['value']) == item