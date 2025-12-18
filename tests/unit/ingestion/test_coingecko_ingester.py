import pytest
from unittest.mock import MagicMock, AsyncMock, patch, call
import asyncio

from ingestion.web2.coingecko_ingester import (
    _get_target_eth_coins,
    ingest_market_data,
    ingest_historical_data,
    TOPIC_COINS_MARKET,
    TOPIC_COIN_HISTORICAL
)

# Mock data
MOCK_COIN_LIST = [
    {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "platforms": {"ethereum": "0x123"}}, 
    {"id": "chainlink", "symbol": "link", "name": "Chainlink", "platforms": {"ethereum": "0x456"}}, 
    {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "platforms": {}}, 
    {"id": "random", "symbol": "rnd", "name": "Random", "platforms": {"ethereum": "0x789"}}, 
]

# Mock Price Feed Keys
# Target symbols derived will be: eth, usd, link
MOCK_PRICE_FEED = {
    "eth-usd": "0x...",
    "link-eth": "0x...",
}

@pytest.fixture
def mock_kafka_producer():
    with patch("ingestion.web2.coingecko_ingester.KafkaProducerWrapper") as mock:
        producer_instance = mock.return_value
        producer_instance.produce = MagicMock()
        producer_instance.flush = MagicMock()
        yield producer_instance

@pytest.fixture
def mock_coingecko_client():
    with patch("ingestion.web2.coingecko_ingester.CoinGeckoClient") as mock:
        client_instance = mock.return_value
        client_instance.__aenter__.return_value = client_instance
        client_instance.__aexit__.return_value = None
        
        # Default behaviors
        client_instance.get_coin_list = AsyncMock(return_value=[])
        client_instance.get_all_coin_markets = AsyncMock(return_value=[])
        client_instance.get_coin_market_chart_range = AsyncMock(return_value={})
        client_instance.get_coin_ohlc = AsyncMock(return_value=[])
        
        yield client_instance

@pytest.mark.asyncio
async def test_get_target_eth_coins_success(mock_coingecko_client):
    # Setup
    mock_coingecko_client.get_coin_list.return_value = MOCK_COIN_LIST
    
    with patch("ingestion.web2.coingecko_ingester.PRICE_FEED_CONTRACT_ADDRESS", MOCK_PRICE_FEED):
        # Execution
        result = await _get_target_eth_coins(mock_coingecko_client)
        
        # Verification
        # Symbols from MOCK_PRICE_FEED: eth, usd, link
        # MOCK_COIN_LIST:
        # - eth (symbol='eth', platform='ethereum'): MATCH
        # - link (symbol='link', platform='ethereum'): MATCH
        # - btc (symbol='btc', no platform): NO MATCH
        # - rnd (symbol='rnd', platform='ethereum'): NO MATCH (rnd not in target symbols)
        
        assert len(result) == 2
        ids = [c["id"] for c in result]
        assert "ethereum" in ids
        assert "chainlink" in ids
        
        # Check contract address extraction
        eth_coin = next(c for c in result if c["id"] == "ethereum")
        assert eth_coin["eth_contract_address"] == "0x123"

@pytest.mark.asyncio
async def test_get_target_eth_coins_empty(mock_coingecko_client):
    mock_coingecko_client.get_coin_list.return_value = []
    
    result = await _get_target_eth_coins(mock_coingecko_client)
    assert result == []

@pytest.mark.asyncio
async def test_ingest_market_data_success(mock_kafka_producer, mock_coingecko_client):
    # Setup
    target_coins = [
        {"id": "chainlink", "symbol": "link", "name": "Chainlink", "eth_contract_address": "0x456"}
    ]
    
    market_data = [
        {
            "id": "chainlink", 
            "current_price": 20.5, 
            "market_cap": 1000000, 
            "image": "http://image.png"
        }
    ]
    
    mock_coingecko_client.get_all_coin_markets.return_value = market_data
    
    with patch("ingestion.web2.coingecko_ingester._get_target_eth_coins", new_callable=AsyncMock) as mock_get_target:
        mock_get_target.return_value = target_coins
        
        await ingest_market_data("kafka:9092")
        
        # Verify
        mock_coingecko_client.get_all_coin_markets.assert_called_once()
        mock_kafka_producer.produce.assert_called_once()
        
        args, kwargs = mock_kafka_producer.produce.call_args
        assert kwargs["topic"] == TOPIC_COINS_MARKET
        assert kwargs["key"] == "chainlink"
        assert kwargs["value"]["current_price"] == 20.5
        assert kwargs["value"]["eth_contract_address"] == "0x456" # Preserved from target_coins
        assert kwargs["value"]["image"] == "http://image.png" # Enriched from market_data

@pytest.mark.asyncio
async def test_ingest_market_data_no_targets(mock_kafka_producer, mock_coingecko_client):
    with patch("ingestion.web2.coingecko_ingester._get_target_eth_coins", new_callable=AsyncMock) as mock_get_target:
        mock_get_target.return_value = []
        
        await ingest_market_data("kafka:9092")
        
        mock_coingecko_client.get_all_coin_markets.assert_not_called()
        mock_kafka_producer.produce.assert_not_called()

class MockTqdm:
    def __init__(self, iterable, **kwargs):
        self.iterable = iterable
    
    def __iter__(self):
        return iter(self.iterable)
    
    def set_postfix(self, **kwargs):
        pass

@pytest.mark.asyncio
async def test_ingest_historical_data_success(mock_kafka_producer, mock_coingecko_client):
    target_coins = [
        {"id": "chainlink", "symbol": "link", "name": "Chainlink", "eth_contract_address": "0x456"}
    ]
    
    # Mock chart data [timestamp, value]
    mock_chart = {
        "prices": [[1000000, 10.0], [2000000, 20.0]],
        "market_caps": [[1000000, 10000], [2000000, 20000]],
        "total_volumes": [[1000000, 500], [2000000, 1000]]
    }
    
    # Mock OHLC data [time, open, high, low, close]
    mock_ohlc = [
        [1000000, 9.0, 11.0, 8.0, 10.0],
        [2000000, 19.0, 21.0, 18.0, 20.0]
    ]
    
    mock_coingecko_client.get_coin_market_chart_range.return_value = mock_chart
    mock_coingecko_client.get_coin_ohlc.return_value = mock_ohlc
    
    # Mock tqdm to avoid output and support set_postfix
    with patch("ingestion.web2.coingecko_ingester.tqdm", side_effect=MockTqdm):
        with patch("ingestion.web2.coingecko_ingester._get_target_eth_coins", new_callable=AsyncMock) as mock_get_target:
            mock_get_target.return_value = target_coins
            
            # Patch sleep to speed up test
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await ingest_historical_data("kafka:9092")
            
            # Verify
            # Should produce 2 records (one for each timestamp)
            assert mock_kafka_producer.produce.call_count == 2
            
            calls = mock_kafka_producer.produce.call_args_list
            values = [c.kwargs['value'] for c in calls]
            
            # Sort by timestamp to be sure
            values.sort(key=lambda x: x['timestamp'])
            
            # Check first record
            r1 = values[0]
            assert r1['timestamp'] == 1000000
            assert r1['coin_id'] == 'chainlink'
            assert r1['price'] == 10.0
            assert r1['open'] == 9.0
            assert r1['high'] == 11.0
            assert r1['low'] == 8.0
            assert r1['close'] == 10.0

@pytest.mark.asyncio
async def test_ingest_historical_data_api_failure(mock_kafka_producer, mock_coingecko_client):
    target_coins = [{"id": "chainlink", "symbol": "link", "name": "Chainlink", "eth_contract_address": "0x456"}]
    
    # Simulate None return (API failure)
    mock_coingecko_client.get_coin_market_chart_range.return_value = None
    
    with patch("ingestion.web2.coingecko_ingester.tqdm", side_effect=MockTqdm):
        with patch("ingestion.web2.coingecko_ingester._get_target_eth_coins", new_callable=AsyncMock) as mock_get_target:
            mock_get_target.return_value = target_coins
            
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await ingest_historical_data("kafka:9092")
                
                # Should not produce anything if chart data is missing
                mock_kafka_producer.produce.assert_not_called()
