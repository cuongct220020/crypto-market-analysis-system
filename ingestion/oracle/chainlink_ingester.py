from typing import List, Dict, Any
from ingestion.kafka_producer_wrapper import KafkaProducerWrapper
from ingestion.oracle.chainlink_client import ChainlinkClient
from utils.logger_utils import get_logger

logger = get_logger("Chainlink Ingester")


class ChainlinkIngester:
    """
    Ingests Chainlink price feeds into Kafka.
    """

    def __init__(self, chainlink_client: ChainlinkClient, kafka_producer: KafkaProducerWrapper):
        self.chainlink_client = chainlink_client
        self.kafka_producer = kafka_producer

    async def ingest_prices(self, pair_names: List[str]) -> Dict[str, Any]:
        """
        Fetches prices from Chainlink and publishes them to Kafka.
        Returns the fetched prices for further processing or logging.
        """
        try:
            prices_data = await self.chainlink_client.get_latest_prices(pair_names)

            if not prices_data:
                logger.warning("No price data fetched from Chainlink.")
                return {}

            topic = "chainlink.eth.prices.latest.v0"
            for pair, data in prices_data.items():
                self.kafka_producer.produce(
                    topic=topic,
                    value=data,
                    schema_key="chainlink_price",
                    key=pair
                )

            logger.info(f"Produced {len(prices_data)} price updates to Kafka topic '{topic}'.")
            return prices_data

        except Exception as e:
            logger.exception(f"Error ingesting Chainlink prices: {e}")
            raise e
