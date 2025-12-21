from ingestion.web2.defillama_client_todo import DefiLlamaClient
from ingestion.kafka_producer_wrapper import KafkaProducerWrapper
from utils.logger_utils import get_logger

logger = get_logger("DefiLlama Ingester")

TOPIC_PROTOCOLS_MARKET = "defillama.protocols.market.v0"

async def ingest_protocol_data(kafka_output: str):
    """
    Ingests protocol TVL and basic info from DefiLlama.
    """
    producer = KafkaProducerWrapper(kafka_broker_url=kafka_output)
    
    async with DefiLlamaClient() as dl:
        pass