import asyncio
import click
import json
from typing import List, Optional

from config.configs import configs
from ingestion.rpc_client import RpcClient
from ingestion.oracle.chainlink_client import ChainlinkClient
from ingestion.kafka_producer_wrapper import KafkaProducerWrapper
from constants.price_feed_contract_address import PRICE_FEED_CONTRACT_ADDRESS
from utils.logger_utils import configure_logging, get_logger

logger = get_logger("Get Eth Latest Token Prices CLI")

@click.command()
@click.option(
    "-p",
    "--pairs",
    default=None,
    type=str,
    help="Comma-separated list of pairs to fetch (e.g. eth-usd,btc-usd). If not provided, fetches all available pairs."
)
@click.option(
    "-o",
    "--output",
    default=configs.kafka.output,
    type=str,
    help="Output format. 'kafka/...' for Kafka, or print to console if not specified."
)
@click.option("--log-file", default=None, show_default=True, type=str, help="Path to the log file.")
def get_eth_latest_token_price(pairs: str, output: str, log_file: str):
    """
    Fetches real-time token prices from Chainlink Price Feeds on Ethereum.
    """
    configure_logging(log_file)
    
    # Parse pairs
    if pairs:
        target_pairs = [p.strip().lower() for p in pairs.split(",")]
    else:
        target_pairs = list(PRICE_FEED_CONTRACT_ADDRESS.keys())
    
    logger.info(f"Starting price fetch for: {target_pairs}")
    
    try:
        asyncio.run(_fetch_and_process(target_pairs, output))
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception as e:
        logger.exception("An error occurred:")
        raise e

async def _fetch_and_process(target_pairs: List[str], output: str):
    rpc_client = RpcClient(rpc_url=configs.ethereum.rpc_provider_uris)
    chainlink_client = ChainlinkClient(rpc_client)

    kafka_producer: Optional[KafkaProducerWrapper] = None
    
    # Determine Output Modes
    console_enabled = False
    kafka_enabled = False
    kafka_url = None

    if output:
        output_lower = output.lower()
        if "console" in output_lower:
            console_enabled = True
        
        if "kafka" in output_lower:
            kafka_enabled = True
            # Try to extract URL if mixed
            parts = output.split(",")
            for p in parts:
                if "kafka" in p.lower():
                    kafka_url = p.strip()
                    break

    # If no specific output specified, default to console if not configured? 
    # Or just rely on flags. If nothing matches, do nothing? 
    # Original logic defaulted to Console if not Kafka.
    if not kafka_enabled and not console_enabled:
        console_enabled = True

    if kafka_enabled and kafka_url:
        try:
            kafka_producer = KafkaProducerWrapper(kafka_broker_url=kafka_url)
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            # If console is enabled, we continue, otherwise return?
            if not console_enabled:
                await rpc_client.close()
                return

    try:
        # Always Fetch
        prices = await chainlink_client.get_latest_prices(target_pairs)
        
        # 1. Output to Kafka
        if kafka_producer and prices:
            logger.info(f"Mode: Ingest to Kafka ({kafka_url})")
            topic = "chainlink.eth.prices.latest.v0"
            for pair, data in prices.items():
                kafka_producer.produce(
                    topic=topic,
                    value=data,
                    schema_key="chainlink_price",
                    key=pair
                )
            kafka_producer.flush()
            print(f"Successfully pushed {len(prices)} prices to Kafka.")
        
        # 2. Output to Console
        if console_enabled:
            logger.info("Mode: Print to Console")
            print("\n--- Real-time Prices (Chainlink) ---")
            print(json.dumps(prices, indent=2))
            print("------------------------------------\n")

    finally:
        if kafka_producer:
            kafka_producer.close()
        await rpc_client.close()
