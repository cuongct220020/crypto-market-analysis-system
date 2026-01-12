import asyncio
import click
from config.configs import configs
from ingestion.web2.coingecko_ingester import ingest_market_data
from utils.logger_utils import configure_logging, get_logger

logger = get_logger("Get Eth Market Data CLI")

@click.command()
@click.option(
    "-o",
    "--output",
    default=configs.kafka.output,
    type=str,
    help="Output format. 'kafka' for Kafka, or print to console if not specified. "
    "e.g. kafka/localhost:9092",
)
@click.option("--log-file", default=None, show_default=True, type=str, help="Path to the log file.")
def get_eth_market_data(output: str, log_file: str):
    """
    Fetches current market data for Ethereum tokens from CoinGecko
    and publishes to Kafka.
    """
    configure_logging(log_file)
    logger.info("Starting CoinGecko market data ingestion...")
    
    try:
        asyncio.run(ingest_market_data(kafka_output=output))
    except KeyboardInterrupt:
        logger.info("Ingestion interrupted by user.")
    except Exception as e:
        logger.exception("An error occurred during ingestion:")
        raise e

if __name__ == "__main__":
    get_eth_market_data()