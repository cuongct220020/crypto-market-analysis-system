import asyncio
import click
import time
from config.configs import configs
from ingestion.web2.coingecko_ingester import ingest_historical_data
from utils.logger_utils import configure_logging, get_logger

logger = get_logger("Get Eth Historical Market Data CLI")

@click.command()
@click.option(
    "-o",
    "--output",
    default=configs.kafka.output,
    type=str,
    help="Output format. 'kafka' for Kafka, or print to console if not specified. "
    "e.g. kafka/localhost:9092",
)
@click.option(
    "--days",
    default="max",
    show_default=True,
    type=str,
    help="Days of OHLC data to fetch (1/7/14/30/90/180/365/max)."
)
@click.option(
    "--start-ts",
    default=None, 
    show_default=True, 
    type=int, 
    help="Start timestamp for market chart data. Defaults to calculated value based on --days (max=365 days ago for safety)."
)
@click.option("--log-file", default=None, show_default=True, type=str, help="Path to the log file.")
def get_eth_historical_token_data(output: str, days: str, start_ts: int, log_file: str):
    """
    Fetches historical market data (OHLC + Market Chart) for Ethereum tokens from CoinGecko.
    Supports output to Kafka, Console, or both.
    Use --output "kafka/..." for Kafka.
    Use --output "console" for Console.
    Use --output "kafka/...,console" for Both.
    """
    configure_logging(log_file)
    logger.info("Starting CoinGecko historical data ingestion...")

    # Calculate start_ts if not provided
    if start_ts is None:
        now = int(time.time())
        if days.isdigit():
            # If days is a number (e.g. 1, 7, 30), start that many days ago
            start_ts = now - (int(days) * 24 * 60 * 60)
            logger.info(f"Calculated start_ts based on days={days}: {start_ts}")
        else:
            # If days is "max", default to 365 days ago to avoid 401 on Demo API
            # Users with Pro keys should manually specify --start-ts for older data
            start_ts = now - (365 * 24 * 60 * 60)
            logger.info(f"Defaulting start_ts to 365 days ago for 'max' days (safe for Demo API): {start_ts}")
    
    # Determine Output Modes
    console_output = False
    if output and "console" in output.lower():
        console_output = True
        logger.info("Console output enabled.")

    try:
        asyncio.run(ingest_historical_data(
            kafka_output=output, 
            days=days, 
            start_ts=start_ts,
            console_output=console_output
        ))
    except KeyboardInterrupt:
        logger.info("Ingestion interrupted by user.")
    except Exception as e:
        logger.exception("An error occurred during ingestion:")
        raise e
