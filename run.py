import uvloop

from cli import cli
from utils.logger_utils import configure_logging, get_logger

configure_logging()
logger = get_logger("Run Entry Point")

if __name__ == "__main__":
    try:
        uvloop.install()
        logger.info("uvloop installed successfully.")
    except ImportError:
        logger.info("uvloop not found, using default asyncio event loop.")

    cli()
