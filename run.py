import uvloop

from cli import cli
from utils.logger_utils import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    try:
        uvloop.install()
        logger.info("uvloop installed successfully.")
    except ImportError:
        logger.info("uvloop not found, using default asyncio event loop.")

    cli()
