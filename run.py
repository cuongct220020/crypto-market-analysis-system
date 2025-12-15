try:
    import uvloop
except ImportError:
    uvloop = None

from cli import cli
from utils.logger_utils import configure_logging, get_logger

configure_logging()
logger = get_logger("Run Entry Point")

if __name__ == "__main__":
    if uvloop:
        uvloop.install()
        logger.info("uvloop installed successfully.")
    else:
        logger.info("uvloop not found, using default asyncio event loop.")

    cli()
