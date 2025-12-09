import asyncio
import logging
from cli import cli

# Configure logging early
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        import uvloop

        uvloop.install()
        logger.info("uvloop installed successfully.")
    except ImportError:
        logger.info("uvloop not found, using default asyncio event loop.")

    cli()
