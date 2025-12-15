import signal
import sys

from utils.logger_utils import get_logger

logger = get_logger("Signal Utils")


def configure_signals():
    """
    Configures signal handlers for graceful shutdown.
    Handles SIGTERM to exit the program cleanly.
    """

    def sigterm_handler(_signo, _stack_frame):
        logger.info("Received SIGTERM. Exiting...")
        sys.exit(0)

    signal.signal(signal.SIGTERM, sigterm_handler)
