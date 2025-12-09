# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Modified By: Cuong CT, 6/12/2025
# Change Description: Integrated with Pydantic Settings for centralized configuration.


import asyncio
import click
from typing import List, Optional

from config.settings import settings
from ingestion.ethereumetl.enums.entity_type import EntityType
from ingestion.ethereumetl.providers.provider_factory import get_provider_from_uri
from ingestion.ethereumetl.streaming.item_exporter_creator import create_item_exporters
from ingestion.blockchainetl.streaming.streamer import Streamer
from ingestion.ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter
from utils.logger_utils import configure_logging, get_logger
from utils.signal_utils import configure_signals


logger = get_logger(__name__)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("-l", "--last-synced-block-file", default="last_synced_block.txt", show_default=True, type=str, help="")
@click.option("--lag", default=0, show_default=True, type=int, help="The number of blocks to lag behind the network.")
@click.option(
    "-p",
    "--provider-uri",
    default=settings.ethereum.provider_uri,  # Use setting from config/settings.py
    show_default=True,
    type=str,
    help="The URI(s) of the web3 provider(s) e.g. " "file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io. Multiple URIs can be separated by commas for fallback.",
)
@click.option(
    "-o",
    "--output",
    default=settings.kafka.output,  # Use setting from config/settings.py
    type=str,
    help="Either kafka, output name and connection host:port e.g. kafka/127.0.0.1:9092 "
    "or not specified will print to console",
)
@click.option("-s", "--start-block", default=None, show_default=True, type=int, help="Start block")
@click.option("--end-block", default=None, show_default=True, type=int, help="End block")
@click.option(
    "-e",
    "--entity-types",
    default=",".join(EntityType.ALL_FOR_INFURA),
    show_default=True,
    type=str,
    help="The list of entity types to export.",
)
@click.option(
    "--period-seconds", default=settings.streamer.period_seconds, show_default=True, type=int, help="How many seconds to sleep between syncs"
)
@click.option(
    "-b",
    "--batch-size",
    default=settings.ethereum.batch_size,
    show_default=True,
    type=int,
    help="How many blocks to batch in single request",
)
@click.option(
    "-B",
    "--block-batch-size",
    default=settings.streamer.block_batch_size,
    show_default=True,
    type=int,
    help="How many blocks to batch in single sync round",
)
@click.option(
    "-w", "--max-workers", default=settings.ethereum.max_workers, show_default=True, type=int, help="The number of workers (max concurrent requests)"
)
@click.option("--log-file", default=None, show_default=True, type=str, help="Log file")
@click.option("--pid-file", default=None, show_default=True, type=str, help="pid file")
def streaming(
    last_synced_block_file: str,
    lag: int,
    provider_uri: str,
    output: str,
    start_block: Optional[int],
    end_block: Optional[int],
    entity_types: str,
    period_seconds: int = 10,
    batch_size: int = 2,
    block_batch_size: int = 10,
    max_workers: int = 5,
    log_file: Optional[str] = None,
    pid_file: Optional[str] = None,
):
    """Streams all data types to Apache Kafka or console for debugging"""
    configure_logging(log_file)
    configure_signals()
    entity_types_list = parse_entity_types(entity_types)
    provider_uris_list = parse_provider_uris(provider_uri)

    if not provider_uris_list:
        raise click.BadOptionUsage("--provider-uri", "At least one valid provider URI must be specified.")

    # Currently, EthStreamerAdapter only supports a single provider URI.
    # The first URI from the list will be used.
    # TODO: Update EthStreamerAdapter to handle a list of URIs for fallback mechanism.
    current_provider_uri = provider_uris_list[0]
    logger.info(f"Starting streaming with providers: {provider_uris_list}. Using {current_provider_uri} as primary.")

    try:
        # Note: We don't pass batch_web3_provider here anymore, as EthStreamerAdapter builds its own Async provider.
        streamer_adapter = EthStreamerAdapter(
            item_exporter=create_item_exporters(output),
            batch_size=batch_size,
            max_workers=max_workers,
            entity_types=entity_types_list,
            # For now, pass the first URI. EthStreamerAdapter needs refactoring to accept a list of URIs.
            # In a future refactor, provider_uris_list should be passed directly to EthStreamerAdapter
            # and the adapter will manage the fallback logic.
            # provider_uri=current_provider_uri # This will need to be added to EthStreamerAdapter's __init__
        )
        streamer = Streamer(
            blockchain_streamer_adapter=streamer_adapter,
            last_synced_block_file=last_synced_block_file,
            lag=lag,
            start_block=start_block,
            end_block=end_block,
            period_seconds=period_seconds,
            block_batch_size=block_batch_size,
            pid_file=pid_file,
        )
        asyncio.run(streamer.stream())
    except KeyboardInterrupt:
        logger.info("Streaming interrupted by user. Shutting down gracefully...")
    except Exception as e:
        logger.exception("An unhandled error occurred during streaming:")
        raise e


def parse_entity_types(entity_types_str: str) -> List[EntityType]:
    entity_types = [c.strip() for c in entity_types_str.split(",")]

    # validate passed types
    for entity_type in entity_types:
        if entity_type not in EntityType.ALL_FOR_STREAMING:
            raise click.BadOptionUsage(
                "--entity-type",
                "{} is not an available entity type. Supply a comma separated list of types from {}".format(
                    entity_type, ",".join(EntityType.ALL_FOR_STREAMING)
                ),
            )
    # Convert string entity types to EntityType enum members
    return [EntityType(et) for et in entity_types]


def parse_provider_uris(provider_uri_str: str) -> List[str]:
    """
    Parses a comma-separated string of provider URIs into a list.
    """
    return [uri.strip() for uri in provider_uri_str.split(',') if uri.strip()]
