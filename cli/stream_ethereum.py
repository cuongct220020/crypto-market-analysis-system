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
from typing import List, Optional

import click

from config.configs import configs
from ingestion.blockchainetl.streaming.streamer import Streamer
from ingestion.ethereumetl.enums.entity_type import EntityType
from ingestion.ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter
from ingestion.ethereumetl.streaming.item_exporter_creator import create_item_exporters
from utils.logger_utils import configure_logging, get_logger
from utils.signal_utils import configure_signals

logger = get_logger("Streaming CLI")


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("-l", "--last-synced-block-file", default="last_synced_block.txt", show_default=True, type=str, help="")
@click.option("--lag", default=0, show_default=True, type=int, help="The number of blocks to lag behind the network.")
@click.option(
    "-p",
    "--provider-uri",
    default=configs.ethereum.provider_uri,  # Use setting from config/configs.py
    show_default=True,
    type=str,
    help="The URI(s) of the web3 provider(s) e.g. "
    "file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io. Multiple URIs can be separated by commas for fallback.",
)
@click.option(
    "-o",
    "--output",
    default=configs.kafka.output,  # Use setting from config/configs.py
    type=str,
    help="Either kafka, output name and connection host:port e.g. kafka/127.0.0.1:9095 "
    "or not specified will print to console",
)
@click.option("-s", "--start-block", default=None, show_default=True, type=int, help="Start block")
@click.option("-e", "--end-block", default=None, show_default=True, type=int, help="End block")
@click.option(
    "-et",
    "--entity-types",
    default=configs.ethereum.entity_types,
    show_default=True,
    type=str,
    help="The list of entity types to export.",
)
@click.option(
    "--period-seconds",
    default=configs.streamer.period_seconds,
    show_default=True,
    type=int,
    help="How many seconds to sleep between syncs",
)
@click.option(
    "-b",
    "--batch-size",
    default=configs.ethereum.batch_size,
    show_default=True,
    type=int,
    help="How many blocks to batch in single request",
)
@click.option(
    "-B",
    "--block-batch-size",
    default=configs.streamer.block_batch_size,
    show_default=True,
    type=int,
    help="How many blocks to batch in single sync round",
)
@click.option(
    "-w",
    "--max-workers",
    default=configs.ethereum.max_workers,
    show_default=True,
    type=int,
    help="The number of workers",
)
@click.option(
    "--max-concurrent-requests",
    default=configs.ethereum.max_concurrent_requests,
    show_default=True,
    type=int,
    help="The number of max concurrent RPC requests (e.g. for free tier)",
)
@click.option("--log-file", default=None, show_default=True, type=str, help="Log file")
@click.option("--pid-file", default=None, show_default=True, type=str, help="pid file")
def stream_ethereum(
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
    max_concurrent_requests: int = 5,
    log_file: Optional[str] = None,
    pid_file: Optional[str] = None,
):
    """Streams all data types to Apache Kafka or console for debugging"""
    configure_logging(log_file)
    configure_signals()
    entity_types_list = _parse_entity_types(entity_types)
    provider_uris_list = _parse_provider_uris(provider_uri)

    logger.info(f"Starting streaming with providers: {provider_uris_list}")

    try:
        streamer_adapter = EthStreamerAdapter(
            item_exporter=create_item_exporters(output, entity_types=entity_types_list),
            batch_size=batch_size,
            max_workers=max_workers,
            max_concurrent_requests=max_concurrent_requests,
            entity_types_list=entity_types_list,
            provider_uri_list=provider_uris_list,
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


def _parse_entity_types(entity_types_str: str) -> List[EntityType]:
    """
    Parse entity types string to a list of EntityType
    """
    entity_types = [c.strip() for c in entity_types_str.split(",")]

    try:
        # validate passed types
        list_entity = []
        for entity_type in entity_types:
            list_entity.append(EntityType(entity_type))
        return list_entity
    except:
        raise click.BadOptionUsage("--entity-types", "Invalid entity types provided.")


def _parse_provider_uris(provider_uri_str: str) -> List[str]:
    """
    Parses a comma-separated string of provider URIs into a list.
    """
    try:
        provider_uris_list = []
        for provider_uri in provider_uri_str.split(","):
            provider_uris_list.append(provider_uri.strip())
        return provider_uris_list

    except:
        raise click.BadOptionUsage("--provider-uri", "At least one valid provider URI must be specified.")