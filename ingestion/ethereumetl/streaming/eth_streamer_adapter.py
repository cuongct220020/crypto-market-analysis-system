import asyncio
from typing import Any, List, Tuple, Union
from web3 import AsyncWeb3

from config.configs import configs
from ingestion.blockchainetl.exporters.in_memory_buffer_exporter import InMemoryBufferExporter
from ingestion.blockchainetl.exporters.console_exporter import ConsoleItemExporter
from ingestion.ethereumetl.enums.entity_type import EntityType
from ingestion.ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ingestion.ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
# from ingestion.ethereumetl.jobs.extract_contracts_job import ExtractContractsJob
from ingestion.ethereumetl.jobs.extract_token_transfers_job import ExtractTokenTransfersJob
from ingestion.ethereumetl.models.block import EthBlock
# from ingestion.ethereumetl.models.contract import EthContract
from ingestion.ethereumetl.models.receipt import EthReceipt
from ingestion.ethereumetl.models.receipt_log import EthReceiptLog
from ingestion.ethereumetl.models.token_transfer import EthTokenTransfer
from ingestion.ethereumetl.models.transaction import EnrichedEthTransaction, EthTransaction
from ingestion.ethereumetl.providers.provider_factory import get_failover_async_provider_from_uris
from ingestion.ethereumetl.streaming.enrich_stream_data import (
    # enrich_contracts,
    enrich_transactions,
)
from utils.logger_utils import get_logger

logger = get_logger("ETH Streamer Adapter")


class EthStreamerAdapter:
    def __init__(
        self,
        item_exporter: Any = ConsoleItemExporter(configs.ethereum.entity_types),
        entity_types_list: List[EntityType] = None,
        provider_uri_list: List[str] = None,
        batch_size: int = configs.ethereum.batch_size,
        max_workers: int = configs.ethereum.max_workers,
        max_concurrent_requests: int = configs.ethereum.max_concurrent_requests,
    ):
        self._item_exporter = item_exporter
        self._entity_types = entity_types_list
        self._batch_size = batch_size
        self._max_workers = max_workers
        self._max_concurrent_requests = max_concurrent_requests
        self._provider_uri_list = provider_uri_list or [configs.ethereum.provider_uri]

        # Build AsyncWeb3 provider
        self._async_provider = get_failover_async_provider_from_uris(
            self._provider_uri_list, timeout=configs.ethereum.rpc_timeout
        )
        self._w3 = AsyncWeb3(self._async_provider)

    async def open(self) -> None:
        self._item_exporter.open()

    async def get_current_block_number(self) -> int:
        return await self._w3.eth.get_block_number()

    async def export_all(self, start_block: int, end_block: int = None) -> None:
        # Export blocks and transactions
        blocks: List[EthBlock] = []
        transactions: List[EthTransaction] = []
        if self._should_export(EntityType.BLOCK) or self._should_export(EntityType.TRANSACTION):
            blocks, transactions = await self._export_blocks_and_transactions(start_block, end_block)


        # Export receipts and logs
        receipts: List[EthReceipt] = []
        receipt_logs: List[EthReceiptLog] = []
        if self._should_export(EntityType.RECEIPT) or self._should_export(EntityType.LOG):
            receipts, logs = await self._export_receipts_and_logs(start_block, end_block)


        # Extract token transfers
        token_transfers: List[EthTokenTransfer] = []
        if self._should_export(EntityType.TOKEN_TRANSFER):
            token_transfers = await self._extract_token_transfers(receipt_logs)


        # # Export contracts
        # contracts: List[EthContract] = []
        # if self._should_export(EntityType.CONTRACT):
        #     contracts = await self._export_contracts(receipt_logs)


        # Create enriched entities based on requested entity types
        enriched_transactions: List[EnrichedEthTransaction] = (
            enrich_transactions(transactions, receipts) if EntityType.TRANSACTION in self._entity_types else []
        )
        # enriched_contracts: List[EnrichedEthContract] = (
        #     enrich_contracts(blocks, contracts) if EntityType.CONTRACT in self._entity_types else []
        # )

        # Combine and sort all items based on requested entity types
        all_items: List[Any] = []
        if EntityType.BLOCK in self._entity_types:
            all_items.extend(_sort_by(blocks, "number"))

        if EntityType.TRANSACTION in self._entity_types:
            all_items.extend(_sort_by(enriched_transactions, ("block_number", "transaction_index")))

        if EntityType.LOG in self._entity_types:
            all_items.extend(_sort_by(receipt_logs, ("block_number", "log_index")))

        if EntityType.TOKEN_TRANSFER in self._entity_types:
            all_items.extend(_sort_by(token_transfers, ("block_number", "log_index")))

        # if EntityType.CONTRACT in self._entity_types:
        #     all_items.extend(_sort_by(enriched_contracts, "block_number"))

        logger.info(
            f"Exporting {len(all_items)} items: "
            f"{len(blocks)} blocks, "
            f"{len(enriched_transactions)} transactions, "
            f"{len(receipt_logs)} logs, "
            f"{len(token_transfers)} token_transfers, "
            # f"{len(contracts)} contracts, "
        )

        self._item_exporter.export_items(all_items)

    async def _export_blocks_and_transactions(
        self, start_block: int, end_block: int
    ) -> Tuple[List[EthBlock], List[EthTransaction]]:

        buffer_exporter = InMemoryBufferExporter(item_types=["block", "transaction"])
        job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self._batch_size,
            web3=self._w3,
            max_workers=self._max_workers,
            max_concurrent_requests=self._max_concurrent_requests,
            item_exporter=buffer_exporter,
            export_blocks=self._should_export(EntityType.BLOCK),
            export_transactions=self._should_export(EntityType.TRANSACTION),
        )
        await job.run()
        blocks = buffer_exporter.get_items("block")
        transactions = buffer_exporter.get_items("transaction")
        return blocks, transactions

    async def _export_receipts_and_logs(
            self,
            start_block: int,
            end_block: int = None
    ) -> Tuple[List[EthReceipt], List[EthReceiptLog]]:

        exporter = InMemoryBufferExporter(item_types=["receipt", "log"])
        job = ExportReceiptsJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self._batch_size,
            web3=self._w3,
            max_workers=self._max_workers,
            max_concurrent_requests=self._max_concurrent_requests,
            item_exporter=exporter,
            export_receipts=self._should_export(EntityType.RECEIPT),
            export_logs=self._should_export(EntityType.LOG),
        )
        await job.run()
        receipts = exporter.get_items("receipt")
        receipt_logs = exporter.get_items("log")
        return receipts, receipt_logs


    @staticmethod
    def _extract_token_transfers(receipt_logs: List[EthReceiptLog]) -> List[EthTokenTransfer]:
        exporter = InMemoryBufferExporter(item_types=["token_transfer"])
        job = ExtractTokenTransfersJob(
            receipt_logs=receipt_logs,
            item_exporter=exporter
        )
        # CPU bound, run in thread
        job.run()
        token_transfers = exporter.get_items("token_transfer")
        return token_transfers

    # @staticmethod
    # async def _export_contracts(receipt_logs: List[EthReceiptLog]) -> List[EthContract]:
    #     exporter = InMemoryBufferExporter(item_types=["contract"])
    #     job = ExtractContractsJob(
    #         receipt_logs=receipt_logs,
    #         item_exporter=exporter
    #     )
    #     # CPU bound
    #     await asyncio.to_thread(job.run)
    #     contracts = exporter.get_items("contract")
    #     return contracts


    def close(self) -> None:
        self._item_exporter.close()

    def _should_export(self, entity_type: EntityType) -> bool:
        if entity_type == EntityType.BLOCK:
            return True

        if entity_type == EntityType.TRANSACTION:
            return (
                EntityType.TRANSACTION in self._entity_types or
                self._should_export(EntityType.RECEIPT)
            )

        if entity_type == EntityType.RECEIPT:
            return (
                EntityType.RECEIPT in self._entity_types or
                self._should_export(EntityType.LOG)
            )

        if entity_type == EntityType.LOG:
            return (
                EntityType.LOG in self._entity_types or
                self._should_export(EntityType.TOKEN_TRANSFER)
            )

        if entity_type == EntityType.TOKEN_TRANSFER:
            return EntityType.TOKEN_TRANSFER in self._entity_types

        # if entity_type == EntityType.CONTRACT:
        #     return EntityType.CONTRACT in self._entity_types

        raise ValueError(f"Unexpected entity type {entity_type.value}")


def _sort_by(arr: List[Any], fields: Union[str, Tuple[str, ...]]) -> List[Any]:
    if isinstance(fields, tuple):
        fields = tuple(fields)

    def get_key(item, f):
        if hasattr(item, "get"):
            return item.get(f)
        return getattr(item, f, None)

    return sorted(arr, key=lambda item: tuple(get_key(item, f) for f in fields))
