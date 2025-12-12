import asyncio
from typing import Any, List, Tuple, Union

from config.settings import settings
from ingestion.blockchainetl.exporters.console_exporter import ConsoleItemExporter
from ingestion.ethereumetl.enums.entity_type import EntityType
from ingestion.ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ingestion.ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ingestion.ethereumetl.jobs.extract_contracts_job import ExtractContractsJob
from ingestion.ethereumetl.jobs.extract_token_transfers_job import ExtractTokenTransfersJob
from ingestion.ethereumetl.models.block import EthBlock
from ingestion.ethereumetl.models.contract import EnrichedEthContract, EthContract
from ingestion.ethereumetl.models.receipt import EthReceipt
from ingestion.ethereumetl.models.receipt_log import EthReceiptLog
from ingestion.ethereumetl.models.token_transfer import EthTokenTransfer
from ingestion.ethereumetl.models.transaction import EnrichedEthTransaction, EthTransaction
from ingestion.ethereumetl.providers.provider_factory import get_failover_async_provider_from_uris
from ingestion.ethereumetl.streaming.enrich_stream_data import (
    enrich_contracts,
    enrich_transactions,
)
from utils.logger_utils import get_logger
from utils.web3_utils import build_async_web3

logger = get_logger("ETH Streamer Adapter")


class InMemoryItemBuffer:
    def __init__(self, item_types):
        self.item_types = item_types
        # Initialize empty lists for all expected item types to prevent KeyError
        self.items = {item_type: [] for item_type in item_types}

    def export_item(self, item):
        if hasattr(item, "get"):
            item_type = item.get("type")
        else:
            item_type = getattr(item, "type", None)

        if item_type is None:
            raise ValueError("type key is not found in item {}".format(repr(item)))

        if item_type not in self.items:
             # Fallback: In case an unexpected item type comes in, initialize it dynamically
             # Though strictly speaking, we should probably warn or stick to predefined types.
             self.items[item_type] = []

        self.items[item_type].append(item)

    def get_items(self, item_type):
        return self.items.get(item_type, [])

    def open(self):
        pass

    def close(self):
        pass


class EthStreamerAdapter:
    def __init__(
        self,
        item_exporter: Any = ConsoleItemExporter(),
        entity_types_list: List[EntityType] = None,
        provider_uri_list: List = None,
        batch_size: int = settings.ethereum.batch_size,
        max_workers: int = settings.ethereum.max_workers,
        max_concurrent_requests: int = settings.ethereum.max_concurrent_requests,
    ):
        self.item_exporter = item_exporter
        self.entity_types = entity_types_list
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.max_concurrent_requests = max_concurrent_requests
        self.provider_uri_list = provider_uri_list or [settings.ethereum.provider_uri]

        # Build AsyncWeb3 provider
        self.async_provider = get_failover_async_provider_from_uris(
            self.provider_uri_list, timeout=settings.ethereum.rpc_timeout
        )
        self.w3 = build_async_web3(self.async_provider)

    async def open(self) -> None:
        self.item_exporter.open()

    async def get_current_block_number(self) -> int:
        return await self.w3.eth.get_block_number()

    async def export_all(self, start_block: int, end_block: int) -> None:
        # Export blocks and transactions
        blocks: List[EthBlock] = []
        transactions: List[EthTransaction] = []
        if self._should_export(EntityType.BLOCK) or self._should_export(EntityType.TRANSACTION):
            blocks, transactions = await self._export_blocks_and_transactions(start_block, end_block)

        # Export receipts and logs
        receipts: List[EthReceipt] = []
        logs: List[EthReceiptLog] = []
        if self._should_export(EntityType.RECEIPT) or self._should_export(EntityType.LOG):
            receipts, logs = await self._export_receipts_and_logs(start_block, end_block)


        # Extract token transfers
        token_transfers: List[EthTokenTransfer] = []
        if self._should_export(EntityType.TOKEN_TRANSFER):
            token_transfers = await self._extract_token_transfers(logs)


        # Export contracts
        contracts: List[EthContract] = []
        if self._should_export(EntityType.CONTRACT):
            contracts = await self._export_contracts(logs)


        # Create enriched entities based on requested entity types
        enriched_transactions: List[EnrichedEthTransaction] = (
            enrich_transactions(transactions, receipts) if EntityType.TRANSACTION in self.entity_types else []
        )
        enriched_contracts: List[EnrichedEthContract] = (
            enrich_contracts(blocks, contracts) if EntityType.CONTRACT in self.entity_types else []
        )

        # Combine and sort all items based on requested entity types
        all_items: List[Any] = []
        if EntityType.BLOCK in self.entity_types:
            all_items.extend(_sort_by(blocks, "number"))

        if EntityType.TRANSACTION in self.entity_types:
            all_items.extend(_sort_by(enriched_transactions, ("block_number", "transaction_index")))

        if EntityType.LOG in self.entity_types:
            all_items.extend(_sort_by(logs, ("block_number", "log_index")))

        if EntityType.TOKEN_TRANSFER in self.entity_types:
            all_items.extend(_sort_by(token_transfers, ("block_number", "log_index")))

        if EntityType.CONTRACT in self.entity_types:
            all_items.extend(_sort_by(enriched_contracts, "block_number"))

        logger.info(
            f"Exporting {len(all_items)} items: "
            f"{len(blocks)} blocks, "
            f"{len(enriched_transactions)} transactions, "
            f"{len(logs)} logs, "
            f"{len(token_transfers)} token_transfers, "
            f"{len(contracts)} contracts, "
        )

        self.item_exporter.export_items(all_items)

    async def _export_blocks_and_transactions(
        self, start_block: int, end_block: int
    ) -> Tuple[List[EthBlock], List[EthTransaction]]:
        exporter = InMemoryItemBuffer(item_types=["block", "transaction"])
        job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            web3=self.w3,
            max_workers=self.max_workers,
            max_concurrent_requests=self.max_concurrent_requests,
            item_exporter=exporter,
            export_blocks=self._should_export(EntityType.BLOCK),
            export_transactions=self._should_export(EntityType.TRANSACTION),
        )
        await job.run()
        blocks = exporter.get_items("block")
        transactions = exporter.get_items("transaction")
        return blocks, transactions

    async def _export_receipts_and_logs(self, start_block: int, end_block: int = None) -> List[EthReceiptLog]:
        exporter = InMemoryItemBuffer(item_types=["receipt", "log"])
        job = ExportReceiptsJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            web3=self.w3,
            max_workers=self.max_workers,
            max_concurrent_requests=self.max_concurrent_requests,
            item_exporter=exporter,
            export_receipts=self._should_export(EntityType.RECEIPT),
            export_logs=self._should_export(EntityType.LOG),
        )
        await job.run()
        receipts = exporter.get_items("receipt")
        logs = exporter.get_items("log")
        return receipts, logs


    @staticmethod
    async def _extract_token_transfers(receipt_logs: List[EthReceiptLog]) -> List[EthTokenTransfer]:
        exporter = InMemoryItemBuffer(item_types=["token_transfer"])
        job = ExtractTokenTransfersJob(
            receipt_logs=receipt_logs,
            item_exporter=exporter
        )
        # CPU bound, run in thread
        await asyncio.to_thread(job.run)
        token_transfers = exporter.get_items("token_transfer")
        return token_transfers

    @staticmethod
    async def _export_contracts(receipt_logs: List[EthReceiptLog]) -> List[EthContract]:
        exporter = InMemoryItemBuffer(item_types=["contract"])
        job = ExtractContractsJob(
            item_exporter=exporter
        )
        # CPU bound
        await asyncio.to_thread(job.run)
        contracts = exporter.get_items("contract")
        return contracts


    def close(self) -> None:
        self.item_exporter.close()

    def _should_export(self, entity_type: EntityType) -> bool:
        if entity_type == EntityType.BLOCK:
            return True

        if entity_type == EntityType.TRANSACTION:
            return EntityType.TRANSACTION in self.entity_types or self._should_export(EntityType.LOG)

        if entity_type == EntityType.RECEIPT:
            return EntityType.TRANSACTION in self.entity_types or self._should_export(EntityType.TOKEN_TRANSFER)

        if entity_type == EntityType.LOG:
            return EntityType.LOG in self.entity_types or self._should_export(EntityType.TOKEN_TRANSFER)

        if entity_type == EntityType.TOKEN_TRANSFER:
            return EntityType.TOKEN_TRANSFER in self.entity_types

        if entity_type == EntityType.CONTRACT:
            return EntityType.CONTRACT in self.entity_types or self._should_export(EntityType.TOKEN)

        raise ValueError(f"Unexpected entity type {entity_type.value}")


def _sort_by(arr: List[Any], fields: Union[str, Tuple[str, ...]]) -> List[Any]:
    if isinstance(fields, tuple):
        fields = tuple(fields)

    def get_key(item, f):
        if hasattr(item, "get"):
            return item.get(f)
        return getattr(item, f, None)

    return sorted(arr, key=lambda item: tuple(get_key(item, f) for f in fields))
