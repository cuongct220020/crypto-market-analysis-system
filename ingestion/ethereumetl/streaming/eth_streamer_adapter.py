import asyncio
from typing import List, Any, Tuple, Union
from web3 import AsyncWeb3

from config.settings import settings
from ingestion.blockchainetl.exporters.console_exporter import ConsoleItemExporter
from ingestion.ethereumetl.enums.entity_type import EntityType
from ingestion.ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ingestion.ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ingestion.ethereumetl.jobs.export_traces_job import ExportTracesJob
from ingestion.ethereumetl.jobs.extract_contracts_job import ExtractContractsJob
from ingestion.ethereumetl.jobs.extract_token_transfers_job import ExtractTokenTransfersJob
from ingestion.ethereumetl.jobs.export_tokens_job import ExportTokensJob
from ingestion.ethereumetl.providers.provider_factory import get_async_provider_from_uri
from ingestion.ethereumetl.streaming.enrich import (
    enrich_contracts,
    enrich_logs,
    enrich_token_transfers,
    enrich_tokens,
    enrich_traces,
    enrich_transactions,
)
from utils.logger_utils import get_logger
from utils.web3_utils import build_async_web3

from ingestion.ethereumetl.models.block import EthBlock
from ingestion.ethereumetl.models.transaction import EthTransaction, EnrichedEthTransaction
from ingestion.ethereumetl.models.receipt import EthReceipt
from ingestion.ethereumetl.models.receipt_log import EthReceiptLog, EnrichedEthReceiptLog
from ingestion.ethereumetl.models.token_transfer import EthTokenTransfer, EnrichedEthTokenTransfer
from ingestion.ethereumetl.models.trace import EthTrace, EnrichedEthTrace
from ingestion.ethereumetl.models.contract import EthContract, EnrichedEthContract
from ingestion.ethereumetl.models.token import EthToken, EnrichedEthToken

logger = get_logger(__name__)

class InMemoryItemBuffer:
    def __init__(self, item_types):
        self.item_types = item_types
        self.items = {}

    def export_item(self, item):
        item_type = item.get("type", None)
        if item_type is None:
            raise ValueError("type key is not found in item {}".format(repr(item)))

        self.items[item_type].append(item)

    def get_items(self, item_type):
        return self.items[item_type]


class EthStreamerAdapter:
    def __init__(
        self,
        item_exporter: Any = ConsoleItemExporter(),
        entity_types: Tuple[EntityType, ...] = tuple(EntityType.ALL_FOR_STREAMING),
        batch_size: int = settings.ethereum.batch_size,
        max_workers: int = settings.ethereum.max_workers,
    ):
        self.item_exporter = item_exporter
        self.entity_types = entity_types
        self.batch_size = batch_size
        self.max_workers = max_workers

        # Build AsyncWeb3 provider
        self._async_provider = get_async_provider_from_uri(settings.ethereum.provider_uri, timeout=settings.ethereum.rpc_timeout)
        self.w3 = build_async_web3(self._async_provider)

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
            receipts, logs = await self._export_receipts_and_logs(transactions)

        # Extract token transfers
        token_transfers: List[EthTokenTransfer] = []
        if self._should_export(EntityType.TOKEN_TRANSFER):
            token_transfers = await self._extract_token_transfers(logs)

        # Export traces
        traces: List[EthTrace] = []
        if self._should_export(EntityType.TRACE):
            traces = await self._export_traces(start_block, end_block)

        # Export contracts
        contracts: List[EthContract] = []
        if self._should_export(EntityType.CONTRACT):
            contracts = await self._export_contracts(traces)

        # Export tokens
        tokens: List[EthToken] = []
        if self._should_export(EntityType.TOKEN):
            tokens = await self._extract_tokens(contracts)

        enriched_blocks: List[EthBlock] = blocks if EntityType.BLOCK in self.entity_types else []
        enriched_transactions: List[EnrichedEthTransaction] = (
            enrich_transactions(transactions, receipts) if EntityType.TRANSACTION in self.entity_types else []
        )
        enriched_logs: List[EnrichedEthReceiptLog] = enrich_logs(blocks, logs) if EntityType.LOG in self.entity_types else []
        enriched_token_transfers: List[EnrichedEthTokenTransfer] = (
            enrich_token_transfers(blocks, token_transfers) if EntityType.TOKEN_TRANSFER in self.entity_types else []
        )
        enriched_traces: List[EnrichedEthTrace] = enrich_traces(blocks, traces) if EntityType.TRACE in self.entity_types else []
        enriched_contracts: List[EnrichedEthContract] = enrich_contracts(blocks, contracts) if EntityType.CONTRACT in self.entity_types else []
        enriched_tokens: List[EnrichedEthToken] = enrich_tokens(blocks, tokens) if EntityType.TOKEN in self.entity_types else []

        logger.info("Exporting with " + type(self.item_exporter).__name__)

        all_items: List[Any] = (
            _sort_by(enriched_blocks, "number")
            + _sort_by(enriched_transactions, ("block_number", "transaction_index"))
            + _sort_by(enriched_logs, ("block_number", "log_index"))
            + _sort_by(enriched_token_transfers, ("block_number", "log_index"))
            + _sort_by(enriched_traces, ("block_number", "trace_index"))
            + _sort_by(enriched_contracts, ("block_number",))
            + _sort_by(enriched_tokens, ("block_number",))
        )

        self.item_exporter.export_items(all_items)

    async def _export_blocks_and_transactions(self, start_block: int, end_block: int) -> Tuple[List[EthBlock], List[EthTransaction]]:
        exporter = InMemoryItemBuffer(item_types=["block", "transaction"])
        job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            web3=self.w3,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_blocks=self._should_export(EntityType.BLOCK),
            export_transactions=self._should_export(EntityType.TRANSACTION),
        )
        await job.run()
        blocks = exporter.get_items("block")
        transactions = exporter.get_items("transaction")
        return blocks, transactions

    async def _export_receipts_and_logs(self, transactions: List[EthTransaction]) -> Tuple[List[EthReceipt], List[EthReceiptLog]]:
        exporter = InMemoryItemBuffer(item_types=["receipt", "log"])
        job = ExportReceiptsJob(
            transaction_hashes_iterable=(transaction.hash for transaction in transactions if transaction.hash),
            batch_size=self.batch_size,
            web3=self.w3,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_receipts=self._should_export(EntityType.RECEIPT),
            export_logs=self._should_export(EntityType.LOG),
        )
        await job.run()
        receipts = exporter.get_items("receipt")
        logs = exporter.get_items("log")
        return receipts, logs

    @staticmethod
    async def _extract_token_transfers(logs: List[EthReceiptLog]) -> List[EthTokenTransfer]:
        exporter = InMemoryItemBuffer(item_types=["token_transfer"])
        job = ExtractTokenTransfersJob(
            logs_iterable=logs,
            item_exporter=exporter,
        )
        # CPU bound, run in thread
        await asyncio.to_thread(job.run)
        token_transfers = exporter.get_items("token_transfer")
        return token_transfers

    async def _export_traces(self, start_block: int, end_block: int) -> List[EthTrace]:
        exporter = InMemoryItemBuffer(item_types=["trace"])
        job = ExportTracesJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            web3=self.w3,
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        await job.run()
        traces = exporter.get_items("trace")
        return traces

    @staticmethod
    async def _export_contracts(traces: List[EthTrace]) -> List[EthContract]:
        exporter = InMemoryItemBuffer(item_types=["contract"])
        job = ExtractContractsJob(
            traces_iterable=traces,
            item_exporter=exporter,
        )
        # CPU bound
        await asyncio.to_thread(job.run)
        contracts = exporter.get_items("contract")
        return contracts

    async def _extract_tokens(self, contracts: List[EthContract]) -> List[EthToken]:
        exporter = InMemoryItemBuffer(item_types=["token"])

        # Filter tokens from contracts (ERC20 or ERC721)
        tokens_iterable = [contract for contract in contracts if contract.is_erc20 or contract.is_erc721]

        job = ExportTokensJob(
            token_addresses_iterable=[{"address": c.address, "block_number": c.block_number} for c in tokens_iterable if c.address],
            web3=self.w3,
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        await job.run()
        tokens = exporter.get_items("token")
        return tokens


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

        if entity_type == EntityType.TRACE:
            return EntityType.TRACE in self.entity_types or self._should_export(EntityType.CONTRACT)

        if entity_type == EntityType.CONTRACT:
            return EntityType.CONTRACT in self.entity_types or self._should_export(EntityType.TOKEN)

        if entity_type == EntityType.TOKEN:
            return EntityType.TOKEN in self.entity_types

        raise ValueError(f"Unexpected entity type {entity_type.value}")


def _sort_by(arr: List[Any], fields: Union[str, Tuple[str, ...]]) -> List[Any]:
    if isinstance(fields, tuple):
        fields = tuple(fields)
    return sorted(arr, key=lambda item: tuple(item.get(f) for f in fields))
