from config.settings import settings  # NEW: Import settings
from ingestion.blockchainetl.jobs.console_item_exporter import ConsoleItemExporter
from ingestion.blockchainetl.jobs.in_memory_item_exporter import InMemoryItemExporter
from ingestion.ethereumetl.enumeration.entity_type import EntityType
from ingestion.ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ingestion.ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ingestion.ethereumetl.jobs.export_traces_job import ExportTracesJob
from ingestion.ethereumetl.jobs.extract_contracts_job import ExtractContractsJob
from ingestion.ethereumetl.jobs.extract_token_transfers_job import ExtractTokenTransfersJob
from ingestion.ethereumetl.jobs.extract_tokens_job import ExtractTokensJob
from ingestion.ethereumetl.providers.auto import get_provider_from_uri  # Move this import up
from ingestion.ethereumetl.streaming.enrich import (
    enrich_contracts,
    enrich_logs,
    enrich_token_transfers,
    enrich_tokens,
    enrich_traces,
    enrich_transactions,
)
from ingestion.ethereumetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from ingestion.ethereumetl.streaming.eth_item_timestamp_calculator import EthItemTimestampCalculator
from utils.logger_utils import get_logger
from utils.thread_utils import ThreadLocalProxy
from utils.web3_utils import build_web3

logger = get_logger(__name__)


class EthStreamerAdapter:
    def __init__(self, item_exporter=ConsoleItemExporter(), entity_types=tuple(EntityType.ALL_FOR_STREAMING)):
        self.item_exporter = item_exporter
        self.entity_types = entity_types
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()

        # Build web3 provider directly from settings here
        self._batch_web3_provider = ThreadLocalProxy(
            lambda: get_provider_from_uri(settings.provider_uri, timeout=settings.rpc_timeout, batch=True)
        )

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self):
        w3 = build_web3(self._batch_web3_provider)  # Use internal provider
        return int(w3.eth.get_block("latest").number)

    def export_all(self, start_block, end_block):
        # Export blocks and transactions
        blocks, transactions = [], []
        if self._should_export(EntityType.BLOCK) or self._should_export(EntityType.TRANSACTION):
            blocks, transactions = self._export_blocks_and_transactions(start_block, end_block)

        # Export receipts and logs
        receipts, logs = [], []
        if self._should_export(EntityType.RECEIPT) or self._should_export(EntityType.LOG):
            receipts, logs = self._export_receipts_and_logs(transactions)

        # Extract token transfers
        token_transfers = []
        if self._should_export(EntityType.TOKEN_TRANSFER):
            token_transfers = self._extract_token_transfers(logs)

        # Export traces
        traces = []
        if self._should_export(EntityType.TRACE):
            traces = self._export_traces(start_block, end_block)

        # Export contracts
        contracts = []
        if self._should_export(EntityType.CONTRACT):
            contracts = self._export_contracts(traces)

        # Export tokens
        tokens = []
        if self._should_export(EntityType.TOKEN):
            tokens = self._extract_tokens(contracts)

        enriched_blocks = blocks if EntityType.BLOCK in self.entity_types else []
        enriched_transactions = (
            enrich_transactions(transactions, receipts) if EntityType.TRANSACTION in self.entity_types else []
        )
        enriched_logs = enrich_logs(blocks, logs) if EntityType.LOG in self.entity_types else []
        enriched_token_transfers = (
            enrich_token_transfers(blocks, token_transfers) if EntityType.TOKEN_TRANSFER in self.entity_types else []
        )
        enriched_traces = enrich_traces(blocks, traces) if EntityType.TRACE in self.entity_types else []
        enriched_contracts = enrich_contracts(blocks, contracts) if EntityType.CONTRACT in self.entity_types else []
        enriched_tokens = enrich_tokens(blocks, tokens) if EntityType.TOKEN in self.entity_types else []

        logger.info("Exporting with " + type(self.item_exporter).__name__)

        all_items = (
            sort_by(enriched_blocks, "number")
            + sort_by(enriched_transactions, ("block_number", "transaction_index"))
            + sort_by(enriched_logs, ("block_number", "log_index"))
            + sort_by(enriched_token_transfers, ("block_number", "log_index"))
            + sort_by(enriched_traces, ("block_number", "trace_index"))
            + sort_by(enriched_contracts, ("block_number",))
            + sort_by(enriched_tokens, ("block_number",))
        )

        self.calculate_item_ids(all_items)
        self.calculate_item_timestamps(all_items)

        self.item_exporter.export_items(all_items)

    def _export_blocks_and_transactions(self, start_block, end_block):
        exporter = InMemoryItemExporter(item_types=["block", "transaction"])
        job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=settings.batch_size,  # Use settings
            batch_web3_provider=self._batch_web3_provider,  # Use internal provider
            max_workers=settings.max_workers,  # Use settings
            item_exporter=exporter,
            export_blocks=self._should_export(EntityType.BLOCK),
            export_transactions=self._should_export(EntityType.TRANSACTION),
        )
        job.run()
        blocks = exporter.get_items("block")
        transactions = exporter.get_items("transaction")
        return blocks, transactions

    def _export_receipts_and_logs(self, transactions):
        exporter = InMemoryItemExporter(item_types=["receipt", "log"])
        job = ExportReceiptsJob(
            transaction_hashes_iterable=(transaction["hash"] for transaction in transactions),
            batch_size=settings.batch_size,  # Use settings
            batch_web3_provider=self._batch_web3_provider,  # Use internal provider
            max_workers=settings.max_workers,  # Use settings
            item_exporter=exporter,
            export_receipts=self._should_export(EntityType.RECEIPT),
            export_logs=self._should_export(EntityType.LOG),
        )
        job.run()
        receipts = exporter.get_items("receipt")
        logs = exporter.get_items("log")
        return receipts, logs

    def _extract_token_transfers(self, logs):
        exporter = InMemoryItemExporter(item_types=["token_transfer"])
        job = ExtractTokenTransfersJob(
            logs_iterable=logs,
            batch_size=settings.batch_size,  # Use settings
            max_workers=settings.max_workers,  # Use settings
            item_exporter=exporter,
        )
        job.run()
        token_transfers = exporter.get_items("token_transfer")
        return token_transfers

    def _export_traces(self, start_block, end_block):
        exporter = InMemoryItemExporter(item_types=["trace"])
        job = ExportTracesJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=settings.batch_size,  # Use settings
            web3=ThreadLocalProxy(lambda: build_web3(self._batch_web3_provider)),  # Use internal provider
            max_workers=settings.max_workers,  # Use settings
            item_exporter=exporter,
        )
        job.run()
        traces = exporter.get_items("trace")
        return traces

    def _export_contracts(self, traces):
        exporter = InMemoryItemExporter(item_types=["contract"])
        job = ExtractContractsJob(
            traces_iterable=traces,
            batch_size=settings.batch_size,  # Use settings
            max_workers=settings.max_workers,  # Use settings
            item_exporter=exporter,
        )
        job.run()
        contracts = exporter.get_items("contract")
        return contracts

    def _extract_tokens(self, contracts):
        exporter = InMemoryItemExporter(item_types=["token"])
        job = ExtractTokensJob(
            contracts_iterable=contracts,
            web3=ThreadLocalProxy(lambda: build_web3(self._batch_web3_provider)),  # Use internal provider
            max_workers=settings.max_workers,  # Use settings
            item_exporter=exporter,
        )
        job.run()
        tokens = exporter.get_items("token")
        return tokens

    def _should_export(self, entity_type):
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

        raise ValueError("Unexpected entity type " + entity_type)

    def calculate_item_ids(self, items):
        for item in items:
            item["item_id"] = self.item_id_calculator.calculate(item)

    def calculate_item_timestamps(self, items):
        for item in items:
            item["item_timestamp"] = self.item_timestamp_calculator.calculate(item)

    def close(self):
        self.item_exporter.close()


def sort_by(arr, fields):
    if isinstance(fields, tuple):
        fields = tuple(fields)
    return sorted(arr, key=lambda item: tuple(item.get(f) for f in fields))
