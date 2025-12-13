from typing import List, Any, Dict

from hexbytes import HexBytes
from web3 import AsyncWeb3

from ingestion.ethereumetl.scheduler.work_scheduler import WorkScheduler
from ingestion.ethereumetl.mappers.receipt_log_mapper import EthReceiptLogMapper
from ingestion.ethereumetl.mappers.receipt_mapper import EthReceiptMapper
from ingestion.ethereumetl.models.receipt import EthReceipt
from utils.async_utils import gather_with_concurrency
from utils.logger_utils import get_logger

logger = get_logger("Receipt Exporter Executor")


class ReceiptExporterExecutor:
    """
    Executor responsible for fetching receipts and logs from RPC and exporting them.
    """
    def __init__(
        self,
        web3: AsyncWeb3,
        item_exporter: Any,
        batch_size: int,
        max_workers: int,
        max_concurrent_requests: int,
        export_receipts: bool = True,
        export_logs: bool = True,
    ):
        self.web3 = web3
        self.item_exporter = item_exporter
        self.max_concurrent_requests = max_concurrent_requests
        self.export_receipts = export_receipts
        self.export_logs = export_logs
        
        # Mappers
        self.receipt_mapper = EthReceiptMapper()
        self.receipt_log_mapper = EthReceiptLogMapper()
        
        # Scheduler
        self.work_scheduler = WorkScheduler(batch_size, max_workers)

    async def execute(self, start_block: int, end_block: int) -> None:
        logger.info(f"Executing receipt export pipeline from {start_block} to {end_block}")
        self.item_exporter.open()
        
        try:
            # We use 'execute' here instead of 'execute_pipeline' because the original logic
            # fetched AND processed in a single batch step (fetch_block_receipts -> export_receipt).
            # However, to be consistent with the pipeline pattern, let's try to separate if possible.
            # But the original code was: "for block in batch: fetch -> export".
            # Let's adapt to pipeline: Fetch (Batch of Blocks -> List[List[Receipts]]) -> Process (List[List[Receipts]] -> Export)
            
            await self.work_scheduler.execute_pipeline(
                work_iterable=range(start_block, end_block + 1),
                fetch_handler=self._fetch_batch_worker,
                process_handler=self._process_batch_worker,
            )
        finally:
            self.work_scheduler.shutdown()
            self.item_exporter.close()

    async def _fetch_batch_worker(self, block_number_batch: List[int]):
        """
        Worker: Fetch receipts for a batch of blocks concurrently.
        """
        tasks = [
            self._fetch_block_receipts(block_number)
            for block_number in block_number_batch
        ]
        # Result is List[List[TxReceipt]]
        return await gather_with_concurrency(self.max_concurrent_requests, *tasks)

    async def _process_batch_worker(self, batch_receipts_lists) -> None:
        """
        Worker: Flatten and export receipts.
        """
        for receipts in batch_receipts_lists:
            if receipts:
                for receipt_data in receipts:
                    self._export_receipt(receipt_data)

    async def _fetch_block_receipts(self, block_number: int):
        try:
            response = await self.web3.provider.make_request("eth_getBlockReceipts", [hex(block_number)])
            if "result" in response and response["result"] is not None:
                logger.info(f"Fetched block receipts for block number {block_number}")
                receipts = response["result"]
                return receipts
            else:
                logger.warning(f"Failed to fetch receipts for block {block_number}: {response}")
                return []
        except Exception as e:
            logger.error(f"Error fetching receipts for block {block_number}: {e}")
            return []

    def _export_receipt(self, receipt_data: Dict[str, Any]) -> None:
        receipt = self.receipt_mapper.json_dict_to_receipt(receipt_data)

        if self.export_receipts:
            self.item_exporter.export_item(receipt)

        if self.export_logs and receipt.logs:
            for log in receipt.logs:
                self.item_exporter.export_item(log)
