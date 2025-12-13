from typing import List, Any

from web3 import AsyncWeb3
from web3.types import BlockData

from ingestion.ethereumetl.scheduler.work_scheduler import WorkScheduler
from ingestion.ethereumetl.mappers.block_mapper import EthBlockMapper
from ingestion.ethereumetl.mappers.transaction_mapper import EthTransactionMapper
from utils.async_utils import gather_with_concurrency
from utils.logger_utils import get_logger

logger = get_logger("Block Exporter Executor")


class BlockExporterExecutor:
    """
    Executor responsible for fetching blocks and transactions from RPC and exporting them.
    """
    def __init__(
        self,
        web3: AsyncWeb3,
        item_exporter: Any,
        batch_size: int,
        max_workers: int,
        max_concurrent_requests: int,
        export_blocks: bool = True,
        export_transactions: bool = True,
    ):
        self.web3 = web3
        self.item_exporter = item_exporter
        self.max_concurrent_requests = max_concurrent_requests
        self.export_blocks = export_blocks
        self.export_transactions = export_transactions
        
        # Mappers
        self.block_mapper = EthBlockMapper()
        self.transaction_mapper = EthTransactionMapper()
        
        # Scheduler (The Engine)
        self.work_scheduler = WorkScheduler(batch_size, max_workers)

    async def execute(self, start_block: int, end_block: int) -> None:
        """
        Main entry point to execute the block export logic.
        """
        logger.info(f"Executing block export pipeline from {start_block} to {end_block}")
        self.item_exporter.open()
        
        try:
            # Execute pipeline: Fetch -> Process
            await self.work_scheduler.execute_pipeline(
                work_iterable=range(start_block, end_block + 1),
                fetch_handler=self._fetch_batch_worker,
                process_handler=self._process_batch_worker,
            )
        finally:
            self.work_scheduler.shutdown()
            self.item_exporter.close()

    async def _fetch_batch_worker(self, block_number_batch: List[int]) -> List[BlockData]:
        """
        Worker: Fetch blocks concurrently from RPC.
        """
        tasks = [
            self.web3.eth.get_block(block_num, full_transactions=self.export_transactions)
            for block_num in block_number_batch
        ]
        return await gather_with_concurrency(self.max_concurrent_requests, *tasks)

    async def _process_batch_worker(self, blocks_data: List[BlockData]) -> None:
        """
        Worker: Map and Export data.
        """
        for block_data in blocks_data:
            self._export_block(block_data)

    def _export_block(self, block_data: BlockData) -> None:
        block = self.block_mapper.web3_dict_to_block(block_data)

        if self.export_blocks:
            self.item_exporter.export_item(block)

        if self.export_transactions and block.transactions:
            for tx in block.transactions:
                self.item_exporter.export_item(tx)
