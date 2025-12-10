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
# Change Description: Refactor to Async Job using Web3.py V6+ and Pydantic Models


from typing import Any, List

from web3 import AsyncWeb3
from web3.types import BlockData

from ingestion.blockchainetl.jobs.async_base_job import AsyncBaseJob
from ingestion.ethereumetl.executors.async_batch_work_executor import AsyncBatchWorkExecutor
from ingestion.ethereumetl.mappers.block_mapper import EthBlockMapper
from ingestion.ethereumetl.mappers.transaction_mapper import EthTransactionMapper
from utils.validation_utils import validate_block_range
from utils.async_utils import gather_with_concurrency
from utils.logger_utils import get_logger

logger = get_logger("Export Blocks Job")


class ExportBlocksJob(AsyncBaseJob):
    def __init__(
        self,
        start_block: int,
        end_block: int,
        batch_size: int,
        web3: AsyncWeb3,
        max_workers: int,
        item_exporter: Any,
        max_concurrent_requests: int = 5,
        export_blocks: bool = True,
        export_transactions: bool = True,
    ):
        validate_block_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.web3 = web3
        self.item_exporter = item_exporter
        self.max_concurrent_requests = max_concurrent_requests

        self.export_blocks = export_blocks
        self.export_transactions = export_transactions
        if not self.export_blocks and not self.export_transactions:
            raise ValueError("At least one of export_blocks or export_transactions must be True")

        self.batch_work_executor = AsyncBatchWorkExecutor(batch_size, max_workers)

        self.block_mapper = EthBlockMapper()
        self.transaction_mapper = EthTransactionMapper()

    async def _start(self) -> None:
        logger.info(f"Starting export of blocks from {self.start_block} to {self.end_block}")
        self.item_exporter.open()

    async def _export(self) -> None:
        logger.info(f"Exporting {self.end_block - self.start_block + 1} blocks from {self.start_block} to {self.end_block}")
        await self.batch_work_executor.execute_pipeline(
            range(self.start_block, self.end_block + 1),
            self._fetch_batch,
            self._process_batch,
        )

    async def _fetch_batch(self, block_number_batch: List[int]) -> List[BlockData]:
        # logger.debug(f"Fetching batch of {len(block_number_batch)} blocks: {block_number_batch[0]} to {block_number_batch[-1]}")
        # Async IO: Fetch blocks concurrently
        tasks = [
            self.web3.eth.get_block(block_num, full_transactions=self.export_transactions)
            for block_num in block_number_batch
        ]
        # Use gather_with_concurrency to limit concurrent requests
        blocks_data = await gather_with_concurrency(self.max_concurrent_requests, *tasks)
        # logger.debug(f"Successfully fetched {len(blocks_data)} blocks in batch")
        return blocks_data

    async def _process_batch(self, blocks_data: List[BlockData]) -> None:
        # CPU Bound: Map and Export
        # logger.debug(f"Processing batch of {len(blocks_data)} blocks")
        for block_data in blocks_data:
            self._export_block(block_data)
        # logger.debug(f"Completed processing batch of {len(blocks_data)} blocks")

    def _export_block(self, block_data: BlockData) -> None:
        # Map Web3 AttributeDict to Pydantic Model using the new web3_dict_to_block method
        block = self.block_mapper.web3_dict_to_block(block_data)
        # logger.debug(f"Exporting block #{block.number} with {len(block.transactions) if block.transactions else 0} transactions")

        if self.export_blocks:
            self.item_exporter.export_item(block)

        if self.export_transactions and block.transactions:
            for tx in block.transactions:
                self.item_exporter.export_item(tx)

    async def _end(self) -> None:
        logger.info("Shutting down ExportBlocksJob resources...")
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
        logger.info("ExportBlocksJob completed successfully")
