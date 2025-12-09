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


import asyncio
from typing import List, Any

from web3 import AsyncWeb3
from web3.types import BlockData

from ingestion.blockchainetl.jobs.async_base_job import AsyncBaseJob
from ingestion.ethereumetl.executors.async_batch_work_executor import AsyncBatchWorkExecutor
from ingestion.ethereumetl.mappers.block_mapper import EthBlockMapper
from ingestion.ethereumetl.mappers.transaction_mapper import EthTransactionMapper


def _validate_range(range_start_incl: int, range_end_incl: int) -> None:
    if range_start_incl < 0 or range_end_incl < 0:
        raise ValueError("range_start and range_end must be greater or equal to 0")

    if range_end_incl < range_start_incl:
        raise ValueError("range_end must be greater or equal to range_start")


class ExportBlocksJob(AsyncBaseJob):
    def __init__(
        self,
        start_block: int,
        end_block: int,
        batch_size: int,
        web3: AsyncWeb3,
        max_workers: int,
        item_exporter: Any,
        export_blocks: bool = True,
        export_transactions: bool = True,
    ):
        _validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.web3 = web3
        self.item_exporter = item_exporter

        self.export_blocks = export_blocks
        self.export_transactions = export_transactions
        if not self.export_blocks and not self.export_transactions:
            raise ValueError("At least one of export_blocks or export_transactions must be True")

        self.batch_work_executor = AsyncBatchWorkExecutor(batch_size, max_workers)

        self.block_mapper = EthBlockMapper()
        self.transaction_mapper = EthTransactionMapper()

    async def _start(self) -> None:
        self.item_exporter.open()

    async def _export(self) -> None:
        await self.batch_work_executor.execute_pipeline(
            range(self.start_block, self.end_block + 1),
            self._fetch_batch,
            self._process_batch,
        )

    async def _fetch_batch(self, block_number_batch: List[int]) -> List[BlockData]:
        # Async IO: Fetch blocks concurrently
        tasks = [
            self.web3.eth.get_block(block_num, full_transactions=self.export_transactions)
            for block_num in block_number_batch
        ]
        return await asyncio.gather(*tasks)

    async def _process_batch(self, blocks_data: List[BlockData]) -> None:
        # CPU Bound: Map and Export
        for block_data in blocks_data:
            self._export_block(block_data)

    def _export_block(self, block_data: BlockData) -> None:
        # Map Web3 AttributeDict to Pydantic Model using the new web3_dict_to_block method
        block = self.block_mapper.web3_dict_to_block(block_data)

        if self.export_blocks:
            self.item_exporter.export_item(block)

        if self.export_transactions and block.transactions:
            for tx in block.transactions:
                self.item_exporter.export_item(tx)

    async def _end(self) -> None:
        self.batch_work_executor.shutdown()
        self.item_exporter.close()