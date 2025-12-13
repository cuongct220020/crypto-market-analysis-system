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
# Change Description: Refactored to coordinate work using BlockExporterExecutor.


from typing import Any

from web3 import AsyncWeb3

from ingestion.blockchainetl.jobs.async_base_job import AsyncBaseJob
from ingestion.ethereumetl.executors.block_exporter_executor import BlockExporterExecutor
from utils.validation_utils import validate_block_range
from utils.logger_utils import get_logger

logger = get_logger("Export Blocks Job")


class ExportBlocksJob(AsyncBaseJob):
    def __init__(
        self,
        start_block: int,
        end_block: int,
        batch_size: int = 5,
        web3: AsyncWeb3 = None,
        max_workers: int = 5,
        item_exporter: Any = None,
        max_concurrent_requests: int = 5,
        export_blocks: bool = True,
        export_transactions: bool = True,
    ):
        validate_block_range(start_block, end_block)

        self.start_block = start_block
        self.end_block = end_block if end_block is not None else start_block
        
        if not export_blocks and not export_transactions:
            raise ValueError("At least one of export_blocks or export_transactions must be True")

        # Initialize the Executor
        self.executor = BlockExporterExecutor(
            web3=web3,
            item_exporter=item_exporter,
            batch_size=batch_size,
            max_workers=max_workers,
            max_concurrent_requests=max_concurrent_requests,
            export_blocks=export_blocks,
            export_transactions=export_transactions,
        )

    async def _start(self) -> None:
        logger.info(f"Starting ExportBlocksJob from {self.start_block} to {self.end_block}")

    async def _export(self) -> None:
        await self.executor.execute(self.start_block, self.end_block)

    async def _end(self) -> None:
        logger.info("ExportBlocksJob completed successfully")