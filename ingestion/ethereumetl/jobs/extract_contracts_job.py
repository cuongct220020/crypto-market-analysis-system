# MIT License
#
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
# Change Description: Refactored to coordinate work using ContractExtractorExecutor with caching.


from typing import Any, List

from web3 import AsyncWeb3

from ingestion.blockchainetl.jobs.async_base_job import AsyncBaseJob
from ingestion.ethereumetl.executors.contract_extractor_executor import ContractExtractorExecutor
from ingestion.ethereumetl.models.receipt_log import EthReceiptLog
from utils.caching_utils import InMemoryDedupStore
from utils.logger_utils import get_logger

logger = get_logger("Extract Contracts Job")


# Extract contracts
class ExtractContractsJob(AsyncBaseJob):
    def __init__(
        self,
        receipt_logs: List[EthReceiptLog],
        item_exporter: Any,
        web3: AsyncWeb3,
        max_workers: int = 5,
    ):
        self.receipt_logs = receipt_logs
        
        self.executor = ContractExtractorExecutor(
            web3=web3,
            item_exporter=item_exporter,
            batch_size=10,
            max_workers=max_workers
        )
        
        # In-memory deduplication store
        self.dedup_store = InMemoryDedupStore[str]()

    async def _start(self) -> None:
        logger.info("Starting ExtractContractsJob...")

    async def _export(self) -> None:
        # Step 1: Extract all addresses from logs
        all_addresses = [log.address for log in self.receipt_logs if log.address]
        
        # Step 2: Filter unique addresses using cache
        unique_addresses = self.dedup_store.filter_new_items(all_addresses)
        
        logger.info(f"Found {len(all_addresses)} total log addresses, {len(unique_addresses)} unique new to fetch.")

        # Step 3: Execute metadata fetching
        if unique_addresses:
            await self.executor.execute(unique_addresses)

    async def _end(self) -> None:
        logger.info("ExtractContractsJob completed successfully")
