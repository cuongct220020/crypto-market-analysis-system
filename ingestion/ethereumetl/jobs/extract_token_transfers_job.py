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
# Change Description: Refactored to coordinate work using TokenTransferExtractorExecutor and ContractExtractorExecutor with caching.

from typing import Any, List

from ingestion.blockchainetl.jobs.base_job import BaseJob
from ingestion.ethereumetl.executors.token_transfer_extractor_executor import TokenTransferExtractorExecutor
from ingestion.ethereumetl.models.receipt_log import EthReceiptLog
from utils.logger_utils import get_logger

logger = get_logger("Extract Token Transfers Job")


class ExtractTokenTransfersJob(BaseJob):
    def __init__(
        self,
        receipt_logs: List[EthReceiptLog],
        item_exporter: Any
    ):
        self.receipt_logs = receipt_logs
        
        # Executor for parsing transfers (CPU bound)
        self.token_executor = TokenTransferExtractorExecutor(item_exporter=item_exporter)

        # # In-memory deduplication store for the scope of this job execution
        # # This prevents fetching metadata for the same token address multiple times in the same batch
        # self.dedup_store = InMemoryDedupStore[str]()

    def _start(self) -> None:
        logger.info("Starting ExtractTokenTransfersJob...")

    def _export(self) -> None:
        # Step 1: Extract Transfers
        transfers = self.token_executor.execute(self.receipt_logs)
        logger.info(f"Extracted {len(transfers)} token transfers successfully!")
        
        # # Step 2: Extract Unique Token Addresses for Metadata Fetching
        # # Filter out duplicates using the dedup store
        # all_token_addresses = [t.token_address for t in transfers if t.token_address]
        # unique_new_addresses = self.dedup_store.filter_new_items(all_token_addresses)
        #
        # logger.info(f"Found {len(all_token_addresses)} total token addresses, {len(unique_new_addresses)} unique new to fetch.")


    def _end(self) -> None:
        logger.info("ExtractTokenTransfersJob completed successfully")