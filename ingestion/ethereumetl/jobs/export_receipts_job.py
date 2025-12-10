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


from typing import Any, Iterable, List

from hexbytes import HexBytes
from web3 import AsyncWeb3
from web3.types import TxReceipt

from ingestion.blockchainetl.jobs.async_base_job import AsyncBaseJob
from ingestion.ethereumetl.executors.async_batch_work_executor import AsyncBatchWorkExecutor
from ingestion.ethereumetl.mappers.receipt_log_mapper import EthReceiptLogMapper
from ingestion.ethereumetl.mappers.receipt_mapper import EthReceiptMapper
from utils.async_utils import gather_with_concurrency
from utils.logger_utils import get_logger

logger = get_logger("Export Receipts Job")


# Exports receipts and logs
class ExportReceiptsJob(AsyncBaseJob):
    def __init__(
        self,
        transaction_hashes_iterable: Iterable[str],
        batch_size: int,
        web3: AsyncWeb3,
        max_workers: int,
        item_exporter: Any,
        max_concurrent_requests: int = 5,
        export_receipts: bool = True,
        export_logs: bool = True,
    ):
        self.transaction_hashes_iterable = transaction_hashes_iterable
        self.web3 = web3
        self.item_exporter = item_exporter
        self.max_concurrent_requests = max_concurrent_requests

        self.export_receipts = export_receipts
        self.export_logs = export_logs
        if not self.export_receipts and not self.export_logs:
            raise ValueError("At least one of export_receipts or export_logs must be True")

        self.batch_work_executor = AsyncBatchWorkExecutor(batch_size, max_workers)

        self.receipt_mapper = EthReceiptMapper()
        self.receipt_log_mapper = EthReceiptLogMapper()

    async def _start(self) -> None:
        self.item_exporter.open()

    async def _export(self) -> None:
        await self.batch_work_executor.execute(self.transaction_hashes_iterable, self._export_batch)

    async def _export_batch(self, transaction_hashes: List[HexBytes]) -> None:
        # Async IO: Fetch receipts concurrently
        # Convert str hashes to HexBytes as expected by web3.py
        tasks = []
        for tx_hash in transaction_hashes:
            try:
                tasks.append(self.web3.eth.get_transaction_receipt(HexBytes(tx_hash)))
            except Exception as e:
                # Log invalid hash, but don't stop the batch.
                logger.error(f"Invalid transaction hash '{tx_hash}': {e}")
                # Create a dummy task that immediately returns None to keep the gather_with_concurrency happy
                tasks.append(self._async_return_none())

        receipts_data = await gather_with_concurrency(self.max_concurrent_requests, *tasks)

        for receipt_data in receipts_data:
            if receipt_data is not None:  # Only process valid receipts
                self._export_receipt(receipt_data)
            else:
                # This case is handled by the error logging above when adding tasks
                # or if get_transaction_receipt itself returns None (which it shouldn't for existing txs)
                pass

    @staticmethod
    async def _async_return_none():
        return None

    def _export_receipt(self, receipt_data: TxReceipt) -> None:
        # Map Web3 AttributeDict to Pydantic Model
        receipt = self.receipt_mapper.web3_dict_to_receipt(receipt_data)

        if self.export_receipts:
            self.item_exporter.export_item(receipt)

        if self.export_logs and receipt.logs:
            for log in receipt.logs:
                self.item_exporter.export_item(log)

    async def _end(self) -> None:
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
