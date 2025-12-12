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
        start_block: int,
        end_block: int,
        batch_size: int,
        web3: AsyncWeb3,
        max_workers: int,
        item_exporter: Any,
        max_concurrent_requests: int = 5,
        export_receipts: bool = True,
        export_logs: bool = True,
    ):
        self.start_block = start_block
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
        await self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1), self._export_batch
        )

    async def _export_batch(self, block_numbers: List[int]) -> None:
        tasks = []
        for block_number in block_numbers:
            tasks.append(self._fetch_block_receipts(block_number))

        results = await gather_with_concurrency(self.max_concurrent_requests, *tasks)

        for receipts in results:
            if receipts:
                for receipt_data in receipts:
                    self._export_receipt(receipt_data)

    async def _fetch_block_receipts(self, block_number: int) -> List[TxReceipt]:
        try:
            response = await self.web3.provider.make_request("eth_getBlockReceipts", [hex(block_number)])
            if "result" in response and response["result"] is not None:
                return [self._sanitize_receipt(r) for r in response["result"]]
            else:
                logger.warning(f"Failed to fetch receipts for block {block_number}: {response}")
                return []
        except Exception as e:
            logger.error(f"Error fetching receipts for block {block_number}: {e}")
            return []

    def _sanitize_receipt(self, receipt: dict) -> dict:
        # Convert hex strings to int/HexBytes to match Web3 format expected by mappers
        for key, value in receipt.items():
            if isinstance(value, str) and value.startswith("0x"):
                if key in ["transactionHash", "blockHash"]:
                    receipt[key] = HexBytes(value)
                elif key in ["blockNumber", "transactionIndex", "cumulativeGasUsed", "gasUsed", "status", "type"]:
                    try:
                        receipt[key] = int(value, 16)
                    except ValueError:
                        pass
            elif isinstance(value, list) and key == "logs":
                for log in value:
                    if "topics" in log:
                        log["topics"] = [HexBytes(t) if isinstance(t, str) else t for t in log["topics"]]
                    if "data" in log and isinstance(log["data"], str):
                        log["data"] = HexBytes(log["data"])
                    if "transactionHash" in log and isinstance(log["transactionHash"], str):
                        log["transactionHash"] = HexBytes(log["transactionHash"])
                    if "blockHash" in log and isinstance(log["blockHash"], str):
                        log["blockHash"] = HexBytes(log["blockHash"])
                    if "blockNumber" in log and isinstance(log["blockNumber"], str):
                        log["blockNumber"] = int(log["blockNumber"], 16)
                    if "transactionIndex" in log and isinstance(log["transactionIndex"], str):
                        log["transactionIndex"] = int(log["transactionIndex"], 16)
                    if "logIndex" in log and isinstance(log["logIndex"], str):
                        log["logIndex"] = int(log["logIndex"], 16)
        return receipt

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
