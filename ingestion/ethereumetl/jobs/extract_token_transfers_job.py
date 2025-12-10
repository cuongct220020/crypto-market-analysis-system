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
# Change Description:

from typing import Any, Iterable

from ingestion.blockchainetl.jobs.base_job import BaseJob
from ingestion.ethereumetl.mappers.receipt_log_mapper import EthReceiptLogMapper
from ingestion.ethereumetl.mappers.token_transfer_mapper import EthTokenTransferMapper
from ingestion.ethereumetl.service.eth_token_transfer_service import EthTokenTransferService


class ExtractTokenTransfersJob(BaseJob):
    def __init__(
        self,
        logs_iterable: Iterable[Any],
        item_exporter: Any,
    ):
        self.logs_iterable = logs_iterable
        self.item_exporter = item_exporter

        self.receipt_log_mapper = EthReceiptLogMapper()
        self.token_transfer_mapper = EthTokenTransferMapper()
        self.token_transfer_service = EthTokenTransferService()

    def _start(self) -> None:
        self.item_exporter.open()

    def _export(self) -> None:
        self._extract_transfers(self.logs_iterable)

    def _extract_transfers(self, logs: Iterable[Any]) -> None:
        for log in logs:
            self._extract_transfer(log)

    def _extract_transfer(self, log: Any) -> None:
        token_transfer = self.token_transfer_service.extract_transfer_from_log(log)
        if token_transfer is not None:
            self.item_exporter.export_item(token_transfer)

    def _end(self) -> None:
        self.item_exporter.close()
