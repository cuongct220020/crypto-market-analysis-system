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
from utils.logger_utils import get_logger

logger = get_logger("Extract Token Transfers Job")


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
        logger.info("Starting extraction of token transfers from logs...")
        self.item_exporter.open()

    def _export(self) -> None:
        logger.info("Starting token transfer extraction process...")
        self._extract_transfers(self.logs_iterable)
        logger.info("Token transfer extraction completed")

    def _extract_transfers(self, logs: Iterable[Any]) -> None:
        processed_logs = 0

        logger.debug("Starting to extract token transfers from logs...")
        for log in logs:
            processed_logs += 1
            self._extract_transfer(log, processed_logs)
            if processed_logs % 1000 == 0:  # Log progress every 1000 logs
                logger.info(f"Processed {processed_logs} logs so far...")

        logger.info(f"Processed {processed_logs} total logs, extracted token transfers")

    def _extract_transfer(self, log: Any, log_number: int = None) -> None:
        logger.debug(f"Processing log {log_number} for token transfer extraction")
        token_transfer = self.token_transfer_service.extract_transfer_from_log(log)
        if token_transfer is not None:
            logger.debug(f"Extracted token transfer: {token_transfer.token_address} - {token_transfer.value}")
            self.item_exporter.export_item(token_transfer)
        else:
            logger.debug(f"No token transfer extracted from log {log_number}")

    def _end(self) -> None:
        logger.info("Shutting down ExtractTokenTransfersJob resources...")
        self.item_exporter.close()
        logger.info("ExtractTokenTransfersJob completed successfully")
