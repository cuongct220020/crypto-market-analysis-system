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
# Change Description: Refactored to Async Job, added Batch Processing for Transfers and Metadata.

import asyncio
from typing import Any, List, Set, Optional

from web3 import AsyncWeb3

from ingestion.blockchainetl.jobs.async_base_job import AsyncBaseJob
from ingestion.ethereumetl.models.contract import EthContract
from ingestion.ethereumetl.models.receipt_log import EthReceiptLog
from ingestion.ethereumetl.models.token_transfer import EthTokenTransfer
from ingestion.ethereumetl.service.eth_contract_token_metadata_service import EthContractTokenMetadataService
from ingestion.ethereumetl.service.eth_token_transfers_service import EthTokenTransfersService
from utils.logger_utils import get_logger

logger = get_logger("Extract Token Transfers Job")


class ExtractTokenTransfersJob(AsyncBaseJob):
    def __init__(
        self,
        receipt_logs: List[EthReceiptLog],
        item_exporter: Any,
        web3: AsyncWeb3,
        max_workers: int = 5,
    ):
        self.receipt_logs = receipt_logs
        self.item_exporter = item_exporter
        self.web3 = web3
        self.max_workers = max_workers
        self.eth_token_transfer_service = EthTokenTransfersService()
        self.eth_contract_metadata_service = EthContractTokenMetadataService(web3)

    async def _start(self) -> None:
        logger.info("Starting extraction of token transfers from logs...")
        self.item_exporter.open()

    async def _export(self) -> None:
        logger.info("Starting token transfer extraction process...")
        
        # Step 1: Filter & Parse Transfers (CPU bound)
        transfers = self._parse_transfers(self.receipt_logs)
        logger.info(f"Parsed {len(transfers)} token transfers from {len(self.receipt_logs)} logs.")

        # Step 2: Extract Unique Token Addresses
        unique_token_addresses = {t.token_address for t in transfers if t.token_address}
        logger.info(f"Found {len(unique_token_addresses)} unique token contracts.")

        # Step 3: Fetch Metadata for unique addresses (IO bound - Async)
        contracts_metadata = await self._fetch_contracts_metadata(unique_token_addresses)
        logger.info(f"Fetched metadata for {len(contracts_metadata)} contracts.")

        # Step 4: Export Data
        if transfers:
            await self.item_exporter.export_items(transfers)
        if contracts_metadata:
            await self.item_exporter.export_items(contracts_metadata)

        logger.info("Token transfer extraction and export completed.")

    def _parse_transfers(self, logs: List[EthReceiptLog]) -> List[EthTokenTransfer]:
        results = []
        for log in logs:
            transfer = self.eth_token_transfer_service.extract_transfer_from_logs(log)
            if transfer:
                results.append(transfer)
        return results

    async def _fetch_contracts_metadata(self, addresses: Set[str]) -> List[EthContract]:
        tasks = []
        for address in addresses:
            tasks.append(self.eth_contract_metadata_service.get_token(address))
        
        # Execute tasks concurrently
        # Note: In a production environment with many tasks, consider using a semaphore 
        # or chunking to limit concurrency (using self.max_workers).
        # For now, asyncio.gather is used for simplicity assuming reasonable batch sizes.
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        valid_contracts = []
        for res in results:
            if isinstance(res, EthContract):
                valid_contracts.append(res)
            elif isinstance(res, Exception):
                logger.error(f"Error fetching contract metadata: {res}")
        
        return valid_contracts

    async def _end(self) -> None:
        self.item_exporter.close()