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
# Change Description: Refactored to Async Job and integrated EthContractTokenMetadataService.


import asyncio
from typing import Any, List, Set

from web3 import AsyncWeb3

from ingestion.blockchainetl.jobs.async_base_job import AsyncBaseJob
from ingestion.ethereumetl.models.contract import EthContract
from ingestion.ethereumetl.models.receipt_log import EthReceiptLog
from ingestion.ethereumetl.service.eth_contract_token_metadata_service import EthContractTokenMetadataService
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
        self.item_exporter = item_exporter
        self.web3 = web3
        self.max_workers = max_workers
        self.eth_contract_metadata_service = EthContractTokenMetadataService(web3)

    async def _start(self) -> None:
        logger.info("Starting extraction of contracts from receipt logs...")
        self.item_exporter.open()

    async def _export(self) -> None:
        logger.info("Starting contract extraction process...")
        
        # Step 1: Extract unique contract addresses from logs
        # We assume the log's address is the contract address emitting the event
        unique_contract_addresses = {log.address for log in self.receipt_logs if log.address}
        logger.info(f"Found {len(unique_contract_addresses)} unique contract addresses.")

        # Step 2: Fetch Metadata for unique addresses (IO bound - Async)
        contracts_metadata = await self._fetch_contracts_metadata(unique_contract_addresses)
        logger.info(f"Fetched metadata for {len(contracts_metadata)} contracts.")

        # Step 3: Export Data
        if contracts_metadata:
            await self.item_exporter.export_items(contracts_metadata)
            
        logger.info("Contract extraction completed")

    async def _fetch_contracts_metadata(self, addresses: Set[str]) -> List[EthContract]:
        tasks = []
        for address in addresses:
            tasks.append(self.eth_contract_metadata_service.get_token(address))
        
        # Execute tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        valid_contracts = []
        for res in results:
            if isinstance(res, EthContract):
                valid_contracts.append(res)
            elif isinstance(res, Exception):
                logger.error(f"Error fetching contract metadata: {res}")
        
        return valid_contracts

    async def _end(self) -> None:
        logger.info("Shutting down Extract Contracts Job resources...")
        self.item_exporter.close()
        logger.info("Extract Contracts Job completed successfully")
