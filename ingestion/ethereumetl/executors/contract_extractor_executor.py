import asyncio
from typing import List, Any

from web3 import AsyncWeb3

from ingestion.ethereumetl.scheduler.work_scheduler import WorkScheduler
from ingestion.ethereumetl.models.contract import EthContract
from ingestion.ethereumetl.service.eth_contract_token_metadata_service import EthContractTokenMetadataService
from utils.logger_utils import get_logger

logger = get_logger("Contract Extractor Executor")


class ContractExtractorExecutor:
    """
    Executor responsible for fetching metadata for a given list of contract addresses.
    It uses WorkScheduler to handle concurrent RPC requests.
    """
    def __init__(
        self,
        web3: AsyncWeb3,
        item_exporter: Any,
        batch_size: int = 10,
        max_workers: int = 5,
    ):
        self.item_exporter = item_exporter
        self.web3 = web3
        self.eth_contract_metadata_service = EthContractTokenMetadataService(web3)
        self.work_scheduler = WorkScheduler(batch_size, max_workers)

    async def execute(self, contract_addresses: List[str]) -> None:
        """
        Fetches metadata for the provided list of contract addresses and exports the results.
        """
        if not contract_addresses:
            logger.info("No contract addresses provided for extraction.")
            return

        logger.info(f"Executing contract metadata extraction for {len(contract_addresses)} addresses...")
        self.item_exporter.open()
        
        try:
            # Execute pipeline: Address -> Metadata -> Export
            await self.work_scheduler.execute_pipeline(
                work_iterable=contract_addresses,
                fetch_handler=self._fetch_metadata_batch_worker,
                process_handler=self._export_metadata_batch_worker
            )
            
        finally:
            self.work_scheduler.shutdown()
            self.item_exporter.close()

    async def _fetch_metadata_batch_worker(self, addresses_batch: List[str]) -> List[EthContract]:
        """
        Worker: Fetch metadata for a batch of addresses.
        """
        tasks = [
            self.eth_contract_metadata_service.get_token(address)
            for address in addresses_batch
        ]
        
        # Return exceptions=True to handle individual failures gracefully
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        valid_contracts = []
        for res in results:
            if isinstance(res, EthContract):
                valid_contracts.append(res)
            elif isinstance(res, Exception):
                logger.error(f"Error fetching contract metadata: {res}")
        
        return valid_contracts

    async def _export_metadata_batch_worker(self, contracts: List[EthContract]) -> None:
        """
        Worker: Export metadata.
        """
        if contracts:
            self.item_exporter.export_items(contracts)