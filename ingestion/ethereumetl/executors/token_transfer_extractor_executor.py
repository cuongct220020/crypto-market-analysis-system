from typing import List, Any

from ingestion.ethereumetl.models.receipt_log import EthReceiptLog
from ingestion.ethereumetl.models.token_transfer import EthTokenTransfer
from ingestion.ethereumetl.service.eth_token_transfers_service import EthTokenTransfersService
from utils.logger_utils import get_logger

logger = get_logger("Token Transfer Extractor Executor")


class TokenTransferExtractorExecutor:
    """
    Executor responsible ONLY for parsing and exporting token transfers from receipt logs.
    It does NOT fetch metadata anymore.
    """
    def __init__(
        self,
        item_exporter: Any,
    ):
        self.item_exporter = item_exporter
        self.eth_token_transfer_service = EthTokenTransfersService()

    def execute(self, receipt_logs: List[EthReceiptLog]) -> List[EthTokenTransfer]:
        """
        Parses logs to extract transfers, exports them, and returns the list of transfers
        so that the caller (Coordinator) can extract unique token addresses for metadata fetching.
        """
        logger.info("Executing token transfer extraction...")
        self.item_exporter.open()
        
        try:
            # Step 1: Filter & Parse Transfers (CPU bound)
            transfers = self._parse_transfers(receipt_logs)
            logger.info(f"Parsed {len(transfers)} token transfers from {len(receipt_logs)} logs.")

            # Step 2: Export Transfers
            if transfers:
                self.item_exporter.export_items(transfers)
            
            return transfers

        finally:
            # Note: We don't close the exporter here if it's shared, but usually open/close is safe/idempotent
            # or managed by the caller. For safety in this specific executor design:
            # If this executor owns the exporter lifecycle for this step, close it.
            # But since we might chain executors, maybe keep open? 
            # Standard pattern in this project seems to be open/close per job run.
            self.item_exporter.close()

    def _parse_transfers(self, logs: List[EthReceiptLog]) -> List[EthTokenTransfer]:
        results = []
        for log in logs:
            # extract_transfer_from_logs now returns a List (from previous refactor request)
            # We need to handle that.
            extracted = self.eth_token_transfer_service.extract_transfer_from_logs(log)
            if extracted:
                results.extend(extracted)
        return results