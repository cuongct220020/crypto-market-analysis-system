from typing import Any, Dict, List, Optional
from ingestion.ethereumetl.models.token_transfer import EthTokenTransfer
from ingestion.ethereumetl.models.receipt_log import EthReceiptLog
from ingestion.ethereumetl.service.eth_token_transfers_service import EthTokenTransfersService
from utils.formatter_utils import hex_to_dec, to_normalized_address

class EthTokenTransferMapper(object):
    @staticmethod
    def token_transfer_to_dict(token_transfer: EthTokenTransfer) -> Dict[str, Any]:
        """
        Converts an EthTokenTransfer object to a dictionary, suitable for Avro serialization.
        """
        # Pydantic's model_dump with exclude_none=False ensures fields match Avro schema explicitly (sending nulls)
        return token_transfer.model_dump(exclude_none=False)

    @staticmethod
    def json_dict_to_token_transfers(log_dict: Dict[str, Any], chain_id: Optional[int] = None) -> List[EthTokenTransfer]:
        """
        Maps a raw JSON-RPC log dict to a list of EthTokenTransfer objects.
        This function now primarily serves as a bridge to the EthTokenTransfersService.
        """
        # Construct an EthReceiptLog from the raw dict
        receipt_log = EthReceiptLog(
            address=to_normalized_address(log_dict.get("address")),
            transaction_hash=log_dict.get("transactionHash"),
            transaction_index=hex_to_dec(log_dict.get("transactionIndex")),
            log_index=hex_to_dec(log_dict.get("logIndex")),
            block_number=hex_to_dec(log_dict.get("blockNumber")),
            block_hash=log_dict.get("blockHash"),
            block_timestamp=hex_to_dec(log_dict.get("blockTimestamp")) if log_dict.get("blockTimestamp") else None,
            topics=log_dict.get("topics", []),
            data=log_dict.get("data", "0x"),
            chain_id=chain_id # Pass chain_id if available
        )
        
        # Delegate the actual extraction logic to the service
        return EthTokenTransfersService.extract_transfer_from_logs(receipt_log)