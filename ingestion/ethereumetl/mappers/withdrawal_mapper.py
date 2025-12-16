from typing import Dict, Any

from ingestion.ethereumetl.models.withdrawal import EthWithdrawal
from utils.formatter_utils import hex_to_dec

class EthWithdrawalMapper(object):
    @staticmethod
    def json_dict_to_withdrawal(json_dict: Dict[str, Any]) -> EthWithdrawal:
        return EthWithdrawal(
            index=hex_to_dec(json_dict.get("index")),
            validator_index=hex_to_dec(json_dict.get("validatorIndex")),
            address=json_dict.get("address"),
            amount=str(hex_to_dec(json_dict.get("amount"))) if json_dict.get("amount") is not None else None,
        )

    @staticmethod
    def withdrawal_to_dict(withdrawal: EthWithdrawal) -> Dict[str, Any]:
        return withdrawal.model_dump(exclude_none=True)