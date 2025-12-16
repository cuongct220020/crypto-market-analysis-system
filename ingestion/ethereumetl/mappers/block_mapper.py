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
# Change Description: Refactored to use Pydantic models and added typing.

from typing import Any, Dict

from ingestion.ethereumetl.mappers.transaction_mapper import EthTransactionMapper
from ingestion.ethereumetl.mappers.withdrawal_mapper import EthWithdrawalMapper
from ingestion.ethereumetl.models.block import EthBlock
from utils.formatter_utils import hex_to_dec, to_normalized_address

class EthBlockMapper(object):
    def __init__(self):
        self.transaction_mapper = EthTransactionMapper()
        self.withdrawal_mapper = EthWithdrawalMapper()

    def json_dict_to_block(self, json_dict: Dict[str, Any]) -> EthBlock:
        block = EthBlock(
            number=hex_to_dec(json_dict.get("number")),
            hash=json_dict.get("hash"),
            mix_hash=json_dict.get("mixHash"),
            parent_hash=json_dict.get("parentHash"),
            nonce=json_dict.get("nonce"),
            sha3_uncles=json_dict.get("sha3Uncles"),
            logs_bloom=json_dict.get("logsBloom"),
            transactions_root=json_dict.get("transactionsRoot"),
            state_root=json_dict.get("stateRoot"),
            receipts_root=json_dict.get("receiptsRoot"),
            miner=to_normalized_address(json_dict.get("miner")),
            difficulty=hex_to_dec(json_dict.get("difficulty")),
            total_difficulty=hex_to_dec(json_dict.get("totalDifficulty")),
            size=hex_to_dec(json_dict.get("size")),
            extra_data=json_dict.get("extraData"),
            gas_limit=hex_to_dec(json_dict.get("gasLimit")),
            gas_used=hex_to_dec(json_dict.get("gasUsed")),
            timestamp=hex_to_dec(json_dict.get("timestamp")),
            base_fee_per_gas=hex_to_dec(json_dict.get("baseFeePerGas")) or 0,
            withdrawals_root=json_dict.get("withdrawalsRoot"),
            blob_gas_used=hex_to_dec(json_dict.get("blobGasUsed")),
            excess_blob_gas=hex_to_dec(json_dict.get("excessBlobGas")),
            parent_beacon_block_root=json_dict.get("parentBeaconBlockRoot")
        )

        if "transactions" in json_dict:
            block.transactions = [
                self.transaction_mapper.json_dict_to_transaction(tx, block_timestamp=block.timestamp)
                for tx in json_dict["transactions"] if isinstance(tx, dict)
            ]
            block.transaction_count = len(json_dict["transactions"])

        if "withdrawals" in json_dict:
            block.withdrawals = [
                self.withdrawal_mapper.json_dict_to_withdrawal(withdrawal)
                for withdrawal in json_dict["withdrawals"] if isinstance(withdrawal, dict)
            ]

        return block

    @staticmethod
    def block_to_dict(block: EthBlock) -> Dict[str, Any]:
        return block.model_dump(exclude_none=True)
