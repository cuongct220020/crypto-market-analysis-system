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

from typing import Any, Dict, List

from ingestion.ethereumetl.mappers.transaction_mapper import EthTransactionMapper
from ingestion.ethereumetl.models.block import EthBlock
from ingestion.ethereumetl.models.withdrawal import Withdrawal
from utils.formatter_utils import hex_to_dec, to_normalized_address


class EthBlockMapper(object):
    def __init__(self, transaction_mapper=None):
        if transaction_mapper is None:
            self.transaction_mapper = EthTransactionMapper()
        else:
            self.transaction_mapper = transaction_mapper

    def json_dict_to_block(self, json_dict: Dict[str, Any]) -> EthBlock:
        block = EthBlock(
            number=hex_to_dec(json_dict.get("number")),
            hash=json_dict.get("hash"),
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
        )

        if "transactions" in json_dict:
            block.transactions = [
                self.transaction_mapper.json_dict_to_transaction(tx, block_timestamp=block.timestamp)
                for tx in json_dict["transactions"]
                if isinstance(tx, dict)
            ]
            block.transaction_count = len(json_dict["transactions"])

        if "withdrawals" in json_dict:
            block.withdrawals = self.parse_withdrawals(json_dict["withdrawals"])

        return block

    @staticmethod
    def parse_withdrawals(withdrawals_json: List[Dict[str, Any]]) -> List[Withdrawal]:
        return [
            Withdrawal(
                index=hex_to_dec(withdrawals_json["index"]),
                validator_index=hex_to_dec(withdrawals_json["validatorIndex"]),
                address=withdrawals_json["address"],
                amount=str(hex_to_dec(withdrawals_json["amount"])) if withdrawal.get("amount") is not None else None,
            )
            for withdrawal in withdrawals_json
        ]

    def web3_dict_to_block(self, web3_dict: Dict[str, Any]) -> EthBlock:
        def to_hex(val):
            if val is None:
                return None
            return val.hex() if hasattr(val, "hex") else str(val)

        block = EthBlock(
            number=web3_dict.get("number"),
            hash=to_hex(web3_dict.get("hash")),
            parent_hash=to_hex(web3_dict.get("parent_hash")),
            nonce=to_hex(web3_dict.get("nonce")),
            sha3_uncles=to_hex(web3_dict.get("sha3_uncles")),
            logs_bloom=to_hex(web3_dict.get("logs_bloom")),
            transactions_root=to_hex(web3_dict.get("transactions_root")),
            state_root=to_hex(web3_dict.get("state_root")),
            receipts_root=to_hex(web3_dict.get("receipts_root")),
            miner=to_normalized_address(web3_dict.get("miner")),
            difficulty=web3_dict.get("difficulty"),
            total_difficulty=web3_dict.get("total_difficulty"),
            size=web3_dict.get("size"),
            extra_data=to_hex(web3_dict.get("extra_data")),
            gas_limit=web3_dict.get("gas_limit"),
            gas_used=web3_dict.get("gas_used"),
            timestamp=web3_dict.get("timestamp"),
            base_fee_per_gas=web3_dict.get("base_fee_per_gas") or 0,
            withdrawals_root=to_hex(web3_dict.get("withdrawals_root")),
            blob_gas_used=web3_dict.get("blob_gas_used"),
            excess_blob_gas=web3_dict.get("excess_blob_gas"),
        )

        if "transactions" in web3_dict:
            transactions = web3_dict["transactions"]
            if (
                transactions
                and isinstance(transactions[0], (dict, object))
                and not isinstance(transactions[0], (str, bytes))
            ):
                block.transactions = [
                    self.transaction_mapper.web3_dict_to_transaction(tx, block_timestamp=block.timestamp)
                    for tx in transactions
                ]
                block.transaction_count = len(transactions)
            else:
                block.transaction_count = len(transactions)

        if "withdrawals" in web3_dict:
            block.withdrawals = self.parse_withdrawals_web3(web3_dict["withdrawals"])

        return block

    @staticmethod
    def parse_withdrawals_web3(withdrawals: List[Any]) -> List[Withdrawal]:
        return [
            Withdrawal(
                index=w.get("index"),
                validator_index=w.get("validator_index"),
                address=w.get("address"),
                amount=str(w.get("amount")) if w.get("amount") is not None else None,
            )
            for w in withdrawals
        ]

    @staticmethod
    def block_to_dict(block: EthBlock) -> Dict[str, Any]:
        return block.model_dump(exclude_none=True)
