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
# Change Description: Add Typing, Using Pydantic Model to implement mapper logic,
# exclude manual convert from model to dict using model_dump() function.

from typing import Any, Dict

from ingestion.ethereumetl.models.transaction import EthTransaction
from utils.formatter_utils import hex_to_dec, to_normalized_address


class EthTransactionMapper(object):
    @staticmethod
    def json_dict_to_transaction(json_dict: Dict[str, Any], **kwargs) -> EthTransaction:

        return EthTransaction(
            hash=json_dict.get("hash"),
            nonce=hex_to_dec(json_dict.get("nonce")),
            block_number=hex_to_dec(json_dict.get("blockNumber")),
            block_hash=json_dict.get("blockHash"),
            block_timestamp=kwargs.get("block_timestamp"),
            transaction_index=hex_to_dec(json_dict.get("transactionIndex")),
            from_address=to_normalized_address(json_dict.get("from")),
            to_address=to_normalized_address(json_dict.get("to")),
            value=str(hex_to_dec(json_dict.get("value"))),
            gas=hex_to_dec(json_dict.get("gas")),
            gas_price=hex_to_dec(json_dict.get("gasPrice")),
            input=json_dict.get("input"),
            max_fee_per_gas=hex_to_dec(json_dict.get("maxFeePerGas")),
            max_priority_fee_per_gas=hex_to_dec(json_dict.get("maxPriorityFeePerGas")),
            transaction_type=hex_to_dec(json_dict.get("type")),
            max_fee_per_blob_gas=hex_to_dec(json_dict.get("maxFeePerBlobGas")),
            blob_versioned_hashes=json_dict.get("blobVersionedHashes", []),
        )

    @staticmethod
    def web3_dict_to_transaction(web3_dict: Dict[str, Any], **kwargs) -> EthTransaction:
        def to_hex(val):
            if val is None:
                return None
            return val.hex() if hasattr(val, "hex") else str(val)

        tx_type = web3_dict.get("type")
        if hasattr(tx_type, "hex"):
            tx_type = int(tx_type.hex(), 16)
        elif isinstance(tx_type, str):
            tx_type = int(tx_type, 16)

        return EthTransaction(
            hash=to_hex(web3_dict.get("hash")),
            nonce=web3_dict.get("nonce"),
            block_number=web3_dict.get("block_number"),
            block_hash=to_hex(web3_dict.get("block_hash")),
            block_timestamp=kwargs.get("block_timestamp"),
            transaction_index=web3_dict.get("transaction_index"),
            from_address=to_normalized_address(web3_dict.get("from")),
            to_address=to_normalized_address(web3_dict.get("to")),
            value=str(web3_dict.get("value")),
            gas=web3_dict.get("gas"),
            gas_price=web3_dict.get("gas_price"),
            input=to_hex(web3_dict.get("input")),
            max_fee_per_gas=web3_dict.get("max_fee_per_gas"),
            max_priority_fee_per_gas=web3_dict.get("max_priority_fee_per_gas"),
            transaction_type=tx_type,
            max_fee_per_blob_gas=web3_dict.get("max_fee_per_blob_gas"),
            blob_versioned_hashes=[to_hex(h) for h in web3_dict.get("blob_versioned_hashes", [])],
        )

    @staticmethod
    def transaction_to_dict(transaction: EthTransaction) -> Dict[str, Any]:
        return transaction.model_dump(exclude_none=True)
