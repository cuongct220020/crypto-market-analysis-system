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
# Change Description: Refactored to use Pydantic models.

from typing import Any, Dict

from ingestion.ethereumetl.models.receipt_log import EthReceiptLog
from utils.formatter_utils import hex_to_dec


class EthReceiptLogMapper(object):
    @staticmethod
    def json_dict_to_receipt_log(json_dict: Dict[str, Any]) -> EthReceiptLog:
        return EthReceiptLog(
            log_index=hex_to_dec(json_dict.get("logIndex")),
            transaction_hash=json_dict.get("transactionHash"),
            transaction_index=hex_to_dec(json_dict.get("transactionIndex")),
            block_hash=json_dict.get("blockHash"),
            block_number=hex_to_dec(json_dict.get("blockNumber")),
            address=json_dict.get("address"),
            data=json_dict.get("data"),
            topics=json_dict.get("topics", []),
        )

    @staticmethod
    def web3_dict_to_receipt_log(web3_dict: Dict[str, Any]) -> EthReceiptLog:
        # Handle web3.py dictionary format (AttributeDict)
        def to_hex(val):
            if val is None:
                return None
            return val.hex() if hasattr(val, "hex") else str(val)

        return EthReceiptLog(
            log_index=web3_dict.get("log_index"),
            transaction_hash=to_hex(web3_dict.get("transaction_hash")),
            transaction_index=web3_dict.get("transaction_index"),
            block_hash=to_hex(web3_dict.get("block_hash")),
            block_number=web3_dict.get("block_number"),
            address=web3_dict.get("address"),
            data=to_hex(web3_dict.get("data")),
            topics=[to_hex(topic) for topic in web3_dict.get("topics", [])],
        )

    @staticmethod
    def receipt_log_to_dict(receipt_log: EthReceiptLog) -> Dict[str, Any]:
        return receipt_log.model_dump(exclude_none=True)
