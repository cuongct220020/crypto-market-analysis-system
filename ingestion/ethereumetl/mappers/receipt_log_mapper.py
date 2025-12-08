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
from utils.formatters import hex_to_dec


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
    def web3_dict_to_receipt_log(dict: Dict[str, Any]) -> EthReceiptLog:
        # Handle web3.py dictionary format
        receipt_log = EthReceiptLog()

        receipt_log.log_index = dict.get("logIndex")

        transaction_hash = dict.get("transactionHash")
        if transaction_hash is not None and hasattr(transaction_hash, "hex"):
            transaction_hash = transaction_hash.hex()
        receipt_log.transaction_hash = transaction_hash

        block_hash = dict.get("blockHash")
        if block_hash is not None and hasattr(block_hash, "hex"):
            block_hash = block_hash.hex()
        receipt_log.block_hash = block_hash

        receipt_log.block_number = dict.get("blockNumber")
        receipt_log.address = dict.get("address")
        receipt_log.data = dict.get("data")

        if "topics" in dict:
            receipt_log.topics = [topic.hex() if hasattr(topic, "hex") else topic for topic in dict["topics"]]

        return receipt_log

    @staticmethod
    def receipt_log_to_dict(receipt_log: EthReceiptLog) -> Dict[str, Any]:
        return receipt_log.model_dump(exclude_none=True)

    @staticmethod
    def dict_to_receipt_log(dict: Dict[str, Any]) -> EthReceiptLog:
        # For re-hydrating from internal dicts (e.g. from Kafka intermediate step)
        topics = dict.get("topics", [])
        if isinstance(topics, str):
            if len(topics.strip()) == 0:
                topics = []
            else:
                topics = topics.strip().split(",")

        return EthReceiptLog(
            log_index=dict.get("log_index"),
            transaction_hash=dict.get("transaction_hash"),
            transaction_index=dict.get("transaction_index"),
            block_hash=dict.get("block_hash"),
            block_number=dict.get("block_number"),
            address=dict.get("address"),
            data=dict.get("data"),
            topics=topics,
        )
