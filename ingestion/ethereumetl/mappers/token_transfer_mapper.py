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

from typing import Any, Dict, List, Optional

from ingestion.ethereumetl.models.token_transfer import EthTokenTransfer
from utils.formatter_utils import chunk_string, hex_to_dec, to_normalized_address

TRANSFER_EVENT_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


class EthTokenTransferMapper(object):
    @staticmethod
    def token_transfer_to_dict(token_transfer: EthTokenTransfer) -> Dict[str, Any]:
        return token_transfer.model_dump(exclude_none=True)

    def json_dict_to_token_transfer(self, log_dict: Dict[str, Any]) -> Optional[EthTokenTransfer]:
        """
        Extracts a token transfer (ERC20 or ERC721) from a raw JSON-RPC log dict.
        Returns None if the log is not a Transfer event.
        """
        topics = log_dict.get("topics", [])
        if not topics:
            return None

        # Check if the event is a Transfer event
        # topic[0] must match the Transfer signature
        if topics[0].casefold() != TRANSFER_EVENT_TOPIC:
            return None

        token_transfer = EthTokenTransfer()
        token_transfer.token_address = to_normalized_address(log_dict.get("address"))
        token_transfer.transaction_hash = log_dict.get("transactionHash")
        token_transfer.transaction_index = hex_to_dec(log_dict.get("transactionIndex"))
        token_transfer.log_index = hex_to_dec(log_dict.get("logIndex"))
        token_transfer.block_number = hex_to_dec(log_dict.get("blockNumber"))
        token_transfer.block_hash = log_dict.get("blockHash")

        topics_len = len(topics)
        data = log_dict.get("data", "0x")
        data_words = self._extract_log_data_words(data)

        # Logic based on topic count
        if topics_len == 3:
            # ERC20: Transfer(from, to, value) -> topics: [sig, from, to], data: value
            if len(data_words) != 1:
                return None
            
            token_transfer.from_address = self._extract_address_from_log_topic(topics[1])
            token_transfer.to_address = self._extract_address_from_log_topic(topics[2])
            
            value_hex = data_words[0]
            value = hex_to_dec(value_hex)
            token_transfer.value = str(value) if value is not None else None
            token_transfer.type = "ERC20" # Infer type hint

        elif topics_len == 4:
            # ERC721: Transfer(from, to, tokenId) -> topics: [sig, from, to, tokenId], data: empty
            token_transfer.from_address = self._extract_address_from_log_topic(topics[1])
            token_transfer.to_address = self._extract_address_from_log_topic(topics[2])
            
            token_id_hex = topics[3]
            value = hex_to_dec(token_id_hex)
            token_transfer.value = str(value) if value is not None else None
            token_transfer.type = "ERC721" # Infer type hint

        else:
            return None

        return token_transfer

    @staticmethod
    def _extract_log_data_words(data: str) -> List[str]:
        if data and len(data) > 2:
            data_without_0x = data[2:]
            words = list(chunk_string(data_without_0x, 64))
            words_with_0x = [f"0x{word}" for word in words]
            return words_with_0x
        return []

    @staticmethod
    def _extract_address_from_log_topic(param: str) -> Optional[str]:
        if param is None:
            return None
        elif len(param) >= 40:
            return to_normalized_address("0x" + param[-40:])
        else:
            return to_normalized_address(param)
