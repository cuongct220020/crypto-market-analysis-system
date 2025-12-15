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
# Change Description:
# - Refactored to explicitly handle ERC20 and ERC721 transfer events.
# - Added type hints and improved code readability.
# - Differentiated parsing logic based on topic count (3 for ERC20, 4 for ERC721).

from typing import List, Optional

from ingestion.ethereumetl.models.token_transfer import EthTokenTransfer
from ingestion.ethereumetl.models.receipt_log import EthReceiptLog
from utils.formatter_utils import chunk_string, hex_to_dec, to_normalized_address
from utils.logger_utils import get_logger

logger = get_logger("ETH Token Transfer Service")


# ERC721_INTERFACE_ID = "0x80ac58cd"  # ERC721 interface ID
TRANSFER_EVENT_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


class EthTokenTransfersService(object):
    @staticmethod
    def extract_transfer_from_logs(receipt_logs: EthReceiptLog) -> List[EthTokenTransfer]:
        """
        Extracts a token transfer (ERC20 or ERC721) from a receipt log.
        """
        topics: List[str] = receipt_logs.topics
        if not topics:
            return []

        # Check if the event is a Transfer event
        if topics[0].casefold() != TRANSFER_EVENT_TOPIC:
            return []

        token_transfer = EthTokenTransfer()
        token_transfer.token_address = to_normalized_address(receipt_logs.address)
        token_transfer.transaction_hash = receipt_logs.transaction_hash
        token_transfer.transaction_index = receipt_logs.transaction_index
        token_transfer.log_index = receipt_logs.log_index
        token_transfer.block_number = receipt_logs.block_number

        result = EthTokenTransfersService.parse_transfer_details(token_transfer, topics, receipt_logs.data)
        return [result] if result else []

    @staticmethod
    def parse_transfer_details(token_transfer: EthTokenTransfer, topics: List[str], data: str) -> Optional[EthTokenTransfer]:
        """
        Parses topics and data to populate the token_transfer object.
        Returns the modified token_transfer or None if parsing fails (e.g. mismatch).
        """
        # Handle unindexed event fields (data)
        data_words = extract_log_data_words(data)

        topics_len = len(topics)

        if topics_len == 3:
            # ERC20 Case
            if len(data_words) != 1:
                # logger.warning(f"ERC20 Transfer mismatch...") # Optional logging
                return None

            token_transfer.from_address = extract_address_from_log_topic(topics[1])
            token_transfer.to_address = extract_address_from_log_topic(topics[2])

            value_hex = data_words[0]
            value = hex_to_dec(value_hex)
            token_transfer.value = str(value) if value is not None else None
            token_transfer.type = "ERC20"

        elif topics_len == 4:
            # ERC721 Case
            token_transfer.from_address = extract_address_from_log_topic(topics[1])
            token_transfer.to_address = extract_address_from_log_topic(topics[2])

            token_id_hex = topics[3]
            value = hex_to_dec(token_id_hex)
            token_transfer.value = str(value) if value is not None else None
            token_transfer.type = "ERC721"

        else:
            return None

        return token_transfer


def extract_log_data_words(data: str) -> List[str]:
    if data and len(data) > 2:
        data_without_0x = data[2:]
        words = list(chunk_string(data_without_0x, 64))
        words_with_0x = [f"0x{word}" for word in words]
        return words_with_0x
    return []


def extract_address_from_log_topic(param: str) -> Optional[str]:
    if param is None:
        return None
    elif len(param) >= 40:
        return to_normalized_address("0x" + param[-40:])
    else:
        return to_normalized_address(param)
