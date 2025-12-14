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

        # Handle unindexed event fields (data)
        data_words = extract_log_data_words(receipt_logs.data)

        # Determine strict parsing based on topic count:
        # ERC20: Transfer(address indexed from, address indexed to, uint256 value)
        #        -> 3 topics (sig, from, to) + 1 data word (value)
        # ERC721: Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
        #        -> 4 topics (sig, from, to, tokenId) + 0 data words (usually)

        token_transfer = EthTokenTransfer()
        token_transfer.token_address = to_normalized_address(receipt_logs.address)
        token_transfer.transaction_hash = receipt_logs.transaction_hash
        token_transfer.transaction_index = receipt_logs.transaction_index
        token_transfer.log_index = receipt_logs.log_index
        token_transfer.block_number = receipt_logs.block_number

        topics_len = len(topics)

        # Logic extraction
        # combined list to access fields universally if needed, but strict checking is better
        # topics[1] = from, topics[2] = to

        if topics_len == 3:
            # ERC20 Case
            if len(data_words) != 1:
                logger.warning(
                    f"ERC20 Transfer mismatch: Expected 3 topics and 1 data word, "
                    f"got {topics_len} topics and {len(data_words)} data words. "
                    f"Log: {receipt_logs.log_index}, Tx: {receipt_logs.transaction_hash}"
                )
                return []

            token_transfer.from_address = extract_address_from_log_topic(topics[1])
            token_transfer.to_address = extract_address_from_log_topic(topics[2])

            value_hex = data_words[0]
            value = hex_to_dec(value_hex)
            token_transfer.value = str(value) if value is not None else None

        elif topics_len == 4:
            # ERC721 Case
            # Note: ERC721 transfers usually have 0 data words, but strict adherence might vary.
            # We focus on the fact that tokenId is in topics[3].

            token_transfer.from_address = extract_address_from_log_topic(topics[1])
            token_transfer.to_address = extract_address_from_log_topic(topics[2])

            token_id_hex = topics[3]
            value = hex_to_dec(token_id_hex)
            token_transfer.value = str(value) if value is not None else None

        else:
            # Fallback or invalid standard handling
            # If it doesn't match 3 or 4 topics, we ignore it or log warning
            logger.debug(
                f"Unexpected number of topics for Transfer event: {topics_len}. "
                f"Log: {receipt_logs.log_index}, Tx: {receipt_logs.transaction_hash}"
            )
            return []

        return [token_transfer]


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


# #     async def detect_token_type(self, token_address: str):
# #         if token_address in self.token_cache:
# #             return self.token_cache[token_address]
# #
# #         async with self.semaphore:
# #             try:
# #                 # Try ERC20
# #                 erc20_contract = self.web3.eth.contract(address=token_address, abi=ERC20_ABI)
# #                 decimals = await self.call_with_retry(erc20_contract.functions.decimals().call)
# #                 if decimals is not None:
# #                     return "ERC20"
# #
# #                 # Try ERC721
# #                 erc721_contract = self.web3.eth.contract(address=token_address, abi=ERC721_ABI)
# #                 try:
# #                     is_erc721 = await self.call_with_retry(
# #                         lambda: erc721_contract.functions.supportsInterface(ERC721_INTERFACE_ID).call())
# #                     if is_erc721:
# #                         return "ERC721"
# #                 except Exception:
# #                     pass
# #
# #                 # Try Name as fallback for ERC721
# #                 try:
# #                     token_name = await self.call_with_retry(erc721_contract.functions.name().call)
# #                     if token_name is not None:
# #                         # This is weak, but follows the snippet logic
# #                         return "ERC721"
# #                 except Exception:
# #                     pass
# #
# #                 return "UNKNOWN"
# #             except Exception as e:
# #                 logger.debug(f"Error detecting token type for {token_address}: {e}")
# #                 return "UNKNOWN"
