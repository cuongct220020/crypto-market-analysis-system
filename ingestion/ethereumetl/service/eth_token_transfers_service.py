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
# - Refactored to handle ERC20, ERC721, and ERC1155 transfer events.
# - Implemented explicit parsing logic for each standard.
# - Added UNKNOWN handling for ambiguous transfers.

from typing import List, Optional
from eth_abi import decode

from ingestion.ethereumetl.models.token_transfer import EthTokenTransfer, TokenStandard
from ingestion.ethereumetl.models.receipt_log import EthReceiptLog
from utils.formatter_utils import chunk_string, hex_to_dec, to_normalized_address
from utils.logger_utils import get_logger
from constants.event_transfer_signature import (
    TRANSFER_EVENT_SIGNATURE,
    TRANSFER_SINGLE_EVENT_SIGNATURE,
    TRANSFER_BATCH_EVENT_SIGNATURE
)

logger = get_logger("ETH Token Transfer Service")

class EthTokenTransfersService(object):
    @staticmethod
    def extract_transfer_from_logs(receipt_logs: EthReceiptLog) -> List[EthTokenTransfer]:
        """
        Extracts token transfers (ERC20, ERC721, ERC1155) from a receipt log.
        """
        topics: List[str] = receipt_logs.topics
        if not topics:
            return []

        sig = topics[0].casefold()
        
        # Initialize base transfer object with common fields
        base_transfer = EthTokenTransfer()
        base_transfer.contract_address = to_normalized_address(receipt_logs.address)
        base_transfer.transaction_hash = receipt_logs.transaction_hash
        base_transfer.transaction_index = receipt_logs.transaction_index
        base_transfer.log_index = receipt_logs.log_index
        base_transfer.block_number = receipt_logs.block_number
        base_transfer.block_hash = receipt_logs.block_hash
        base_transfer.block_timestamp = receipt_logs.block_timestamp

        # Dispatch based on Event Signature
        if sig == TRANSFER_EVENT_SIGNATURE:
            return EthTokenTransfersService._handle_erc20_721(base_transfer, topics, receipt_logs.data)
            
        elif sig == TRANSFER_SINGLE_EVENT_SIGNATURE:
            return EthTokenTransfersService._handle_erc1155_single(base_transfer, topics, receipt_logs.data)
            
        elif sig == TRANSFER_BATCH_EVENT_SIGNATURE:
            return EthTokenTransfersService._handle_erc1155_batch(base_transfer, topics, receipt_logs.data)
            
        return []

    @staticmethod
    def _handle_erc20_721(base: EthTokenTransfer, topics: List[str], data: str) -> List[EthTokenTransfer]:
        """
        Handles standard Transfer(from, to, value/tokenId) event.
        Distinguishes between ERC20 (3 topics) and ERC721 (4 topics).
        """
        transfer = base.model_copy()
        topics_len = len(topics)
        data_words = extract_log_data_words(data)

        if topics_len == 3:
            # ERC20: Transfer(from, to, value)
            # topics: [sig, from, to]
            # data: value
            if len(data_words) != 1:
                transfer.token_standard = TokenStandard.UNKNOWN
                return [transfer] # Return as Unknown or empty? Prefer explicit Unknown if signature matched but format failed.

            transfer.token_standard = TokenStandard.ERC20
            transfer.from_address = extract_address_from_log_topic(topics[1])
            transfer.to_address = extract_address_from_log_topic(topics[2])
            
            val = hex_to_dec(data_words[0])
            transfer.value = str(val) if val is not None else None
            
            return [transfer]

        elif topics_len == 4:
            # ERC721: Transfer(from, to, tokenId)
            # topics: [sig, from, to, tokenId]
            transfer.token_standard = TokenStandard.ERC721
            transfer.from_address = extract_address_from_log_topic(topics[1])
            transfer.to_address = extract_address_from_log_topic(topics[2])
            
            # Value in ERC721 context represents the Token ID
            val = hex_to_dec(topics[3])
            transfer.value = str(val) if val is not None else None
            
            return [transfer]

        else:
            # Signature matches but topic count is weird -> Unknown
            transfer.token_standard = TokenStandard.UNKNOWN
            return [transfer]

    @staticmethod
    def _handle_erc1155_single(base: EthTokenTransfer, topics: List[str], data: str) -> List[EthTokenTransfer]:
        """
        Handles ERC1155 TransferSingle(operator, from, to, id, value)
        """
        if len(topics) != 4:
            return []

        transfer = base.model_copy()
        transfer.token_standard = TokenStandard.ERC1155
        
        # topics: [sig, operator, from, to]
        # We skip operator for the simplified transfer model, focus on from/to
        transfer.from_address = extract_address_from_log_topic(topics[2])
        transfer.to_address = extract_address_from_log_topic(topics[3])
        
        try:
            # data: id (uint256), value (uint256)
            # Remove 0x prefix
            data_bytes = bytes.fromhex(data[2:] if data.startswith("0x") else data)
            decoded = decode(['uint256', 'uint256'], data_bytes)
            
            token_id = decoded[0]
            amount = decoded[1]
            
            # NOTE: Current schema 'value' is ambiguous for 1155. 
            # We store 'amount' to be consistent with ERC20, but this loses TokenID info.
            # In a real scenario, we should append token_id to the record.
            transfer.value = str(amount)
            
            return [transfer]
        except Exception as e:
            logger.debug(f"Failed to decode ERC1155 Single data: {e}")
            transfer.token_standard = TokenStandard.UNKNOWN
            return [transfer]

    @staticmethod
    def _handle_erc1155_batch(base: EthTokenTransfer, topics: List[str], data: str) -> List[EthTokenTransfer]:
        """
        Handles ERC1155 TransferBatch(operator, from, to, ids[], values[])
        """
        if len(topics) != 4:
            return []

        from_addr = extract_address_from_log_topic(topics[2])
        to_addr = extract_address_from_log_topic(topics[3])
        
        transfers = []
        try:
            # data: ids[] (uint256[]), values[] (uint256[])
            data_bytes = bytes.fromhex(data[2:] if data.startswith("0x") else data)
            decoded = decode(['uint256[]', 'uint256[]'], data_bytes)
            
            ids = decoded[0]
            values = decoded[1]
            
            if len(ids) != len(values):
                return []
                
            for i in range(len(ids)):
                t = base.model_copy()
                t.token_standard = TokenStandard.ERC1155
                t.from_address = from_addr
                t.to_address = to_addr
                t.value = str(values[i]) # Storing amount
                transfers.append(t)
                
            return transfers
        except Exception as e:
            logger.debug(f"Failed to decode ERC1155 Batch data: {e}")
            # If batch fails, we might return a single Unknown record or nothing.
            # Returning nothing is safer to avoid pollution.
            return []


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
