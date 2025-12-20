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
# - Added Mint/Burn detection using ZERO_ADDRESS.
# - Updated to use the new flattened EthTokenTransfer model.

from typing import List, Optional
from eth_abi import decode

from ingestion.ethereumetl.models.token_transfer import (
    EthTokenTransfer, TokenStandard, TransferType, ERC1155TransferMode
)
from ingestion.ethereumetl.models.receipt_log import EthReceiptLog
from utils.formatter_utils import chunk_string, hex_to_dec, to_normalized_address
from utils.logger_utils import get_logger
from constants.event_transfer_signature import (
    TRANSFER_EVENT_SIGNATURE,
    TRANSFER_SINGLE_EVENT_SIGNATURE,
    TRANSFER_BATCH_EVENT_SIGNATURE
)

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

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
        
        # Base transfer object with common fields
        base_transfer_data = {
            "contract_address": to_normalized_address(receipt_logs.address),
            "transaction_hash": receipt_logs.transaction_hash,
            "transaction_index": receipt_logs.transaction_index,
            "log_index": receipt_logs.log_index,
            "block_number": receipt_logs.block_number,
            "block_hash": receipt_logs.block_hash,
            "block_timestamp": receipt_logs.block_timestamp,
            "chain_id": receipt_logs.chain_id if hasattr(receipt_logs, 'chain_id') else None # Safely get chain_id
        }

        # Dispatch based on Event Signature
        if sig == TRANSFER_EVENT_SIGNATURE:
            return EthTokenTransfersService._handle_erc20_721(base_transfer_data, topics, receipt_logs.data)
            
        elif sig == TRANSFER_SINGLE_EVENT_SIGNATURE:
            return EthTokenTransfersService._handle_erc1155_single(base_transfer_data, topics, receipt_logs.data)
            
        elif sig == TRANSFER_BATCH_EVENT_SIGNATURE:
            return EthTokenTransfersService._handle_erc1155_batch(base_transfer_data, topics, receipt_logs.data)
            
        return []

    @staticmethod
    def _handle_erc20_721(base_data: dict, topics: List[str], data: str) -> List[EthTokenTransfer]:
        transfer = EthTokenTransfer(**base_data)
        topics_len = len(topics)
        data_words = extract_log_data_words(data)

        _from = extract_address_from_log_topic(topics[1])
        _to = extract_address_from_log_topic(topics[2])

        transfer.from_address = _from
        transfer.to_address = _to
        
        # Determine TransferType (Mint/Burn)
        if _from == ZERO_ADDRESS:
            transfer.transfer_type = TransferType.MINT
        elif _to == ZERO_ADDRESS:
            transfer.transfer_type = TransferType.BURN

        if topics_len == 3:
            # ERC20: Transfer(from, to, value)
            if len(data_words) != 1:
                transfer.token_standard = TokenStandard.UNKNOWN
                return [transfer]

            transfer.token_standard = TokenStandard.ERC20
            val = hex_to_dec(data_words[0])
            
            # ERC20: Value filled, TokenID Null
            transfer.value = str(val)
            transfer.token_id = None
            
            return [transfer]

        elif topics_len == 4:
            # ERC721: Transfer(from, to, tokenId)
            transfer.token_standard = TokenStandard.ERC721
            val = hex_to_dec(topics[3]) # Token ID is in topic[3]
            
            # ERC721: TokenID filled, Value Null
            transfer.token_id = str(val)
            transfer.value = None 
            
            return [transfer]

        else:
            transfer.token_standard = TokenStandard.UNKNOWN
            return [transfer]

    @staticmethod
    def _handle_erc1155_single(base_data: dict, topics: List[str], data: str) -> List[EthTokenTransfer]:
        """
        Handles ERC1155 TransferSingle(operator, from, to, id, value)
        """
        if len(topics) != 4: # [sig, operator, from, to]
            return []

        transfer = EthTokenTransfer(**base_data)
        transfer.token_standard = TokenStandard.ERC1155
        transfer.erc1155_mode = ERC1155TransferMode.SINGLE
        
        transfer.operator_address = extract_address_from_log_topic(topics[1])
        _from = extract_address_from_log_topic(topics[2])
        _to = extract_address_from_log_topic(topics[3])

        transfer.from_address = _from
        transfer.to_address = _to
        
        # Determine TransferType (Mint/Burn)
        if _from == ZERO_ADDRESS:
            transfer.transfer_type = TransferType.MINT
        elif _to == ZERO_ADDRESS:
            transfer.transfer_type = TransferType.BURN
        
        try:
            data_bytes = bytes.fromhex(data[2:] if data.startswith("0x") else data)
            decoded = decode(['uint256', 'uint256'], data_bytes)
            
            token_id = decoded[0]
            amount = decoded[1]
            
            # ERC1155: Both filled
            transfer.token_id = str(token_id)
            transfer.value = str(amount)
            
            return [transfer]
        except Exception as e:
            logger.debug(f"Failed to decode ERC1155 Single data: {e}. Raw Data: {data}")
            transfer.token_standard = TokenStandard.UNKNOWN
            return [transfer]

    @staticmethod
    def _handle_erc1155_batch(base_data: dict, topics: List[str], data: str) -> List[EthTokenTransfer]:
        """
        Handles ERC1155 TransferBatch(operator, from, to, ids[], values[])
        """
        if len(topics) != 4: # [sig, operator, from, to]
            return []

        _operator = extract_address_from_log_topic(topics[1])
        _from = extract_address_from_log_topic(topics[2])
        _to = extract_address_from_log_topic(topics[3])
        
        transfers = []
        try:
            data_bytes = bytes.fromhex(data[2:] if data.startswith("0x") else data)
            decoded = decode(['uint256[]', 'uint256[]'], data_bytes)
            
            ids = decoded[0]
            values = decoded[1]
            
            if len(ids) != len(values):
                logger.warning(f"ERC1155 Batch: ids length {len(ids)} != values length {len(values)}. Skipping.")
                return []
            
            # Determine TransferType (Mint/Burn)
            transfer_type = TransferType.TRANSFER
            if _from == ZERO_ADDRESS:
                transfer_type = TransferType.MINT
            elif _to == ZERO_ADDRESS:
                transfer_type = TransferType.BURN

            # Split Batch into individual transfer objects
            for i in range(len(ids)):
                t = EthTokenTransfer(**base_data)
                t.token_standard = TokenStandard.ERC1155
                t.erc1155_mode = ERC1155TransferMode.BATCH
                t.operator_address = _operator
                t.from_address = _from
                t.to_address = _to
                t.transfer_type = transfer_type
                
                # ERC1155: Both filled
                t.token_id = str(ids[i])
                t.value = str(values[i])
                
                transfers.append(t)
                
            return transfers
        except Exception as e:
            logger.debug(f"Failed to decode ERC1155 Batch data: {e}. Raw Data: {data}")
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
