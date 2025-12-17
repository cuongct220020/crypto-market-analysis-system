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
# Change Description: Refactored to support multi-standard transfer extraction via Service.

from typing import Any, Dict, Optional

from ingestion.ethereumetl.models.token_transfer import EthTokenTransfer
from ingestion.ethereumetl.service.eth_token_transfers_service import EthTokenTransfersService
from utils.formatter_utils import hex_to_dec, to_normalized_address

TRANSFER_EVENT_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


class EthTokenTransferMapper(object):
    @staticmethod
    def token_transfer_to_dict(token_transfer: EthTokenTransfer) -> Dict[str, Any]:
        return token_transfer.model_dump(exclude_none=True)

    @staticmethod
    def json_dict_to_token_transfer(log_dict: Dict[str, Any]) -> Optional[EthTokenTransfer]:
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

        # Delegate parsing to Service
        data = log_dict.get("data", "0x")
        return EthTokenTransfersService.parse_transfer_details(token_transfer, topics, data)
