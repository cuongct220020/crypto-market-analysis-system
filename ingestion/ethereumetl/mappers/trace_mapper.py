# MIT License
#
# Copyright (c) 2018 Evgeniy Filatov, evgeniyfilatov@gmail.com
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
# Change Description: Refactored to use Pydantic models and added comprehensive typing.

from typing import Any, Dict, List, Tuple

from constants.mainnet_daofork_state_changes import DAOFORK_BLOCK_NUMBER
from ingestion.ethereumetl.models.trace import EthTrace
from utils.formatters import hex_to_dec, to_normalized_address


class EthTraceMapper(object):
    @staticmethod
    def json_dict_to_trace(json_dict: Dict[str, Any]) -> EthTrace:
        action = json_dict.get("action", {}) or {}
        result = json_dict.get("result", {}) or {}
        error = json_dict.get("error")
        trace_type = json_dict.get("type")

        trace_data = {
            "block_number": json_dict.get("blockNumber"),
            "transaction_hash": json_dict.get("transactionHash"),
            "transaction_index": json_dict.get("transactionPosition"),
            "subtraces": json_dict.get("subtraces", 0),
            "trace_address": json_dict.get("traceAddress", []),
            "error": error,
            "trace_type": trace_type,
        }

        # common fields in call/create
        if trace_type in ("call", "create"):
            trace_data["from_address"] = to_normalized_address(action.get("from"))
            trace_data["value"] = hex_to_dec(action.get("value"))
            trace_data["gas"] = hex_to_dec(action.get("gas"))
            trace_data["gas_used"] = hex_to_dec(result.get("gasUsed"))

        # process different trace types
        if trace_type == "call":
            trace_data["call_type"] = action.get("callType")
            trace_data["to_address"] = to_normalized_address(action.get("to"))
            trace_data["input"] = action.get("input")
            trace_data["output"] = result.get("output")
        elif trace_type == "create":
            trace_data["to_address"] = result.get("address")
            trace_data["input"] = action.get("init")
            trace_data["output"] = result.get("code")
        elif trace_type == "suicide":
            trace_data["from_address"] = to_normalized_address(action.get("address"))
            trace_data["to_address"] = to_normalized_address(action.get("refundAddress"))
            trace_data["value"] = hex_to_dec(action.get("balance"))
        elif trace_type == "reward":
            trace_data["to_address"] = to_normalized_address(action.get("author"))
            trace_data["value"] = hex_to_dec(action.get("value"))
            trace_data["reward_type"] = action.get("rewardType")

        return EthTrace(**trace_data)

    def geth_trace_to_traces(self, geth_trace: Any) -> List[EthTrace]:
        # geth_trace is a custom object, using Any for now without its definition
        block_number = geth_trace.block_number
        transaction_traces = geth_trace.transaction_traces

        traces = []

        for tx_index, tx_trace in enumerate(transaction_traces):
            traces.extend(
                self._iterate_transaction_trace(
                    block_number,
                    tx_index,
                    tx_trace,
                )
            )

        return traces

    @staticmethod
    def genesis_alloc_to_trace(allocation: Tuple[str, int]) -> EthTrace:
        address: str = allocation[0]
        value: int = allocation[1]

        return EthTrace(block_number=0, to_address=address, value=value, trace_type="genesis", status=1)

    @staticmethod
    def daofork_state_change_to_trace(state_change: Tuple[str, str, int]) -> EthTrace:
        from_address = state_change[0]
        to_address = state_change[1]
        value = state_change[2]

        return EthTrace(
            block_number=DAOFORK_BLOCK_NUMBER,
            from_address=from_address,
            to_address=to_address,
            value=value,
            trace_type="daofork",
            status=1,
        )

    def _iterate_transaction_trace(
        self, block_number: int, tx_index: int, tx_trace: Dict[str, Any], trace_address: List[int] | None = None
    ) -> List[EthTrace]:
        if trace_address is None:
            trace_address = []

        trace_data = {
            "block_number": block_number,
            "transaction_index": tx_index,
            "from_address": to_normalized_address(tx_trace.get("from")),
            "to_address": to_normalized_address(tx_trace.get("to")),
            "input": tx_trace.get("input"),
            "output": tx_trace.get("output"),
            "value": hex_to_dec(tx_trace.get("value")),
            "gas": hex_to_dec(tx_trace.get("gas")),
            "gas_used": hex_to_dec(tx_trace.get("gasUsed")),
            "error": tx_trace.get("error"),
            "trace_type": tx_trace.get("type").lower() if tx_trace.get("type") else None,
        }

        if trace_data["trace_type"] == "selfdestruct":
            trace_data["trace_type"] = "suicide"
        elif trace_data["trace_type"] in ("call", "callcode", "delegatecall", "staticcall"):
            trace_data["call_type"] = trace_data["trace_type"]
            trace_data["trace_type"] = "call"

        calls = tx_trace.get("calls", [])
        trace_data["subtraces"] = len(calls)
        trace_data["trace_address"] = trace_address

        result = [EthTrace(**trace_data)]

        for call_index, call_trace in enumerate(calls):
            result.extend(
                self._iterate_transaction_trace(block_number, tx_index, call_trace, trace_address + [call_index])
            )

        return result

    @staticmethod
    def trace_to_dict(trace: EthTrace) -> Dict[str, Any]:
        return trace.model_dump(exclude_none=True)
