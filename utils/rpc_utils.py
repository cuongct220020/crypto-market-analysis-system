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
# Change Description: Refactored for performance, readability, and type safety.

from typing import Any, Dict, Generator, List, Union

from utils.exceptions import RetriableValueError
from utils.logger_utils import get_logger

logger = get_logger(__name__)

JSON_RPC_INTERNAL_ERROR = -32603
JSON_RPC_SERVER_ERROR_MIN = -32099
JSON_RPC_SERVER_ERROR_MAX = -32000


def rpc_response_batch_to_results(
    response: List[Dict[str, Any]],
) -> Generator[Any, None, None]:
    for response_item in response:
        yield rpc_response_to_result(response_item)


def rpc_response_to_result(response: Dict[str, Any]) -> Any:
    result = response.get("result")
    if result is not None:
        return result

    error = response.get("error")
    error_message = f"result is None in response {response}."

    if error is None:
        error_message += " Make sure Ethereum node is synced."
        # When nodes are behind a load balancer it makes sense to retry the request
        # in hopes it will go to other, synced node
        raise RetriableValueError(error_message)

    if is_retriable_error(error.get("code")):
        raise RetriableValueError(error_message)

    raise ValueError(error_message)


def is_retriable_error(error_code: Union[int, str, None]) -> bool:
    if error_code is None or not isinstance(error_code, int):
        return False

    # https://www.jsonrpc.org/specification#error_object
    if error_code == JSON_RPC_INTERNAL_ERROR or (JSON_RPC_SERVER_ERROR_MAX >= error_code >= JSON_RPC_SERVER_ERROR_MIN):
        return True

    return False


def check_classic_provider_uri(chain: str, provider_uri: str) -> str:
    if chain == "classic" and provider_uri == "https://mainnet.infura.io":
        logger.warning("ETC Chain not supported on Infura.io. Using https://ethereumclassic.network instead")
        return "https://ethereumclassic.network"
    return provider_uri
