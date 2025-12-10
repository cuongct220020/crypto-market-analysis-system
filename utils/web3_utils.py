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
# Change Description: Added build_async_web3 function for AsyncWeb3 compatibility.

from typing import Any

from web3 import AsyncWeb3, Web3
from web3.middleware import geth_poa_middleware


def build_web3(provider: Any) -> Web3:
    """
    Builds a synchronous Web3 instance with Geth PoA middleware injected.
    Args:
        provider: The Web3 provider instance (e.g., HTTPProvider, IPCProvider).
    Returns:
        A configured synchronous Web3 instance.
    """
    w3 = Web3(provider)
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    return w3


def build_async_web3(provider: Any) -> AsyncWeb3:
    """
    Builds an asynchronous AsyncWeb3 instance with Geth PoA middleware injected.
    Args:
        provider: The AsyncWeb3 provider instance (e.g., AsyncHTTPProvider, AsyncIPCProvider).
    Returns:
        A configured asynchronous AsyncWeb3 instance.
    """
    w3 = AsyncWeb3(provider)
    # w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    return w3
