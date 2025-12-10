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
# Modified by: Dang Tien Cuong, 2025
# Change Description: Refactored from auto.py. Removed legacy batch provider. Added Type Hints.

from urllib.parse import urlparse

from web3 import AsyncHTTPProvider, HTTPProvider, WebsocketProvider
from web3.providers.async_base import AsyncBaseProvider
from web3.providers.base import BaseProvider

from ingestion.ethereumetl.providers.failover_provider import FailoverAsyncHTTPProvider

DEFAULT_TIMEOUT = 60


def get_provider_from_uri(uri_string: str, timeout: int = DEFAULT_TIMEOUT) -> BaseProvider:
    """
    Creates a synchronous Web3 provider based on the URI scheme.
    Currently supports HTTP/HTTPS.
    """
    uri = urlparse(uri_string)

    if uri.scheme == "http" or uri.scheme == "https":
        request_kwargs = {"timeout": timeout}
        return HTTPProvider(uri_string, request_kwargs=request_kwargs)
    elif uri.scheme == "ws" or uri.scheme == "wss":
        return WebsocketProvider(uri_string, websocket_kwargs={"timeout": timeout})
    else:
        raise ValueError(f"Unknown uri scheme {uri_string}. Supported: http, https, ws, wss")


def get_async_provider_from_uri(uri_string: str, timeout: int = DEFAULT_TIMEOUT) -> AsyncBaseProvider:
    """
    Creates an asynchronous Web3 provider based on the URI scheme.
    Crucial for high-throughput streaming applications.
    """
    uri = urlparse(uri_string)

    if uri.scheme == "http" or uri.scheme == "https":
        request_kwargs = {"timeout": timeout}
        return AsyncHTTPProvider(uri_string, request_kwargs=request_kwargs)
    # Note: AsyncWebsocketProvider is available in newer web3.py versions but often requires
    # specific setup. For now, we prioritize robust HTTP async fetching.
    # Future TODO: Add support for Async IPC/WebSocket for lower latency.
    else:
        raise ValueError(f"Unknown uri scheme {uri_string}. Supported: http, https")


def get_failover_async_provider_from_uris(uri_strings: list[str], timeout: int = DEFAULT_TIMEOUT) -> AsyncBaseProvider:
    """
    Creates a FailoverAsyncHTTPProvider from a list of URIs.
    Useful for high availability.
    """
    if not uri_strings:
        raise ValueError("uri_strings list cannot be empty")

    request_kwargs = {"timeout": timeout}
    return FailoverAsyncHTTPProvider(uri_strings, request_kwargs=request_kwargs)
