from typing import Any, List
from urllib.parse import urlparse

from web3 import AsyncHTTPProvider
from web3.providers.async_base import AsyncBaseProvider
from web3.types import RPCEndpoint, RPCResponse

from utils.logger_utils import get_logger

logger = get_logger("Failover Async HTTP Provider")


class FailoverAsyncHTTPProvider(AsyncBaseProvider):
    """
    A Web3 AsyncProvider that supports a list of upstream providers.
    It attempts to send requests to the current primary provider.
    If the request fails, it switches to the next provider in the list (Round Robin / Failover).
    """

    def __init__(
            self,
            endpoint_uris: List[str],
            request_kwargs: Any = None
        ):
        super().__init__()
        self._endpoint_uris = endpoint_uris
        self._providers = []

        for uri in endpoint_uris:
            parsed = urlparse(uri)
            if parsed.scheme in ["http", "https"]:
                # Merge user provided request_kwargs with ssl=False to bypass verification error on macOS
                # Note: In production, one should properly configure SSL certificates instead of disabling verification.
                kwargs = request_kwargs.copy() if request_kwargs else {}
                if "ssl" not in kwargs:
                    kwargs["ssl"] = False
                
                self._providers.append(AsyncHTTPProvider(uri, request_kwargs=kwargs))
            else:
                # TODO: Support other schemes like ws/wss if needed in future
                logger.warning(f"FailoverAsyncHTTPProvider currently only supports http/https. Skipping {uri}")

        if not self._providers:
            raise ValueError("No valid http/https providers found in the provided list.")

        self._current_index = 0

    async def make_request(self, method: RPCEndpoint, params: Any) -> RPCResponse:
        total_providers = len(self._providers)
        attempts = 0

        while attempts < total_providers:
            provider = self._providers[self._current_index]
            try:
                # Delegate to the underlying AsyncHTTPProvider
                return await provider.make_request(method, params)
            except Exception as e:
                # Log the failure and the provider that failed
                logger.warning(
                    f"Provider {self._endpoint_uris[self._current_index]} failed with error: {str(e)}. "
                    "Switching to next provider."
                )

                # Rotate to the next provider
                self._current_index = (self._current_index + 1) % total_providers
                attempts += 1

        # If we've tried all providers and they all failed
        raise ConnectionError(f"All {total_providers} providers failed to respond to request.")
