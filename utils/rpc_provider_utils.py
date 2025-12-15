from urllib.parse import urlparse

from web3 import HTTPProvider, IPCProvider
from web3.providers.base import BaseProvider
from utils.logger_utils import get_logger

logger = get_logger("RPC Provider Utils")


def get_provider_from_uri(uri_string: str, timeout: int = 60) -> BaseProvider:
    """
    Creates a synchronous Web3 provider based on the URI scheme.
    Currently supports HTTP/HTTPS.
    """
    uri = urlparse(uri_string)

    if uri.scheme == "http" or uri.scheme == "https":
        request_kwargs = {"timeout": timeout}
        return HTTPProvider(uri_string, request_kwargs=request_kwargs)
    elif uri.scheme == "file" or uri_string.endswith(".ipc"):
        return IPCProvider(uri_string)
    else:
        raise ValueError(f"Unknown uri schema {uri_string}. Supported: http, https, file (ipc)")


def check_classic_provider_uri(chain: str, provider_uri: str) -> str:
    if chain == "classic" and provider_uri == "https://mainnet.infura.io":
        logger.warning("ETC Chain not supported on Infura.io. Using https://ethereumclassic.network instead")
        return "https://ethereumclassic.network"
    return provider_uri