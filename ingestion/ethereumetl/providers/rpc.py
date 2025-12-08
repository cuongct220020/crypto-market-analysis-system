# The MIT License (MIT)
#
# Copyright (c) 2016 Piper Merriam
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


import requests
from web3 import HTTPProvider

from utils.logger_utils import get_logger

logger = get_logger(__name__)


# Mostly copied from web3.py/providers/rpc.py. Supports batch requests.
# Will be removed once batch feature is added to web3.py https://github.com/ethereum/web3.py/issues/832
class BatchHTTPProvider(HTTPProvider):

    def make_batch_request(self, text):
        logger.debug("Making request HTTP. URI: %s, Request: %s", self.endpoint_uri, text)
        request_data = text.encode("utf-8")
        raw_response = requests.post(self.endpoint_uri, request_data, headers=self.get_request_headers())

        content = raw_response.content

        response = self.decode_rpc_response(content)

        logger.debug("Getting response HTTP. URI: %s, " "Request: %s, Response: %s", self.endpoint_uri, text, response)

        return response
