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
# Modified by: Cuong CT, 6/12/2025
# Change Description: using eth_utils library for implement some formatter utilities

from typing import Generator, Optional

from eth_utils import to_checksum_address as eth_to_normalized_address
from eth_utils import to_int

from utils.logger_utils import get_logger

logger = get_logger("Formatter Utils")


def hex_to_dec(hex_string: int | None) -> int | None:
    """
    Converts a hex string to decimal integer.
    """
    if hex_string is None:
        return None
    try:
        return to_int(hexstr=hex_string)
    except (ValueError, TypeError):
        logger.warning(f"Invalid hex string for conversion: {hex_string}")
        return None


def to_float_or_none(val: float| str | None) -> float | None:
    if isinstance(val, float):
        return val
    if val is None or val == "":
        return None
    try:
        return float(val)
    except ValueError:
        logger.debug(f"Cannot convert value to float: {val}")
        return None


def chunk_string(string: str, length: int) -> Generator[str, None, None]:
    """
    Splits string into chunks of specified length.
    """
    if not string:
        return
    for i in range(0, len(string), length):
        yield string[i : i + length]


def to_normalized_address(address: Optional[str]) -> Optional[str]:
    """
    Convert address to lowercase
    Safe-guards against None or invalid types to maintain backward compatibility.
    """
    if address is None or not isinstance(address, str):
        return None

    try:
        return eth_to_normalized_address(address)
    except ValueError:
        # Fallback cho các trường hợp địa chỉ "dị" mà hàm cũ vẫn chấp nhận
        return address.lower() if hasattr(address, "lower") else address
