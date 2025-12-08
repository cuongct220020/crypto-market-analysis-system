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

from typing import List

from pydantic import BaseModel, ConfigDict, Field

from ingestion.ethereumetl.models.receipt_log import EthReceiptLog


class EthReceipt(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    transaction_hash: str | None = None
    transaction_index: int | None = None
    block_hash: str | None = None
    block_number: int | None = None
    cumulative_gas_used: int | None = None
    gas_used: int | None = None
    contract_address: str | None = None
    logs: List[EthReceiptLog] = Field(default_factory=list)
    root: str | None = None
    status: int | None = None
    effective_gas_price: int | None = None
    l1_fee: int | None = None
    l1_gas_used: int | None = None
    l1_gas_price: int | None = None
    l1_fee_scalar: float | None = None
    blob_gas_price: int | None = None
    blob_gas_used: int | None = None
