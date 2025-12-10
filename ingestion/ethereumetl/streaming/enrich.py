from typing import List

from ingestion.ethereumetl.models.block import EthBlock
from ingestion.ethereumetl.models.contract import EnrichedEthContract, EthContract
from ingestion.ethereumetl.models.receipt import EthReceipt
from ingestion.ethereumetl.models.receipt_log import EnrichedEthReceiptLog, EthReceiptLog
from ingestion.ethereumetl.models.token import EnrichedEthToken, EthToken
from ingestion.ethereumetl.models.token_transfer import EnrichedEthTokenTransfer, EthTokenTransfer
from ingestion.ethereumetl.models.trace import EnrichedEthTrace, EthTrace
from ingestion.ethereumetl.models.transaction import EnrichedEthTransaction, EthTransaction


def enrich_transactions(transactions: List[EthTransaction], receipts: List[EthReceipt]) -> List[EnrichedEthTransaction]:
    receipts_map = {r.transaction_hash: r for r in receipts}
    enriched_transactions = []

    for transaction in transactions:
        receipt = receipts_map.get(transaction.hash)

        # Convert base model to enriched model
        enriched_tx = EnrichedEthTransaction.model_validate(transaction.model_dump())

        if receipt:
            enriched_tx.receipt_cumulative_gas_used = receipt.cumulative_gas_used
            enriched_tx.receipt_gas_used = receipt.gas_used
            enriched_tx.receipt_contract_address = receipt.contract_address
            enriched_tx.receipt_root = receipt.root
            enriched_tx.receipt_status = receipt.status
            enriched_tx.receipt_effective_gas_price = receipt.effective_gas_price
            enriched_tx.receipt_l1_fee = receipt.l1_fee
            enriched_tx.receipt_l1_gas_used = receipt.l1_gas_used
            enriched_tx.receipt_l1_gas_price = receipt.l1_gas_price
            enriched_tx.receipt_l1_fee_scalar = receipt.l1_fee_scalar
            enriched_tx.receipt_blob_gas_price = receipt.blob_gas_price
            enriched_tx.receipt_blob_gas_used = receipt.blob_gas_used

        enriched_transactions.append(enriched_tx)

    return enriched_transactions


def enrich_logs(blocks: List[EthBlock], logs: List[EthReceiptLog]) -> List[EnrichedEthReceiptLog]:
    blocks_map = {b.number: b for b in blocks}
    enriched_logs = []

    for log in logs:
        block = blocks_map.get(log.block_number)
        enriched_log = EnrichedEthReceiptLog.model_validate(log.model_dump())

        if block:
            enriched_log.block_timestamp = block.timestamp
            enriched_log.block_hash = block.hash

        enriched_logs.append(enriched_log)

    return enriched_logs


def enrich_token_transfers(
    blocks: List[EthBlock], token_transfers: List[EthTokenTransfer]
) -> List[EnrichedEthTokenTransfer]:
    blocks_map = {b.number: b for b in blocks}
    enriched_transfers = []

    for transfer in token_transfers:
        block = blocks_map.get(transfer.block_number)
        enriched_transfer = EnrichedEthTokenTransfer.model_validate(transfer.model_dump())

        if block:
            enriched_transfer.block_timestamp = block.timestamp
            enriched_transfer.block_hash = block.hash

        enriched_transfers.append(enriched_transfer)

    return enriched_transfers


def enrich_traces(blocks: List[EthBlock], traces: List[EthTrace]) -> List[EnrichedEthTrace]:
    blocks_map = {b.number: b for b in blocks}
    enriched_traces = []

    for trace in traces:
        block = blocks_map.get(trace.block_number)
        enriched_trace = EnrichedEthTrace.model_validate(trace.model_dump())

        if block:
            enriched_trace.block_timestamp = block.timestamp
            enriched_trace.block_hash = block.hash

        enriched_traces.append(enriched_trace)

    return enriched_traces


def enrich_contracts(blocks: List[EthBlock], contracts: List[EthContract]) -> List[EnrichedEthContract]:
    blocks_map = {b.number: b for b in blocks}
    enriched_contracts = []

    for contract in contracts:
        block = blocks_map.get(contract.block_number)
        enriched_contract = EnrichedEthContract.model_validate(contract.model_dump())

        if block:
            enriched_contract.block_timestamp = block.timestamp
            enriched_contract.block_hash = block.hash

        enriched_contracts.append(enriched_contract)

    return enriched_contracts


def enrich_tokens(blocks: List[EthBlock], tokens: List[EthToken]) -> List[EnrichedEthToken]:
    blocks_map = {b.number: b for b in blocks}
    enriched_tokens = []

    for token in tokens:
        block = blocks_map.get(token.block_number)
        enriched_token = EnrichedEthToken.model_validate(token.model_dump())

        if block:
            enriched_token.block_timestamp = block.timestamp
            enriched_token.block_hash = block.hash

        enriched_tokens.append(enriched_token)

    return enriched_tokens
