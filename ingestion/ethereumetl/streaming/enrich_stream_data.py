from typing import List

from ingestion.ethereumetl.models.block import EthBlock
from ingestion.ethereumetl.models.contract import EthContract, EnrichedEthContract
from ingestion.ethereumetl.models.receipt import EthReceipt
from ingestion.ethereumetl.models.transaction import EnrichedEthTransaction, EthTransaction


def enrich_transactions(
        transactions: List[EthTransaction],
        receipts: List[EthReceipt]
    ) -> List[EnrichedEthTransaction]:
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


# async def fetch_and_enrich_transactions(client, block_number):
#     # 1. Gọi song song 2 API để tối ưu thời gian
#     # Lưu ý: eth_getBlock phải set full_transactions=True để lấy chi tiết tx object
#     block_task = client.w3.eth.get_block(block_number, full_transactions=True)
#     receipts_task = client.w3.provider.make_request("eth_getBlockReceipts", [hex(block_number)])
#
#     block, receipts_resp = await asyncio.gather(block_task, receipts_task)
#
#     # Chuyển đổi block sang dict (xử lý HexBytes)
#     block_data = json.loads(AsyncWeb3.to_json(block))
#     receipts_list = receipts_resp['result']
#
#     # 2. Tạo Dictionary cho Receipts để tra cứu nhanh (O(1))
#     # Key là transactionHash (lowercase để an toàn)
#     receipts_map = {r['transactionHash'].lower(): r for r in receipts_list}
#
#     enriched_txs = []
#
#     block_timestamp = block_data.get('timestamp')
#
#     # 3. Duyệt qua từng transaction trong Block và Merge dữ liệu
#     for tx in block_data['transactions']:
#         tx_hash = tx['hash']
#         receipt = receipts_map.get(tx_hash.lower())
#
#         if not receipt:
#             print(f"Warning: Missing receipt for tx {tx_hash}")
#             continue
#
#         # --- A. Map dữ liệu từ TRANSACTION Object ---
#         # Lưu ý: Cần convert các giá trị Hex sang Int/Str nếu cần thiết
#         # Web3.py thường đã convert sẵn nếu dùng get_block, nhưng json.loads sẽ trả về string/int tùy trường hợp
#
#         eth_tx = EnrichedEthTransaction(
#             # Transaction Fields
#             hash=tx_hash,
#             nonce=tx.get('nonce'),
#             block_hash=tx.get('blockHash'),
#             block_number=tx.get('blockNumber'),
#             block_timestamp=block_timestamp,  # Lấy từ block header
#             transaction_index=tx.get('transactionIndex'),
#             from_address=tx.get('from'),
#             to_address=tx.get('to'),
#             value=str(tx.get('value')),  # Convert sang string để tránh tràn số int64
#             gas=tx.get('gas'),
#             gas_price=tx.get('gasPrice'),
#             input=tx.get('input'),
#             max_fee_per_gas=tx.get('maxFeePerGas'),
#             max_priority_fee_per_gas=tx.get('maxPriorityFeePerGas'),
#             transaction_type=int(tx.get('type', '0x0'), 16) if isinstance(tx.get('type'), str) else tx.get('type'),
#             max_fee_per_blob_gas=tx.get('maxFeePerBlobGas'),
#             blob_versioned_hashes=tx.get('blobVersionedHashes', []),
#
#             # --- B. Map dữ liệu từ RECEIPT Object ---
#             receipt_cumulative_gas_used=int(receipt.get('cumulativeGasUsed', 0), 16) if isinstance(
#                 receipt.get('cumulativeGasUsed'), str) else receipt.get('cumulativeGasUsed'),
#             receipt_gas_used=int(receipt.get('gasUsed', 0), 16) if isinstance(receipt.get('gasUsed'),
#                                                                               str) else receipt.get('gasUsed'),
#             receipt_contract_address=receipt.get('contractAddress'),
#             receipt_root=receipt.get('root'),
#
#             # Status: 1 (Success) hoặc 0 (Fail). Cần convert hex -> int
#             receipt_status=int(receipt.get('status', 0), 16) if isinstance(receipt.get('status'), str) else receipt.get(
#                 'status'),
#
#             receipt_effective_gas_price=int(receipt.get('effectiveGasPrice', 0), 16) if isinstance(
#                 receipt.get('effectiveGasPrice'), str) else receipt.get('effectiveGasPrice'),
#
#             # Các trường L2 (Optimism/Arbitrum) hoặc EIP-4844
#             receipt_l1_fee=int(receipt.get('l1Fee', 0), 16) if receipt.get('l1Fee') else None,
#             receipt_l1_gas_used=int(receipt.get('l1GasUsed', 0), 16) if receipt.get('l1GasUsed') else None,
#             receipt_l1_gas_price=int(receipt.get('l1GasPrice', 0), 16) if receipt.get('l1GasPrice') else None,
#             receipt_l1_fee_scalar=float(receipt.get('l1FeeScalar')) if receipt.get('l1FeeScalar') else None,
#
#             receipt_blob_gas_price=int(receipt.get('blobGasPrice', 0), 16) if receipt.get('blobGasPrice') else None,
#             receipt_blob_gas_used=int(receipt.get('blobGasUsed', 0), 16) if receipt.get('blobGasUsed') else None,
#         )
#
#         enriched_txs.append(eth_tx)
#
#     return enriched_txs